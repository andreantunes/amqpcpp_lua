local amqpcpp = require('amqpcpp')

local _getPublishConnection
local _getPublisher

local p_publishConnections = { }
local PublishClass = { }

function _getPublisher(connectionConfig)
  local t = {
    connectionConfig = connectionConfig
  }

  if not connectionConfig.host or
    not connectionConfig.port or
    not connectionConfig.username or
    not connectionConfig.password or
    not connectionConfig.vhost then

    ngx.log(ngx.ERR, "could not initialize rabbitmq, no config found.")
    return
  end

  setmetatable(t, { __index = PublishClass })
  return t
end

local function _coPublish(premature, ...)
  PublishClass.coPublish(...)
end

function PublishClass:asyncPublish(exchange, routingKey, subject, data, correlationId, expirationMs)
  ngx.timer.at(0, _coPublish, self, exchange, routingKey, subject, data, correlationId, expirationMs)
end
 
function PublishClass:coPublish(exchange, routingKey, subject, data, correlationId, expirationMs)
  local id = _getPublishConnection(self.connectionConfig)
  amqpcpp.publish(id, exchange, routingKey, subject, data, correlationId or "", tostring(expirationMs or "") or "")
end 

function PublishClass:cpPublishRpc(exchange, routingKey, subject, data, expirationMs)
  local id = _getPublishConnection(self.connectionConfig)
  local correlationId = amqpcpp.publish_rpc(id, exchange, routingKey, subject, data, tostring(expirationMs or ""))
  return id, correlationId
end

function PublishClass:coPublishRpcAndWait(exchange, routingKey, subject, data, expirationMs)
  local id = _getPublishConnection(self.connectionConfig)
  local correlationId = amqpcpp.publish_rpc(id, exchange, routingKey, subject, data, tostring(expirationMs or ""))
  return self:coWaitForRpcAnswer(id, correlationId, expirationMs)
end

function PublishClass:coDeclareQueue(queueName, timeout)
  local id = _getPublishConnection(self.connectionConfig)
  amqpcpp.declare_queue(id, queueName, 0, timeout or 0)
end

function PublishClass:coWaitForRpcAnswer(connectionId, correlationId, expirationMs)
  local timeout = ngx.now() * 1000 + expirationMs

  while true do
    local amqpFailed, success, message = amqpcpp.get_rpc_message(connectionId, correlationId)
  
    if amqpFailed then
      ngx.log(ngx.ERR, "Connection failed, could not retrieve.")
      return false  
    end  
  
    if success then
      return true, message
    end 
  
    if ngx.now() * 1000 > timeout then
      return false
    end
  
    ngx.sleep(0.032)

    if ngx.worker.exiting() then
      return false
    end
  end
end

function _getPublishConnection(connectionConfig)
  local connectionHash = table.concat({ connectionConfig.host, connectionConfig.port, connectionConfig.username, connectionConfig.password, connectionConfig.vhost })
  local id = p_publishConnections[connectionHash]
  local connecting = false

  if not id then
    connecting = true
    id = amqpcpp.create(connectionConfig.host, connectionConfig.port, connectionConfig.username, connectionConfig.password, connectionConfig.vhost)
    amqpcpp.start(id)

    p_publishConnections[connectionHash] = id

    ngx.timer.at(0, function()
      while true do
        local ok, err = amqpcpp.poll(id)
 
        if not ok then
          amqpcpp.terminate(id)
          ngx.log(ngx.ERR, err)
          amqpcpp.start(id)
        end

        ngx.sleep(0.032)

        if ngx.worker.exiting() then
          break
        end
      end
    end)
  end

  while not amqpcpp.is_ready(id) do
    ngx.sleep(0.032)

    if ngx.worker.exiting() then
      break
    end
  end

  if connecting then
    print("Connected to rabbit!");
  end

  return id
end

local _getConsumer
local ConsumerClass = { }

function _getConsumer(connectionConfig)
  local t = { 
    connectionConfig = connectionConfig,
    bindings = { },
    exchanges = { },
    autoConvertMesageToStreamSubjects = { },
    dedupAlgorithm = false
  }

  if not connectionConfig.host or
    not connectionConfig.port or
    not connectionConfig.username or
    not connectionConfig.password or
    not connectionConfig.vhost then

    ngx.log(ngx.ERR, "could not initialize rabbitmq, no config found.")
    return
  end

  setmetatable(t, { 
    __index = ConsumerClass,
  })

  return t
end

function ConsumerClass:syncLoop(onData)
  local id = amqpcpp.create(self.connectionConfig.host, self.connectionConfig.port, self.connectionConfig.username, self.connectionConfig.password, self.connectionConfig.vhost)

  while true do
    amqpcpp.start(id)
    self:coConsumeUntilFails(id, onData)
    amqpcpp.terminate(id)

    if ngx.worker.exiting() then
      break
    end

    ngx.sleep(5)
  end
end

function ConsumerClass:init(queueName)
  self.queueName = queueName
end

function ConsumerClass:enableAutoConvertMessageToStream(subject)
  table.insert(self.autoConvertMesageToStreamSubjects, subject)
end

function ConsumerClass:enableDedupAlgorithm()
  self.dedupAlgorithm = true
end 

function ConsumerClass:bind(exchange, queueName, routingKey)
  table.insert(self.bindings, { exchange = exchange, queueName = queueName, routingKey = routingKey })
end

function ConsumerClass:declareExchange(exchange, mode, flags)
  table.insert(self.exchanges, { exchange = exchange, mode = mode, flags = flags })
end

function ConsumerClass:coConsumeUntilFails(id, onData)
  if self.dedupAlgorithm then
    amqpcpp.enable_dedup_algorithm(id) 
  end

  for _, subject in ipairs(self.autoConvertMesageToStreamSubjects) do
    amqpcpp.enable_auto_convert_message_to_stream(id, subject)
  end

  for _, exchange in ipairs(self.exchanges) do
    amqpcpp.declare_exchange(id, exchange.exchange, exchange.mode, exchange.flags)
  end

  amqpcpp.declare_queue(id, self.queueName, 0)

  for _, binding in ipairs(self.bindings) do
    amqpcpp.bind_queue(id, binding.exchange, binding.queueName, binding.routingKey)
  end

  amqpcpp.consume(id, self.queueName, 0)

  while true do
    local ok, err = amqpcpp.poll(id)

    if not ok then
      ngx.log(ngx.ERR, err)
      break
    end

    local ok, msg, ack, exchange, routingKey, replyTo, correlationId, subject = amqpcpp.get_ready_message(id)
    local maxProcessingTimeTimeout = ngx.now() * 1000 + (100)

    while ok do
      local callOk, errorOrResult = pcall(onData, msg, subject, exchange, routingKey, replyTo, correlationId)
      if callOk then
        if errorOrResult == false then
          amqpcpp.reject(id, ack)
        else
          amqpcpp.ack(id, ack)
        end
      else
        amqpcpp.ack(id, ack)
        ngx.log(ngx.ERR, errorOrResult)
      end

      ok, msg, ack, exchange, routingKey, replyTo, correlationId, subject = amqpcpp.get_ready_message(id)

      ngx.update_time()
      if ngx.now() * 1000 > maxProcessingTimeTimeout then -- we might have highly intensive actions that might prevent amqpcpp.poll calling fast enough thus causing timeout
        break
      end 
    end

    if ngx.worker.exiting() then
      break
    end

    ngx.sleep(0.032)
  end
end

return {
  getPublisher = _getPublisher,
  getConsumer = _getConsumer,
  tableToStream = amqpcpp.table_to_stream,
  streamToTable = amqpcpp.stream_to_table,
}
