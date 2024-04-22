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

function PublishClass:asyncPublish(exchange, routingKey, data, subject, correlationId, expirationMs)
  ngx.timer.at(0, _coPublish, self, exchange, routingKey, data, subject, correlationId, expirationMs)
end 

function PublishClass:coPublish(exchange, routingKey, data, subject, correlationId, expirationMs)
  local id = _getPublishConnection(self.connectionConfig)
  amqpcpp.publish(id, exchange, routingKey, data, subject or "", correlationId or "", tostring(expirationMs or "") or "")
end 

function PublishClass:cpPublishRpc(exchange, routingKey, data, subject, expirationMs)
  local id = _getPublishConnection(self.connectionConfig)
  local correlationId = amqpcpp.publish_rpc(id, exchange, routingKey, data, subject or "", tostring(expirationMs or ""))
  return id, correlationId
end

function PublishClass:coPublishRpcAndWait(exchange, routingKey, data, subject, expirationMs)
  local id = _getPublishConnection(self.connectionConfig)
  local correlationId = amqpcpp.publish_rpc(id, exchange, routingKey, data, subject or "", tostring(expirationMs or ""))
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
      print("Connection failed, could not retrieve.")
      return false  
    end  
  
    if success then
      return true, message
    end 
  
    if ngx.now() * 1000 > timeout then
      return false
    end
  
    ngx.sleep(0.032)
  end
end

function _getPublishConnection(connectionConfig)
  local connectionHash = table.concat({ connectionConfig.host, connectionConfig.port, connectionConfig.username, connectionConfig.password, connectionConfig.vhost })
  local id = p_publishConnections[connectionHash]

  if id and not amqpcpp.is_ok(id) then
    id = nil
  end

  if not id then
    id = amqpcpp.init(connectionConfig.host, connectionConfig.port, connectionConfig.username, connectionConfig.password, connectionConfig.vhost)
    p_publishConnections[connectionHash] = id

    ngx.timer.at(0, function()
      while true do
        local ok, err = amqpcpp.poll(id)
 
        if not ok then
          amqpcpp.terminate(id)
          p_publishConnections[connectionHash] = nil
          ngx.log(ngx.ERR, err)
          break
        end

        ngx.sleep(0.032)
      end
    end)
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
    autoConvertMesageToStreamSubjects = { }
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
  while true do
    self:coConsumeUntilFails(onData)
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

function ConsumerClass:bind(exchange, queueName, routingKey)
  table.insert(self.bindings, { exchange = exchange, queueName = queueName, routingKey = routingKey })
end

function ConsumerClass:declareExchange(exchange, mode, flags)
  table.insert(self.exchanges, { exchange = exchange, mode = mode, flags = flags })
end

function ConsumerClass:coConsumeUntilFails(onData)
  local id = amqpcpp.init(self.connectionConfig.host, self.connectionConfig.port, self.connectionConfig.username, self.connectionConfig.password, self.connectionConfig.vhost)

  for _, subject in ipairs(self.autoConvertMesageToStreamSubjects) do
    amqpcpp.enable_auto_convert_message_to_stream(id, subject)
  end

  for _, exchange in ipairs(self.exchanges) do
    amqpcpp.declare_exchange(id, exchange.exchange, exchange.mode, exchange.flags)
  end

  for _, binding in ipairs(self.bindings) do
    amqpcpp.bind_queue(id, binding.exchange, binding.queueName, binding.routingKey)
  end

  amqpcpp.declare_queue(id, self.queueName, 0)
  amqpcpp.consume(id, self.queueName, 0)

  while true do
    local ok, err = amqpcpp.poll(id)

    if not ok then
      print(err)
      break
    end

    local ok, msg, ack, exchange, routingKey, replyTo, correlationId, subject = amqpcpp.get_ready_message(id)
    local maxProcessingTimeTimeout = ngx.now() * 1000 + (100)

    while ok do
      local callOk, errorOrResult = pcall(onData, msg, subject, exchange, routingKey, replyTo, correlationId)
      if not callOk then
        ngx.log(ngx.ERR, errorOrResult)
        amqpcpp.reject(id, ack)

      elseif errorOrResult == true then
        amqpcpp.ack(id, ack)

      else
        amqpcpp.reject(id, ack)
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

  amqpcpp.terminate(id)
end

return {
  getPublisher = _getPublisher,
  getConsumer = _getConsumer,
  tableToStream = amqpcpp.table_to_stream,
  streamToTable = amqpcpp.stream_to_table,
}
