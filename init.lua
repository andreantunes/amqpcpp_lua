local amqpcpp = require('amqpcpp')

local _getPublishConnection
local _getPublisher
local _coTest

local PublishClass = { }

function _coTest(connectionConfig, waitTimeMs)
  local publisher = _getPublisher(connectionConfig)
  publisher:asyncDeclareQueue("_testConn")
  local result = publisher:coWaitForDeclaredQueue("_testConn", waitTimeMs)
  publisher:stopCyclePoll()
  return result
end

function _getPublisher(connectionConfig)
  if not connectionConfig.host or
    not connectionConfig.port or
    not connectionConfig.username or
    not connectionConfig.password or
    not connectionConfig.vhost then

    ngx.log(ngx.ERR, "amqpcpp_helper error: could not initialize rabbitmq, no config found. " .. debug.traceback())
    return
  end

  local t = {
    connectionConfig = connectionConfig,
    connectionId = nil,
    autoPoll = true,
  }

  local connectionId = _getPublishConnection(connectionConfig, t)
  t.connectionId = connectionId

  setmetatable(t, { __index = PublishClass })
  return t
end

function PublishClass:asyncPublish(exchange, routingKey, subject, data, correlationId, expirationMs)
  amqpcpp.publish(self.connectionId, exchange, routingKey, subject, data, correlationId or "", tostring(expirationMs or "") or "")
end

function PublishClass:asyncPublishRpc(exchange, routingKey, subject, data, expirationMs)
  return amqpcpp.publish_rpc(self.connectionId, exchange, routingKey, subject, data, tostring(expirationMs or ""))
end

function PublishClass:coPublishRpc(exchange, routingKey, subject, data, expirationMs)
  local correlationId = self:asyncPublishRpc(exchange, routingKey, subject, data, expirationMs)
  return self:coWaitForRpcAnswer(correlationId, expirationMs)
end

function PublishClass:asyncDeclareQueue(queueName, queueTimeout)
  amqpcpp.declare_queue(self.connectionId, queueName, 0, queueTimeout or 0)
end

function PublishClass:asyncRemoveQueue(queueName)
  amqpcpp.remove_queue(self.connectionId, queueName)
end

function PublishClass:coWaitForDeclaredQueue(queueName, expirationMs)
  ngx.update_time()
  local timeout = ngx.now() * 1000 + expirationMs

  while true do
    local success = amqpcpp.declare_queue_is_ready(self.connectionId, queueName)

    if success then
      return true
    end

    if ngx.now() * 1000 > timeout then
      ngx.log(ngx.ERR, "amqpcpp_helper error: coWaitForDeclaredQueue timed out. " .. debug.traceback())
      return false
    end

    ngx.sleep(0.032)
  end
end

function PublishClass:coWaitForRpcAnswer(correlationId, expirationMs)
  ngx.update_time()
  local timeout = ngx.now() * 1000 + expirationMs

  while true do
    local success, message = amqpcpp.get_rpc_message(self.connectionId, correlationId)

    if success then
      return true, message
    end

    if ngx.now() * 1000 > timeout then
      ngx.log(ngx.ERR, "amqpcpp_helper error: waiting for answer timed out." .. debug.traceback())
      return false
    end

    ngx.sleep(0.032)
  end
end

function PublishClass:stopCyclePoll()
  self.autoPoll = false
end

function PublishClass:poll()
  local ok, err = amqpcpp.poll(self.connectionId)

  if not ok then
    ngx.log(ngx.ERR, "amqpcpp_helper error: " .. tostring(err))
    return false
  end

  return true
end

function PublishClass:coFlushAndExit()
  amqpcpp.declare_queue(self.connectionId, "__exiting", 0, 0)

  while true do
    local ok, err = amqpcpp.poll(self.connectionId)

    if amqpcpp.declare_queue_is_ready(self.connectionId, "__exiting") then
      return
    end

    if not ok then
      amqpcpp.terminate(self.connectionId)
      ngx.log(ngx.ERR, "amqpcpp_helper error: " .. tostring(err))
      break
    end

    ngx.sleep(0.032)
  end
end

function _getPublishConnection(connectionConfig, publish)
  local id = amqpcpp.create(connectionConfig.host, connectionConfig.port, connectionConfig.username, connectionConfig.password, connectionConfig.vhost)
  amqpcpp.start(id)

  ngx.timer.at(0, function()
    while true do
      local ok, err = amqpcpp.poll(id)

      if not ok then
        amqpcpp.terminate(id)
        ngx.log(ngx.ERR, "amqpcpp_helper error: " .. tostring(err) .. " " .. connectionConfig.host .. ":" .. connectionConfig.port .. " user:" .. connectionConfig.username)
        amqpcpp.start(id)
        ngx.sleep(1)
      end

      ngx.sleep(0.032)

      if not publish.autoPoll then
        break
      end
    end
  end)

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
    dedupAlgorithm = false,
    declareQueues = { },
    consumeQueues = { }
  }

  if not connectionConfig.host or
    not connectionConfig.port or
    not connectionConfig.username or
    not connectionConfig.password or
    not connectionConfig.vhost then

    ngx.log(ngx.ERR, "amqpcpp_helper error: could not initialize rabbitmq, no config found.")
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

function ConsumerClass:declareQueue(name, durable)
  table.insert(self.declareQueues, { name = name, durable = durable })
end

function ConsumerClass:consumeQueue(name)
  table.insert(self.consumeQueues, { name = name })
end

function ConsumerClass:enableAutoConvertMessageToStream(subject)
  table.insert(self.autoConvertMesageToStreamSubjects, subject)
end

function ConsumerClass:enableDedupAlgorithm()
  self.dedupAlgorithm = true
end

function ConsumerClass:bindQueue(exchange, routingKey, queueName)
  table.insert(self.bindings, { exchange = exchange, queueName = queueName, routingKey = routingKey })
end

function ConsumerClass:declareExchange(exchange, mode, durable)
  table.insert(self.exchanges, { exchange = exchange, mode = mode, durable = durable })
end

function ConsumerClass:coConsumeUntilFails(id, onData)
  if self.dedupAlgorithm then
    amqpcpp.enable_dedup_algorithm(id)
  end

  for _, subject in ipairs(self.autoConvertMesageToStreamSubjects) do
    amqpcpp.enable_auto_convert_message_to_stream(id, subject)
  end

  for _, exchange in ipairs(self.exchanges) do
    local mode = 0
    if exchange.mode == "fanout" then
      mode = 0

    elseif exchange.mode == "direct" then
      mode = 1

    elseif exchange.mode == "topic" then
      mode = 2

    elseif exchange.mode == "headers" then
      mode = 3
    end

    local flags = 0
    if exchange.durable then
      flags = 1
    end

    amqpcpp.declare_exchange(id, exchange.exchange, mode, flags)
  end

  for _, declare in ipairs(self.declareQueues) do
    local flags = 0

    if declare.durable then
      flags = 1
    end

    amqpcpp.declare_queue(id, declare.name, flags)
  end

  for _, binding in ipairs(self.bindings) do
    amqpcpp.bind_queue(id, binding.exchange, binding.routingKey, binding.queueName)
  end

  for _, consume in ipairs(self.consumeQueues) do
    amqpcpp.consume(id, consume.name, 0)
  end

  local exiting = false

  while true do
    local ok, err = amqpcpp.poll(id)

    if not ok then
      ngx.log(ngx.ERR, "amqpcpp_helper error: " .. tostring(err))
      break
    end

    if not ngx.worker.exiting() then
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
          ngx.log(ngx.ERR, "amqpcpp_helper error: " .. tostring(errorOrResult))
        end

        ngx.update_time()
        if ngx.now() * 1000 > maxProcessingTimeTimeout then -- we might have highly intensive actions that might prevent amqpcpp.poll calling fast enough thus causing timeout
          break
        end

        ok, msg, ack, exchange, routingKey, replyTo, correlationId, subject = amqpcpp.get_ready_message(id)
      end
    else
      if not exiting then
        exiting = true
        amqpcpp.declare_queue(id, "__exiting", 0, 0)
      end

      if amqpcpp.declare_queue_is_ready(id, "__exiting") then
        break
      end
    end

    ngx.sleep(0.032)
  end
end

return {
  coTest = _coTest,
  getPublisher = _getPublisher,
  getConsumer = _getConsumer,
  tableToStream = amqpcpp.table_to_stream,
  streamToTable = amqpcpp.stream_to_table,
}
