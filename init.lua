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

function PublishClass:publish(exchange, routingKey, data, subject, correlationId, expirationMs)
  local id = _getPublishConnection(self.connectionConfig)
  amqpcpp.publish(id, exchange, routingKey, data, subject or "", correlationId or "", tostring(expirationMs or "") or "")
end 

function PublishClass:publishRpc(exchange, routingKey, data, subject, expirationMs)
  local id = _getPublishConnection(self.connectionConfig)
  local correlationId = amqpcpp.publish_rpc(id, exchange, routingKey, data, subject or "", tostring(expirationMs or ""))
  return id, correlationId
end

function PublishClass:publishRpcAndWait(exchange, routingKey, data, subject, expirationMs)
  local id = _getPublishConnection(self.connectionConfig)
  local correlationId = amqpcpp.publish_rpc(id, exchange, routingKey, data, subject or "", tostring(expirationMs or ""))
  return self:waitForRpcAnswer(id, correlationId, expirationMs)
end

function PublishClass:waitForRpcAnswer(connectionId, correlationId, expirationMs)
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

    ngx.timer.at(0.1, function()
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
    exchanges = { }
  }

  if not connectionConfig.host or
    not connectionConfig.port or
    not connectionConfig.username or
    not connectionConfig.password or
    not connectionConfig.vhost then

    ngx.log(ngx.ERR, "could not initialize rabbitmq, no config found.")
    return
  end

  setmetatable(t, { __index = ConsumerClass })
  return t
end

function ConsumerClass:loop(onData)
  while true do
    self:consumeUntilFails(onData)
    ngx.sleep(5)
  end
end

function ConsumerClass:init(queueName, exchanges, bindings)
  self.queueName = queueName
end

function ConsumerClass:consumeUntilFails(onData)
  local id = amqpcpp.init(self.connectionConfig.host, self.connectionConfig.port, self.connectionConfig.username, self.connectionConfig.password, self.connectionConfig.vhost)

  amqpcpp.declare_queue(id, self.queueName, 0)
  amqpcpp.consume(id, self.queueName, 0)

  while true do
    local ok, err = amqpcpp.poll(id)

    if not ok then
      print(err)
      break
    end

    local ok, msg, ack, exchange, routingKey, replyTo, correlationId, subject = amqpcpp.get_ready_message(id)

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
    end

    ngx.sleep(0.032)
  end

  amqpcpp.terminate(id)
end

return {
  getPublisher = _getPublisher,
  getConsumer = _getConsumer,
}
