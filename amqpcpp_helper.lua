local amqpcpp = require('amqpcpp')

local p_rabbitConnections = { }

local _consumeUntilFails
local _consumeForever
local _getPublishConnection
local _publish
local _publishRpc
local _waitForAnswer

function _consumeUntilFails(queueName, onData, connectionConfig)
  if not connectionConfig.host or 
    not connectionConfig.port or 
    not connectionConfig.username or 
    not connectionConfig.password or
    not connectionConfig.vhost then

    ngx.log(ngx.ERR, "could not initialize rabbitmq, no config found.")
    return
  end

  local id = amqpcpp.init(connectionConfig.host, connectionConfig.port, connectionConfig.username, connectionConfig.password, connectionConfig.vhost)

  if type(queueName) == 'table' then
    for _, queueNameIt in ipairs(queueName) do
      amqpcpp.declare_queue(id, queueNameIt)
      amqpcpp.consume(id, queueNameIt)
    end
  else
    amqpcpp.declare_queue(id, queueName)
    amqpcpp.consume(id, queueName)
  end

  while true do
    local ok, err = amqpcpp.poll(id)

    if not ok then
      print(err)
      break 
    end

    local ok, msg, ack, exchange, routingKey, replyTo, correlationId = amqpcpp.get_ready_message(id)  
 
    while ok do
      local callOk, errorOrResult = pcall(onData, msg, id, ack, exchange, routingKey, replyTo, correlationId)
      if not callOk then
        ngx.log(ngx.ERR, errorOrResult)
        amqpcpp.reject(id, ack)

      elseif errorOrResult == true then
        amqpcpp.ack(id, ack)

      else
        amqpcpp.reject(id, ack)
      end

      ok, msg, ack, exchange, routingKey, replyTo, correlationId = amqpcpp.get_ready_message(id)
    end

    ngx.sleep(0.032)
  end

  amqpcpp.terminate(id)
end

function _consumeForever(queueName, onData, connectionConfig)
  while true do
    _consumeUntilFails(queueName, onData, connectionConfig)
    ngx.sleep(5)
  end
end

function _getPublishConnection(connectionConfig)
  if not connectionConfig.host or 
    not connectionConfig.port or 
    not connectionConfig.username or 
    not connectionConfig.password or
    not connectionConfig.vhost then

    ngx.log(ngx.ERR, "could not initialize rabbitmq, no config found.")
    return
  end

  local connectionHash = table.concat({ connectionConfig.host, connectionConfig.port, connectionConfig.username, connectionConfig.password, connectionConfig.vhost })
  local id = p_rabbitConnections[connectionHash]

  if id and not amqpcpp.is_ok(id) then
    id = nil
  end 

  if not id then
    id = amqpcpp.init(connectionConfig.host, connectionConfig.port, connectionConfig.username, connectionConfig.password, connectionConfig.vhost)
    p_rabbitConnections[connectionHash] = id

    ngx.timer.at(0.1, function()
      while true do
        local ok, err = amqpcpp.poll(id)

        if not ok then
          amqpcpp.terminate(id)
          p_rabbitConnections[connectionHash] = nil
          ngx.log(ngx.ERR, err)
          break
        end

        ngx.sleep(0.032)
      end
    end)
  end

  return id
end

function _publish(exchange, routingKey, data, connectionConfig, config)
  local id = _getPublishConnection(connectionConfig)
  local correlationId = ""

  if config then
    correlationId = config.correlationId or ""
  end

  amqpcpp.publish(id, exchange, routingKey, data, correlationId)
end

function _publishRpc(exchange, routingKey, data, connectionConfig, config)
  local id = _getPublishConnection(connectionConfig)
  return id, amqpcpp.publish_rpc(id, exchange, routingKey, data)
end 

function _waitForAnswer(connectionId, correlationId, timeout)
  local timeout = os.time() + timeout
  
  while true do 
    local amqpFailed, success, message = amqpcpp.get_rpc_message(connectionId, correlationId)
  
    if amqpFailed then
      print("Connection failed, could not retrieve.")
      return false  
    end  
 
    if success then
      return true, message
    end 

    if os.time() > timeout then
      return false
    end

    ngx.sleep(0.032)
  end
end

return {
  consumeForever = _consumeForever,
  publish = _publish,
  publishRpc = _publishRpc,
  waitForAnswer = _waitForAnswer
}
