#include <unistd.h>
#include <amqpcpp.h>
#include <amqpcpp/libboostasio.h>
#include <sstream>
#include <boost/asio.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <chrono>
#include <thread>
#include <list>

extern "C" {
  #include "lua.h"
  #include "lua.h"
  #include "lauxlib.h"
}

std::string generateUniqueString()
{
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    std::stringstream ss;
    ss << uuid;
    return ss.str();
}

class AmqpHandler : public AMQP::LibBoostAsioHandler
{
private:
    virtual void onError(AMQP::TcpConnection *connection, const char *message) override {
        error = true;
        errorMessage = std::string(message);
    }

public:
    bool error = false;
    std::string errorMessage;

    AmqpHandler(boost::asio::io_context &io_context) : AMQP::LibBoostAsioHandler(io_context) {}

    virtual ~AmqpHandler() = default;
};

struct AmqpMessage
{
    std::string message;
    std::string exchange;
    std::string routingKey;
    std::string correlationId;
    std::string replyTo;
    std::string subject;
    std::string expiration;
    uint64_t ack;
};

typedef std::shared_ptr<AmqpMessage> AmqpMessagePtr;
class Amqp
{
public:
    Amqp(const std::string& address) {
        m_address = address;
        m_handler = new AmqpHandler(m_io_context);
        m_connection = new AMQP::TcpConnection(m_handler, AMQP::Address(address));
        m_channel = new AMQP::TcpChannel(m_connection);

        m_channel->onError([this](const char* message) {
            m_channelError = true;
            m_channelErrorMessage = message;
        });
    }

    ~Amqp() {
        //this order matters
        delete m_channel;
        m_channel = NULL;

        delete m_connection;
        m_connection = NULL;

        delete m_handler;
        m_handler = NULL;
    }

    bool poll(std::string& error) {
        if(m_failed)
            return false;

        m_io_context.poll();
        m_io_context.reset();

        if(m_handler->error) {
            error = m_handler->errorMessage;
            m_failed = true;
            return false;
        }

        if(m_consumerCancelled) {
            error = "Channel cancelled";
            m_failed = true;
            return false;
        }

        if(m_channelError) {
            error = m_channelErrorMessage;
            m_failed = true;
            return false;
        }

        if(!m_channel->usable()) {
            error = "Channel !usabled().";
            m_failed = true;
            return false;
        }

        return true;
    }

    void consume(const std::string& queueName, int flags) {
        m_channel->consume(queueName, flags)
            .onReceived([this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
                const AMQP::Table& headers = message.headers();
                const std::string& subject = headers["subject"];

                AmqpMessagePtr amqpMessage = AmqpMessagePtr(new AmqpMessage);
                amqpMessage->message = std::string(message.body(), message.bodySize());
                amqpMessage->exchange = message.exchange();
                amqpMessage->routingKey = message.routingkey();
                amqpMessage->replyTo = message.replyTo();
                amqpMessage->correlationId = message.correlationID();
                amqpMessage->ack = deliveryTag;
                amqpMessage->subject = subject;
                m_messages.push_back(amqpMessage);
            })
            .onSuccess([](const std::string &consumertag) {
                //std::cout << "start!" << consumertag << std::endl;
            })
            .onCancelled([this](const std::string &consumertag) {
                m_consumerCancelled = true;
            })
            .onError([this](const char *message) {
                m_channelError = true;
                m_channelErrorMessage = std::string(message);
            });
    }

    bool hasFailed() {
        return m_failed;
    }

    std::string publishRPC(const std::string& exchange, const std::string& routingKey, const std::string& message, const std::string& subject, const std::string& expiration) {
        enableReplyTo();

        std::string correlationId = generateUniqueString();

        if(!m_replyToActive || m_enablingReplyTo) {
            AmqpMessagePtr amqpMessage = AmqpMessagePtr(new AmqpMessage);
            amqpMessage->message = message;
            amqpMessage->exchange = exchange;
            amqpMessage->routingKey = routingKey;
            amqpMessage->correlationId = correlationId;
            amqpMessage->expiration = expiration;
            amqpMessage->subject = subject;
            m_preReplyToMessages.push_back(amqpMessage);
            return correlationId;
        }

        AMQP::Envelope publishEnvelope(message);
        publishEnvelope.setReplyTo("amq.rabbitmq.reply-to");
        publishEnvelope.setCorrelationID(correlationId);

        if(!expiration.empty())
            publishEnvelope.setExpiration(expiration);

        AMQP::Table headers;
        headers["subject"] = subject;
        publishEnvelope.setHeaders(headers);

        m_channel->publish(exchange, routingKey, publishEnvelope);
        return correlationId;
    }

    void ack(uint64_t ack) {
        m_channel->ack(ack);
    }

    void reject(uint64_t ack) {
        m_channel->ack(ack, AMQP::requeue);
    }

    std::list<AmqpMessagePtr>& getMessages() { return m_messages; }
    std::unordered_map<std::string, std::string>& getRPCMessages() { return m_rpcMessages; }

    void declareExchange(const std::string& exchangeName, int mode, int flags) {
        m_channel->declareExchange(exchangeName, AMQP::ExchangeType(mode), flags);
    }

    void declareQueue(const std::string& queueName, int flags) {
        AMQP::Table declareQueueArguments;
        //declareQueueArquments["x-queue-type"] = "quorum";
        declareQueueArguments["x-queue-version"] = 2;
        m_channel->declareQueue(queueName, flags, declareQueueArguments);
    }

    void bindQueue(const std::string& exchangeName, const std::string& queueName, const std::string& routingKey) {
        m_channel->bindQueue(exchangeName, queueName, routingKey);
    }

    void publish(const std::string& exchange, const std::string& routingKey, const std::string& message, const std::string& subject, const std::string& correlationId, const std::string& expiration) {
        AMQP::Envelope publishEnvelope(message);

        if(!correlationId.empty())
            publishEnvelope.setCorrelationID(correlationId);

        if(!expiration.empty())
            publishEnvelope.setExpiration(expiration);

        AMQP::Table headers;
        headers["subject"] = subject;
        publishEnvelope.setHeaders(headers);
        m_channel->publish(exchange, routingKey, publishEnvelope); // we don't need ack yet.
    }

private:
    void enableReplyTo() {
        if(m_enablingReplyTo || m_replyToActive)
            return;

        m_enablingReplyTo = true;
        m_channel->consume("amq.rabbitmq.reply-to", AMQP::noack)
            .onReceived([this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
                m_rpcMessages[message.correlationID()] = std::string(message.body(), message.bodySize());
            })
            .onSuccess([this](const std::string &consumertag) {
                m_replyToActive = true;
                m_enablingReplyTo = false;

                for(const auto& preReplyToMessage : m_preReplyToMessages) {
                    AMQP::Envelope publishEnvelope(preReplyToMessage->message);
                    publishEnvelope.setReplyTo("amq.rabbitmq.reply-to");
                    publishEnvelope.setCorrelationID(preReplyToMessage->correlationId);

                    if(!preReplyToMessage->expiration.empty())
                        publishEnvelope.setExpiration(preReplyToMessage->expiration);

                    AMQP::Table headers;
                    headers["subject"] = preReplyToMessage->subject;
                    publishEnvelope.setHeaders(headers);

                    m_channel->publish(preReplyToMessage->exchange, preReplyToMessage->routingKey, publishEnvelope);
                }

                m_preReplyToMessages.clear();
            })
            .onCancelled([this](const std::string &consumertag) {
                m_consumerCancelled = true;
            })
            .onError([this](const char *message) {
                m_channelError = true;
                m_channelErrorMessage = std::string(message);
            });
    }

    boost::asio::io_context m_io_context;
    AmqpHandler* m_handler;
    AMQP::TcpChannel* m_channel;
    AMQP::TcpConnection* m_connection;

    bool m_consumerCancelled = false;
    bool m_channelError = false;
    bool m_replyToActive = false;
    bool m_enablingReplyTo = false;
    bool m_failed = false;

    std::string m_channelErrorMessage;
    std::string m_address;

    std::list<AmqpMessagePtr> m_messages;
    std::list<AmqpMessagePtr> m_preReplyToMessages;
    std::unordered_map<std::string, std::string> m_rpcMessages;
};

std::string lua_pop_string(lua_State* L)
{
    size_t size = 0;
    const char* str = lua_tolstring(L, -1, &size);
    std::string ret;

    if(str)
        ret = std::string(str, size);

    lua_pop(L, 1);
    return ret;
}

double lua_pop_number(lua_State* L)
{
    double ret = lua_tonumber(L, -1);
    lua_pop(L, 1);
    return ret;
}

int Id = 0;
std::unordered_map<int, Amqp*> Amqps; 

int amqp_init(lua_State *L)
{
    std::string vtable = lua_pop_string(L);
    std::string password = lua_pop_string(L);
    std::string user = lua_pop_string(L);
    uint16_t port = lua_pop_number(L);
    std::string host = lua_pop_string(L);

    std::stringstream connectionString;
    connectionString << "amqp://" << user << ":" << password << "@" << host << ":" << port << vtable;
    Amqp* handler = new Amqp(connectionString.str());
    Amqps[++Id] = handler;

    lua_pushnumber(L, Id);
    return 1;
}

int amqp_consume(lua_State *L)
{
    int flags = lua_pop_number(L);
    std::string channelName = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        std::cerr << "amqp_consume !amqp (" << id << ")" << std::endl;
        return 0;
    }

    amqp->consume(channelName, flags);
    return 0;
}

int amqp_ack(lua_State *L)
{
    uint64_t ack = lua_pop_number(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        std::cerr << "amqp_ack !amqp (" << id << ")" << std::endl;
        return 0;
    }

    amqp->ack(ack);
    return 0;
}

int amqp_reject(lua_State *L)
{
    uint64_t ack = lua_pop_number(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        std::cerr << "amqp_reject !amqp (" << id << ")" << std::endl;
        return 0;
    }

    amqp->reject(ack);
    return 0;
}

int amqp_get_ready_message(lua_State *L)
{
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        std::cerr << "amqp_get_ready_message !amqp (" << id << ")" << std::endl;
        return 0;
    }

    auto& messages = amqp->getMessages();

    if(messages.empty()) {
        lua_pushboolean(L, false);
        return 1;
    }

    auto first = messages.front();
    messages.pop_front();

    lua_pushboolean(L, true);
    lua_pushlstring(L, first->message.c_str(), first->message.size());
    lua_pushnumber(L, first->ack);
    lua_pushstring(L, first->exchange.c_str());
    lua_pushstring(L, first->routingKey.c_str());
    lua_pushstring(L, first->replyTo.c_str());
    lua_pushstring(L, first->correlationId.c_str());
    lua_pushstring(L, first->subject.c_str());

    return 8;
}

int amqp_declare_exchange(lua_State *L)
{
    int flags = lua_pop_number(L);
    int mode = lua_pop_number(L);
    std::string exchangeName = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        std::cerr << "amqp_declare_exchange !amqp (" << id << ")" << std::endl;
        return 0;
    }

    amqp->declareExchange(exchangeName, mode, flags);
    return 0;
}

int amqp_declare_queue(lua_State *L)
{
    int flags = lua_pop_number(L);
    std::string queueName = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        std::cerr << "amqp_declare_queue !amqp (" << id << ")" << std::endl;
        return 0;
    }

    amqp->declareQueue(queueName, flags);
    return 0;
}

int amqp_bind_queue(lua_State *L)
{
    std::string key = lua_pop_string(L);
    std::string queueName = lua_pop_string(L);
    std::string exchangeName = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        std::cerr << "amqp_bind_queue !amqp (" << id << ")" << std::endl;
        return 0;
    }

    amqp->bindQueue(exchangeName, queueName, key);
    return 0;
}

int amqp_terminate(lua_State *L)
{
    int id = lua_pop_number(L);

    auto it = Amqps.find(id);
    if(it != Amqps.end()) {
        delete it->second;
        Amqps.erase(it);
    }

    std::cerr << "amqp_terminate !amqp (" << id << ")" << std::endl;
    return 0;
}

int amqp_poll(lua_State *L)
{
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        std::cerr << "amqp_poll !amqp (" << id << ")" << std::endl;
        lua_pushboolean(L, false);
        return 1;
    }

    std::string error;

    if(!amqp->poll(error)) {
        lua_pushboolean(L, false);
        lua_pushstring(L, error.c_str());
        return 2;
    }

    lua_pushboolean(L, true);
    return 1;
}

int amqp_publish(lua_State *L)
{
    //todo: need to be more reliable
    std::string expiration = lua_pop_string(L);
    std::string correlationId = lua_pop_string(L);
    std::string subject = lua_pop_string(L);
    std::string message = lua_pop_string(L);
    std::string routeId = lua_pop_string(L);
    std::string exchange = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        std::cerr << "amqp_publish !amqp (" << id << ")" << std::endl;
        return 0;
    }

    amqp->publish(exchange, routeId, message, subject, correlationId, expiration);
    return 0;
}

int amqp_publish_rpc(lua_State *L)
{
    std::string expiration = lua_pop_string(L);
    std::string subject = lua_pop_string(L);
    std::string message = lua_pop_string(L);
    std::string routeId = lua_pop_string(L);
    std::string exchange = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        std::cerr << "amqp_publish_rpc !amqp (" << id << ")" << std::endl;
        return 0;
    }

    std::string correlationId = amqp->publishRPC(exchange, routeId, message, subject, expiration);
    lua_pushstring(L, correlationId.c_str());
    return 1;
}

int amqp_is_ok(lua_State *L)
{
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp || amqp->hasFailed()) {
        lua_pushboolean(L, false);
        return 1;
    }

    lua_pushboolean(L, true);
    return 1;
}

int amqp_get_rpc_message(lua_State *L)
{
    std::string correlationId = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_pushboolean(L, true);
        return 1;
    }

    auto& rpcMessages = amqp->getRPCMessages();
    auto it = rpcMessages.find(correlationId);

    if(it != rpcMessages.end()) {
        lua_pushboolean(L, false);
        lua_pushboolean(L, true);
        lua_pushlstring(L, it->second.c_str(), it->second.size());

        rpcMessages.erase(it);
        return 3;
    }

    lua_pushboolean(L, false);
    lua_pushboolean(L, false);
    return 2;
}

/* ... more functions ... */

extern "C" {
int luaopen_amqpcpp(lua_State *L)
{
    lua_newtable(L);
    lua_pushcfunction(L, amqp_init);
    lua_setfield(L, -2, "init");

    lua_pushcfunction(L, amqp_terminate);
    lua_setfield(L, -2, "terminate");

    lua_pushcfunction(L, amqp_get_ready_message);
    lua_setfield(L, -2, "get_ready_message");

    lua_pushcfunction(L, amqp_consume);
    lua_setfield(L, -2, "consume");

    lua_pushcfunction(L, amqp_publish);
    lua_setfield(L, -2, "publish");

    lua_pushcfunction(L, amqp_publish_rpc);
    lua_setfield(L, -2, "publish_rpc");

    lua_pushcfunction(L, amqp_is_ok);
    lua_setfield(L, -2, "is_ok");

    lua_pushcfunction(L, amqp_get_rpc_message);
    lua_setfield(L, -2, "get_rpc_message");

    lua_pushcfunction(L, amqp_ack);
    lua_setfield(L, -2, "ack");

    lua_pushcfunction(L, amqp_reject);
    lua_setfield(L, -2, "reject");

    lua_pushcfunction (L, amqp_poll);
    lua_setfield(L, -2, "poll");

    lua_pushcfunction (L, amqp_declare_exchange);
    lua_setfield(L, -2, "declare_exchange");

    lua_pushcfunction (L, amqp_declare_queue);
    lua_setfield(L, -2, "declare_queue");

    lua_pushcfunction (L, amqp_bind_queue);
    lua_setfield(L, -2, "bind_queue");

    return 1;
}
}
