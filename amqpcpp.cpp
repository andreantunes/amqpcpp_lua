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
#include <unordered_set>

extern "C" {
  #include "lua.h"
  #include "lua.h"
  #include "lauxlib.h"
}

enum {
    s_table = 1,
    s_number = 2,
    s_string = 3,
    s_boolean = 4,
    s_settable = 5,
    s_lstring = 6
};

void lua_push_error(lua_State* L, const std::string& error)
{
    lua_pushstring(L, error.c_str());
    lua_error(L);
}

int64_t getMsTicks() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string generateUniqueString()
{
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    std::stringstream ss;
    ss << uuid;
    return ss.str();
}

void tableValueToStream(lua_State* L, std::ostream& stream, int index)
{
    int type = lua_type(L, index);
    if(type == LUA_TBOOLEAN) {
        char k = s_boolean;
        bool b = lua_toboolean(L, index);
        stream.write(&k, 1);
        stream.write((char*)&b, 1);

    } else if(type == LUA_TNUMBER) {
        char k = s_number;
        stream.write(&k, 1);
        double value = lua_tonumber(L, index);
        stream.write((char*)&value, sizeof(double));

    } else if(type == LUA_TSTRING) {
        size_t size = 0;
        const char* str = lua_tolstring(L, index, &size);

        if(size > 0xffff) {
            char k = s_lstring;
            stream.write(&k, 1);
            stream.write((char*)&size, 4);

        } else {
            char k = s_string;
            stream.write(&k, 1);
            stream.write((char*)&size, 2);
        }

        stream.write(str, size);
    } else {
        lua_push_error(L, "!tableValueToStream(L, stream, -1)");
    }
}

bool hasValidLuaType(lua_State* L, int index, bool isKey)
{
    int type = lua_type(L, index);

    return type == LUA_TBOOLEAN ||
        type == LUA_TNUMBER ||
        type == LUA_TSTRING ||
        (!isKey && type == LUA_TTABLE);
}

std::string getLuaTypeName(lua_State *L, int index)
{
    const char *ret = lua_typename(L, lua_type(L, index));
    return std::string(ret ? ret : "?");
}

void tableToStream(lua_State* L, std::ostream& stream)
{
    int oldTop = lua_gettop(L);

    lua_pushnil(L);

    while(lua_next(L, -2) != 0) {
        /* uses 'key' (at index -2) and 'value' (at index -1) */
        if(hasValidLuaType(L, -2, true) && hasValidLuaType(L, -1, false)) {
            tableValueToStream(L, stream, -2);

            if(lua_istable(L, -1)) {
                char t = s_table;
                stream.write(&t, 1);
                tableToStream(L, stream);

            } else
                tableValueToStream(L, stream, -1);

            char t = s_settable;
            stream.write(&t, 1);

        } else {
            std::stringstream err;
            err << "tableToStream trying to write an invalid luaType " << getLuaTypeName(L, -2) << " = " << getLuaTypeName(L, -1);
            lua_push_error(L, err.str());
        }

        lua_pop(L, 1);
    }

    int newTop = lua_gettop(L);

    if(newTop != oldTop) {
        lua_push_error(L, "tableToStream newTop != oldTop" );
    }
}

bool getUChar(const char* stream, size_t& size, unsigned char& attr, size_t maxSize)
{
    if(size >= maxSize)
        return false;

    attr = *(unsigned char*)(stream + size);
    size++;
    return true;
}

bool getDouble(const char* stream, size_t& size, double& attr, size_t maxSize)
{
    if(size + sizeof(double) >= maxSize)
        return false;

    attr = *((double*)(stream + size));
    size += sizeof(double);
    return true;
}

bool getLString(const char* stream, size_t& size, std::string& attr, size_t maxSize)
{
    if(size + 4 >= maxSize)
        return false;

    uint32_t stringSize = *((uint32_t*)(stream + size));
    size += 4;

    if(size + stringSize >= maxSize)
        return false;

    if(stringSize > 0) {
        attr = std::string(stream + size, stringSize);
        size += stringSize;
    }

    return true;
}

bool getString(const char* stream, size_t& size, std::string& attr, size_t maxSize)
{
    if(size + 2 >= maxSize)
        return false;

    uint16_t stringSize = *((uint16_t*)(stream + size));
    size += 2;

    if(size + stringSize >= maxSize)
        return false;

    if(stringSize > 0) {
        attr = std::string(stream + size, stringSize);
        size += stringSize;
    }

    return true;
}

void streamToTable(const char* stream, lua_State* L, size_t& size, size_t maxSize)
{
    lua_newtable(L);

    int oldTop = lua_gettop(L);
    unsigned char attr;

    while(getUChar(stream, size, attr, maxSize)) {
        if(attr == 255) // this is just to signal that we are using stream
            continue;

        if(attr == s_table) {
            lua_newtable(L);

        } else if(attr == s_number) {
            double value;

            if(!getDouble(stream, size, value, maxSize)) {
                lua_push_error(L, "streamToTable !stream.getDouble(value) ");
                break;
            }

            lua_pushnumber(L, value);

        } else if(attr == s_lstring) {
            std::string value;

            if(!getLString(stream, size, value, maxSize)) {
                lua_push_error(L, "streamToTable !stream.getLString(value) ");
                break;
            }

            lua_pushlstring(L, value.c_str(), value.size());

        } else if(attr == s_string) {
            std::string value;

            if(!getString(stream, size, value, maxSize)) {
                lua_push_error(L, "streamToTable !stream.getString(value) ");
                break;
            }

            lua_pushlstring(L, value.c_str(), value.size());

        } else if(attr == s_boolean) {
            uint8_t value;

            if(!getUChar(stream, size, value, maxSize)) {
                lua_push_error(L, "streamToTable !stream.getUChar(value) ");
                break;
            }

            lua_pushboolean(L, value);

        } else if(attr == s_settable) {
            lua_settable(L, -3);

        } else {
            std::stringstream ss;
            ss << "streamToTable unknown attr " << attr << ";";
            lua_push_error(L, ss.str());
            break;
        }
    }

    int newTop = lua_gettop(L);

    if(newTop - oldTop > 0) {
        lua_push_error(L, "streamToTable newTop - oldTop > 0");

    } else if(newTop != oldTop) {
        lua_push_error(L, "streamToTable newTop != oldTop");
    }
}

class AmqpHandler : public AMQP::LibBoostAsioHandler
{
private:
    virtual void onError(AMQP::TcpConnection *connection, const char *message) override {
        error = true;
        errorMessage = std::string(message);
    }

    virtual void onReady(AMQP::TcpConnection *connection) override
    {
        ready = true;
    }

public:
    bool error = false;
    bool ready = false;
    std::string errorMessage;

    AmqpHandler(boost::asio::io_context &io_context) : AMQP::LibBoostAsioHandler(io_context) {}

    virtual ~AmqpHandler() = default;
};

struct AmqpMessage
{
    std::string dupUuid;
    std::string message;
    std::string exchange;
    std::string routingKey;
    std::string correlationId;
    std::string replyTo;
    std::string subject;
    std::string expiration;
    uint64_t ack;
    uint64_t dupOrderId = 0;
};

typedef std::shared_ptr<AmqpMessage> AmqpMessagePtr;
class Amqp
{
public:
    Amqp(const std::string& address) {
        m_address = address;
        m_uuid = generateUniqueString();
    }

    void start() {
        m_io_context = new boost::asio::io_context;
        m_handler = new AmqpHandler(*m_io_context);
        m_connection = new AMQP::TcpConnection(m_handler, AMQP::Address(m_address));
        m_channel = new AMQP::TcpChannel(m_connection);

        m_channel->onError([this](const char* message) {
            m_channelError = true;
            m_channelErrorMessage = message;
        });
    }

    void terminate() {
        //this order matters
        delete m_channel;
        m_channel = NULL;

        delete m_connection;
        m_connection = NULL;

        delete m_handler;
        m_handler = NULL;

        delete m_io_context;
        m_io_context = NULL;

        m_consumerCancelled = false;
        m_channelError = false;
        m_replyToActive = false;
        m_enablingReplyTo = false;
        m_failed = false;

        m_channelErrorMessage = "";
        m_messages.clear();
        m_preReplyToMessages.clear();
        m_rpcMessages.clear();
    }

    bool poll(std::string& error) {
        if(m_failed)
            return false;

        m_io_context->poll();
        m_io_context->reset();

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

    bool started() {
        return !!m_io_context;
    }

    bool ready() {
        return m_handler->ready;
    }

    void consume(const std::string& queueName, int flags) {
        if(m_useDedupAlgorithm)
            flags += AMQP::exclusive;

        m_channel->consume(queueName, flags)
            .onReceived([this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
                const AMQP::Table& headers = message.headers();
                const std::string& subject = headers["subject"];
                const std::string& dupUuid = headers["dup-uuid"];

                AmqpMessagePtr amqpMessage = AmqpMessagePtr(new AmqpMessage);
                amqpMessage->message = std::string(message.body(), message.bodySize());
                amqpMessage->exchange = message.exchange();
                amqpMessage->routingKey = message.routingkey();
                amqpMessage->replyTo = message.replyTo();
                amqpMessage->correlationId = message.correlationID();
                amqpMessage->ack = deliveryTag;
                amqpMessage->subject = subject;
                amqpMessage->dupUuid = dupUuid;
                amqpMessage->dupOrderId = headers["dup-order-id"];

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

    AmqpMessagePtr getMessage() {
        if(m_messages.empty())
            return nullptr;

        auto message = m_messages.front();
        m_messages.pop_front();

        bool parseMessage = true;

        if(m_useDedupAlgorithm) {
            if(!message->dupUuid.empty()) {
                auto it = m_lastReadMessageFromPublisherUuid.find(message->dupUuid);

                if(it != m_lastReadMessageFromPublisherUuid.end()) {
                    if(message->dupOrderId > it->second)
                        it->second = message->dupOrderId;

                    else
                        parseMessage = false;

                } else
                    m_lastReadMessageFromPublisherUuid[message->dupUuid] = message->dupOrderId;
            }
        }

        if(!parseMessage) {
            ack(message->ack);
            return nullptr;
        }

        return message;
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
    std::unordered_set<std::string>& getAutoConvertMessageToStream() { return m_autoConvertMessageToStream; }

    void declareExchange(const std::string& exchangeName, int mode, int flags) {
        m_channel->declareExchange(exchangeName, AMQP::ExchangeType(mode), flags);
    }

    void declareQueue(const std::string& queueName, int flags, int timeout) {
        AMQP::Table declareQueueArguments;
        //declareQueueArquments["x-queue-type"] = "quorum";
        declareQueueArguments["x-queue-version"] = 2;

        if(timeout > 0)
            declareQueueArguments["x-expires"] = timeout;

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
        headers["dup-uuid"] = m_uuid;
        headers["dup-order-id"] = ++m_publishMessageOrderId;
        headers["sent-at"] = getMsTicks();

        publishEnvelope.setHeaders(headers);
        m_channel->publish(exchange, routingKey, publishEnvelope); // we don't need ack yet.
    }

    void enableAutoConvertMessageToStream(std::string& subject) { m_autoConvertMessageToStream.insert(subject); }
    void enableDedupAlgorithm() { m_useDedupAlgorithm = true; }

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

    boost::asio::io_context* m_io_context;
    AmqpHandler* m_handler;
    AMQP::TcpChannel* m_channel;
    AMQP::TcpConnection* m_connection;

    bool m_consumerCancelled = false;
    bool m_channelError = false;
    bool m_replyToActive = false;
    bool m_enablingReplyTo = false;
    bool m_failed = false;
    bool m_useDedupAlgorithm = false;

    uint64_t m_publishMessageOrderId = 0;

    std::unordered_set<std::string> m_autoConvertMessageToStream;
    std::unordered_map<std::string, uint64_t> m_lastReadMessageFromPublisherUuid;

    std::string m_channelErrorMessage;
    std::string m_address;
    std::string m_uuid;

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

int lua_amqp_create(lua_State *L)
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

int lua_amqp_is_ready(lua_State *L)
{
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_is_ready");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    lua_pushboolean(L, amqp->ready());
    return 1;
}

int lua_amqp_consume(lua_State *L)
{
    int flags = lua_pop_number(L);
    std::string channelName = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_consume");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    amqp->consume(channelName, flags);
    return 0;
}

int lua_amqp_ack(lua_State *L)
{
    uint64_t ack = lua_pop_number(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_ack");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    amqp->ack(ack);
    return 0;
}

int lua_amqp_reject(lua_State *L)
{
    uint64_t ack = lua_pop_number(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_reject");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    amqp->reject(ack);
    return 0;
}

int lua_amqp_get_ready_message(lua_State *L)
{
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_get_ready_message");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    auto first = amqp->getMessage();

    if(!first) {
        lua_pushboolean(L, false);
        return 1;
    }

    lua_pushboolean(L, true);

    auto& autoConvertMessageToStream = amqp->getAutoConvertMessageToStream();
    
    if(autoConvertMessageToStream.count(first->subject)) {
        size_t sizePos = 0;
        streamToTable(first->message.c_str(), L, sizePos, first->message.size());
    } else
        lua_pushlstring(L, first->message.c_str(), first->message.size());

    lua_pushnumber(L, first->ack);
    lua_pushstring(L, first->exchange.c_str());
    lua_pushstring(L, first->routingKey.c_str());
    lua_pushstring(L, first->replyTo.c_str());
    lua_pushstring(L, first->correlationId.c_str());
    lua_pushstring(L, first->subject.c_str());

    return 8;
}

int lua_amqp_declare_exchange(lua_State *L)
{
    int flags = lua_pop_number(L);
    int mode = lua_pop_number(L);
    std::string exchangeName = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_declare_exchange");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    amqp->declareExchange(exchangeName, mode, flags);
    return 0;
}

int lua_amqp_declare_queue(lua_State *L)
{
    int nArgs = lua_gettop(L);
    int timeout = 0;

    if(nArgs > 3)
        timeout = lua_pop_number(L);

    int flags = lua_pop_number(L);
    std::string queueName = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_declare_queue");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    amqp->declareQueue(queueName, flags, timeout);
    return 0;
}

int lua_amqp_bind_queue(lua_State *L)
{
    std::string queueName = lua_pop_string(L);
    std::string key = lua_pop_string(L);
    std::string exchangeName = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_bind_queue");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    amqp->bindQueue(exchangeName, queueName, key);
    return 0;
}

int lua_amqp_start(lua_State *L)
{
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_start");
        return 0;
    }

    amqp->start();
    return 0;
}

int lua_amqp_enable_auto_convert_message_to_stream(lua_State *L)
{
    std::string subject = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_enable_auto_convert_message_to_stream");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    amqp->enableAutoConvertMessageToStream(subject);
    return 0;
}

int lua_amqp_enable_dedup_algorithm(lua_State *L)
{
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_enable_dedup_algorithm");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    amqp->enableDedupAlgorithm();
    return 0;
}

int lua_amqp_poll(lua_State *L)
{
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_poll");
        return 1;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
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

int lua_amqp_publish(lua_State *L)
{
    //todo: need to be more reliable
    std::string expiration = lua_pop_string(L);
    std::string correlationId = lua_pop_string(L);
    std::string message = lua_pop_string(L);
    std::string subject = lua_pop_string(L);
    std::string routeId = lua_pop_string(L);
    std::string exchange = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_publish");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    if(subject.size() > 1024) {
        std::stringstream ss;
        ss << "subject.size() > 1024 " << subject.size();
        lua_push_error(L, ss.str());
        return 0;
    }

    amqp->publish(exchange, routeId, message, subject, correlationId, expiration);
    return 0;
}

int lua_amqp_publish_rpc(lua_State *L)
{
    std::string expiration = lua_pop_string(L);
    std::string message = lua_pop_string(L);
    std::string subject = lua_pop_string(L);
    std::string routeId = lua_pop_string(L);
    std::string exchange = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_publish");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    std::string correlationId = amqp->publishRPC(exchange, routeId, message, subject, expiration);
    lua_pushstring(L, correlationId.c_str());
    return 1;
}

int lua_amqp_is_ok(lua_State *L)
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

int lua_amqp_get_rpc_message(lua_State *L)
{
    std::string correlationId = lua_pop_string(L);
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_get_rpc_message");
        return 1;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
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

int lua_amqp_terminate(lua_State *L)
{
    int id = lua_pop_number(L);

    Amqp* amqp = Amqps[id];
    if(!amqp) {
        lua_push_error(L, "!amqp lua_amqp_terminate");
        return 0;
    }

    if(!amqp->started()) {
        lua_push_error(L, "!amqp started");
        return 0;
    }

    amqp->terminate();
    return 0;
}

int lua_table_to_stream(lua_State *L)
{
    if(!lua_istable(L, -1)) {
        lua_push_error(L, "lua_table_to_stream !lua_istable(L, -1)");
        return 0;
    }

    struct StringStreamBuf : public std::streambuf {
        std::streamsize xsputn(const char_type* s, std::streamsize n) {
            buffer.append(s, n);
            return n;
        }

        std::string buffer;
    };

    static StringStreamBuf stringStreamBuf;
    std::ostream stream(&stringStreamBuf);
    char c = 255;
    stream.write(&c, 1);
    tableToStream(L, stream);

    lua_pop(L, 1);
    lua_pushlstring(L, stringStreamBuf.buffer.c_str(), stringStreamBuf.buffer.size());
    stringStreamBuf.buffer.clear();
    return 1;
}

int lua_stream_to_table(lua_State *L)
{
    if(!lua_isstring(L, -1)) {
        lua_push_error(L, "stream_to_table !lua_isstring");
        return 0;
    }

    std::string str = lua_pop_string(L);
    size_t curSize = 0;
    streamToTable(str.c_str(), L, curSize, str.size());
    return 1;
}

/* ... more functions ... */

extern "C" {
int luaopen_amqpcpp(lua_State *L)
{
    lua_newtable(L);
    lua_pushcfunction(L, lua_amqp_create);
    lua_setfield(L, -2, "create");

    lua_pushcfunction (L, lua_amqp_start);
    lua_setfield(L, -2, "start");

    lua_pushcfunction(L, lua_amqp_is_ready);
    lua_setfield(L, -2, "is_ready");

    lua_pushcfunction(L, lua_amqp_terminate);
    lua_setfield(L, -2, "terminate");

    lua_pushcfunction(L, lua_amqp_get_ready_message);
    lua_setfield(L, -2, "get_ready_message");

    lua_pushcfunction(L, lua_amqp_consume);
    lua_setfield(L, -2, "consume");

    lua_pushcfunction(L, lua_amqp_publish);
    lua_setfield(L, -2, "publish");

    lua_pushcfunction(L, lua_amqp_publish_rpc);
    lua_setfield(L, -2, "publish_rpc");

    lua_pushcfunction(L, lua_amqp_is_ok);
    lua_setfield(L, -2, "is_ok");

    lua_pushcfunction(L, lua_amqp_get_rpc_message);
    lua_setfield(L, -2, "get_rpc_message");

    lua_pushcfunction(L, lua_amqp_ack);
    lua_setfield(L, -2, "ack");

    lua_pushcfunction(L, lua_amqp_reject);
    lua_setfield(L, -2, "reject");

    lua_pushcfunction (L, lua_amqp_poll);
    lua_setfield(L, -2, "poll");

    lua_pushcfunction (L, lua_amqp_declare_exchange);
    lua_setfield(L, -2, "declare_exchange");

    lua_pushcfunction (L, lua_amqp_declare_queue);
    lua_setfield(L, -2, "declare_queue");

    lua_pushcfunction (L, lua_amqp_bind_queue);
    lua_setfield(L, -2, "bind_queue");

    lua_pushcfunction (L, lua_amqp_enable_auto_convert_message_to_stream);
    lua_setfield(L, -2, "enable_auto_convert_message_to_stream");

    lua_pushcfunction (L, lua_amqp_enable_dedup_algorithm);
    lua_setfield(L, -2, "enable_dedup_algorithm");

    lua_pushcfunction (L, lua_table_to_stream);
    lua_setfield(L, -2, "table_to_stream");

    lua_pushcfunction (L, lua_stream_to_table);
    lua_setfield(L, -2, "stream_to_table");

    return 1;
}
}
