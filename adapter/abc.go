package adapter

import (
	"encoding/json"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	stan "github.com/nats-io/stan.go"
)

// MsgPair 消息和消息的所属 Topic
type MsgPair interface {
	// Topic 返回消息所属主题
	Topic() string
	// Data 返回消息的内容
	Data() interface{}
	// Ack acknowledge
	Ack() error
	// Nack not acknowledge
	Nack() error
	// // Reject 拒绝
	// Reject()
}

// ConnectCloser 00
type ConnectCloser interface {
	// Connect 连接到 Broker
	Connect() error
	// Close 断开与 Broker 的连接
	Close() error
	// IsClosed 判断当前连接是否断开
	IsClosed() bool
}

// Subscriber 消息的消费者
type Subscriber interface {

	// Subscribe 订阅 Topic
	Subscribe(topics ...string) error

	// Unsubscribe 取消订阅 Topic
	Unsubscribe(topics ...string) error

	// // AllTopics 获取 Broker 上的所有 Topic
	// AllTopics() (topics []string, err error)

	// GetChan 获取用于读取的 Channel
	GetChan() <-chan MsgPair
}

// Publisher 消息的发布者
type Publisher interface {
	// Publish 发布消息
	Publish(topic string, message interface{}) error
}

// MessageClient 00
type MessageClient interface {
	ConnectCloser
	Subscriber
	Publisher
}

// Message 从 Broker 收取的普通消息
type Message struct {
	topic string
	data  interface{}
}

// Topic 返回消息所属主题
func (m *Message) Topic() string {
	return m.topic
}

// Data 返回消息的内容
func (m *Message) Data() interface{} {
	return m.data
}

// Ack acknowledge
func (m *Message) Ack() error {
	// Redis Pub/Sub 不支持 ack/nack
	return nil
}

// Nack not acknowledge
func (m *Message) Nack() error {
	// Redis Pub/Sub 不支持 ack/nack
	return nil
}

// CreateClient 通过 uri 获取所需要的 MessageClient
func CreateClient(uri string) MessageClient {
	if strings.Index(uri, "redis://") == 0 {
		return &RedisMessageClient{
			URI: uri,
			mu:  new(sync.RWMutex),
		}
	} else if strings.Index(uri, "amqp://") == 0 {
		return &AmqpMessageClient{
			URI:   uri,
			Queue: "streaming-porter",
			mu:    new(sync.RWMutex),
		}

	} else if strings.Index(uri, "nats://") == 0 {
		return &NatsStreamingClient{
			URI:       uri,
			ClusterID: "test-cluster",
			ClientID:  "streaming-porter",
			subMap:    make(map[string]stan.Subscription),
			channel:   make(chan MsgPair, 1),
			mu:        new(sync.RWMutex),
		}
	} else if strings.Index(uri, "mqtt://") == 0 {
		uri := strings.Replace(uri, "mqtt", "tcp", 1)
		opt := mqtt.NewClientOptions()
		opt.AddBroker(uri)
		opt.ClientID = "streaming-porter"
		return &MqttStreamingClient{
			URI: uri,
			opt: opt,
			mu:  new(sync.RWMutex),
		}
	}
	return nil
}

// Marshal 序列化消息
func Marshal(message interface{}) (buff []byte, err error) {
	switch message.(type) {
	case []byte:
		return message.([]byte), nil
	case string:
		buff = []byte(message.(string))
	default:
		buff, err = json.Marshal(message)
		return
	}
	return
}
