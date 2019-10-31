package adapter

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"gitlab.jiangxingai.com/applications/base-modules/internal-sdk/go-utils/logger"
	"sync"
	"time"

	stan "github.com/nats-io/stan.go"
	pb "github.com/nats-io/stan.go/pb"
)

type natsStreamingChannel struct {
	// {
	// 	"cluster_id": "test-cluster",
	// 	"server_id": "PW2dC0vUNWC8YDAAS5TeyH",
	// 	"now": "2019-08-20T08:55:31.728499877Z",
	// 	"offset": 0,
	// 	"limit": 1024,
	// 	"count": 1,
	// 	"total": 1,
	// 	"names": [
	// 	"hello"
	// 	]
	// 	}

	ClusterID string   `json:"cluster_id"`
	ServerID  string   `json:"server_id"`
	Now       string   `json:"now"`
	Offset    int      `json:"offset"`
	Limit     int      `json:"limit"`
	Count     int      `json:"count"`
	Total     int      `json:"total"`
	Names     []string `json:"names"`
}

// NatsMessage amqp 协议的消息体
type NatsMessage struct {
	Message
	Delivery *stan.Msg
}

// Ack acknowledge
func (m *NatsMessage) Ack() error {
	return transformError(m.Delivery.Ack())
}

// Nack not acknowledge
func (m *NatsMessage) Nack() error {
	return nil
}

// NatsStreamingClient nats-streaming 协议的 MessageClient 实现
// c *NatsStreamingClient adapter.MessageClient
type NatsStreamingClient struct {
	URI       string
	conn      stan.Conn
	ClusterID string
	ClientID  string
	subMap    map[string]stan.Subscription
	channel   chan MsgPair
	mu        *sync.RWMutex
}

// Connect 连接到 Broker
func (c *NatsStreamingClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		opt := stan.NatsURL(c.URI)
		stan.ConnectWait(time.Second * 100)
		conn, err := stan.Connect(c.ClusterID, c.ClientID, opt)

		if err != nil {
			return transformError(err)
		}
		c.conn = conn
	}

	return nil
}

// Close 断开与 Broker 的连接
func (c *NatsStreamingClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		if c.channel != nil {
			// c.channel <- nil
			close(c.channel)
			c.channel = nil
		}
		return transformError(err)
	}
	return nil
}

// IsClosed 判断当前连接是否断开
func (c *NatsStreamingClient) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		conn := c.conn.NatsConn()
		if conn != nil {
			return conn.IsClosed()
		}
	}
	return true
}

func (c *NatsStreamingClient) getConn() stan.Conn {

	if c.conn == nil {
		for {
			if err := c.Connect(); err == nil {
				break
			}
		}
	}
	return c.conn
}

func (c *NatsStreamingClient) onMessage(msg *stan.Msg) {
	if msg != nil {
		c.channel <- &NatsMessage{
			Message: Message{
				topic: msg.Subject,
				data:  msg.Data,
			},
			Delivery: msg,
		}
	}
}

// Subscribe 订阅 Topic
func (c *NatsStreamingClient) Subscribe(topics ...string) error {
	for _, topic := range topics {
		sub, err := c.getConn().QueueSubscribe(
			topic,
			c.ClientID,
			c.onMessage,
			stan.DurableName(fmt.Sprintf("porter-%s", topic)),
			stan.StartAt(pb.StartPosition_First),
			stan.AckWait(time.Second*5),
			stan.StartAt(pb.StartPosition_First),
			stan.SetManualAckMode(),
		)
		if err != nil {
			return err
		}
		c.subMap[topic] = sub
	}
	return nil
}

// Unsubscribe 取消订阅 Topic
func (c *NatsStreamingClient) Unsubscribe(topics ...string) error {
	for _, topic := range topics {
		sub := c.subMap[topic]
		if sub != nil {
			if err := sub.Unsubscribe(); err != nil {
				return transformError(err)
			}
		}
		delete(c.subMap, topic)
	}
	return nil
}

// AllTopics 获取 Broker 上的所有 Topic
func (c *NatsStreamingClient) AllTopics() (topics []string, err error) {
	uri, err := url.Parse(c.URI)

	if err != nil {
		logger.Fatal(err)
	}

	port, err := strconv.Atoi(uri.Port())
	if err != nil {
		port = 8222
	}

	URL := fmt.Sprintf("http://%s:%d/streaming/channelsz?subz=1", uri.Hostname(), port+4000)

	req, err := http.NewRequest("GET", URL, nil)

	cli := &http.Client{}
	resp, err := cli.Do(req)
	if err != nil {
		logger.Fatal(err)
	}

	defer resp.Body.Close()

	buff, err := ioutil.ReadAll(resp.Body)

	channels := natsStreamingChannel{}
	err = json.Unmarshal(buff, &channels)
	if err != nil {
		logger.Fatal(err)
	}

	return channels.Names, nil
}

// GetChan 获取用于读取的 Channel
func (c *NatsStreamingClient) GetChan() <-chan MsgPair {
	c.getConn()
	logger.Info("Make Chan:", c.URI)
	return c.channel
}

// Publish 发布消息
func (c *NatsStreamingClient) Publish(topic string, message interface{}) error {
	b, err := Marshal(message)
	if err != nil {
		return err
	}
	return transformError(c.getConn().Publish(topic, b))
}

func transformError(err error) error {
	switch err {
	case nil:
		return nil
	case stan.ErrConnectReqTimeout:
		return ErrConnectReqTimeout
	case stan.ErrCloseReqTimeout:
		return ErrCloseReqTimeout
	case stan.ErrSubReqTimeout:
		return ErrSubReqTimeout
	case stan.ErrUnsubReqTimeout:
		return ErrUnsubReqTimeout
	case stan.ErrConnectionClosed:
		return ErrConnectionClosed
	case stan.ErrTimeout:
		return ErrTimeout
	case stan.ErrBadAck:
		return ErrBadAck
	case stan.ErrBadSubscription:
		return ErrBadSubscription
	case stan.ErrBadConnection:
		return ErrBadConnection
	case stan.ErrManualAck:
		return ErrManualAck
	case stan.ErrNilMsg:
		return ErrNilMsg
	case stan.ErrNoServerSupport:
		return ErrNoServerSupport
	case stan.ErrMaxPings:
		return ErrMaxPings
	}
	return err
}
