package adapter

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"

	"github.com/streadway/amqp"
	"gitlab.jiangxingai.com/applications/base-modules/internal-sdk/go-utils/logger"
)

const defaultExchange = ""

// const (
// 	amqpExchangeCreatedTopic = "exchange.created"
// 	amqpEventExchange        = "amq.rabbitmq.event"
// )

const (
	exchangeTypeDirect = "direct"
	exchangeTypeFanout = "fanout"
)

type amqpExchangeStruct struct {
	// arguments                 string `json:"arguments"`
	AutoDelete             bool   `json:"auto_delete"`
	Durable                bool   `json:"durable"`
	Internal               bool   `json:"internal"`
	Name                   string `json:"name"`
	Type                   string `json:"type"`
	UserWhoPerformedAction string `json:"user_who_performed_action"`
	Vhost                  string `json:"vhost"`
}

// AMQPMessage amqp 协议的消息体
type AMQPMessage struct {
	Message
	Delivery *amqp.Delivery
}

// Ack acknowledge
func (m *AMQPMessage) Ack() error {
	return m.Delivery.Ack(false)
}

// Nack not acknowledge
func (m *AMQPMessage) Nack() error {
	return m.Delivery.Nack(false, true)
}

// AmqpMessageClient 处理 AMQP 协议的消息接收发送。
// 进支持 AMQP 0.9.1 协议
type AmqpMessageClient struct {
	URI        string
	Queue      string
	channel    chan MsgPair
	conn       *amqp.Connection
	pubChannel *amqp.Channel
	subChannel *amqp.Channel
	mu         *sync.RWMutex
}

// Connect bala
// c *AmqpMessageClient adapter.MessageClient
func (c *AmqpMessageClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// TODO: 断线重连
	if c.conn != nil {
		return nil
	}
	connection, err := amqp.Dial(c.URI)
	if err == nil {
		c.conn = connection
	}
	return err
}

// Close bala
func (c *AmqpMessageClient) Close() error {
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
		return err
	}
	return nil
}

// IsClosed 判断当前连接是否断开
func (c *AmqpMessageClient) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn.IsClosed()
	}
	return true
}

// PubChannel bala
func (c *AmqpMessageClient) PubChannel() *amqp.Channel {
	if c.pubChannel != nil {
		return c.pubChannel
	}
	if c.conn == nil {
		for {
			err := c.Connect()
			if err == nil {
				break
			}
		}
	}
	channel, err := c.conn.Channel()
	if err == nil {
		c.pubChannel = channel
	}
	return channel
}

// SubChannel bala
func (c *AmqpMessageClient) SubChannel() *amqp.Channel {
	if c.subChannel != nil {
		return c.subChannel
	}
	if c.conn == nil {
		for {
			err := c.Connect()
			if err == nil {
				break
			}
		}
	}
	channel, err := c.conn.Channel()
	if err == nil {
		c.subChannel = channel
		channel.QueueDeclare(c.Queue, true, false, false, false, nil)
		// // TODO: 监听事件
		// err = c.subChannel.QueueBind(c.Queue, amqpExchangeCreatedTopic, amqpEventExchange, false, nil)
		// if err != nil {
		// 	if amqpError, ok := err.(*amqp.Error); ok {
		// 		logger.Info(amqpError)
		// 		channel, err = c.conn.Channel()
		// 	} else {
		// 		logger.Info(err)
		// 	}
		// }
	}
	return channel
}

// Subscribe bala
func (c *AmqpMessageClient) Subscribe(topics ...string) error {
	channel := c.SubChannel()
	for _, topic := range topics {

		// TODO
		err := channel.QueueBind(c.Queue, "", topic, false, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// Unsubscribe bala
func (c *AmqpMessageClient) Unsubscribe(topics ...string) error {
	channel := c.SubChannel()
	for _, topic := range topics {

		// TODO
		err := channel.QueueUnbind(c.Queue, "", topic, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// AllTopics bala
func (c *AmqpMessageClient) AllTopics() (topics []string, err error) {
	uri, _ := amqp.ParseURI(c.URI)

	auth := uri.AMQPlainAuth()

	URL := fmt.Sprintf("http://%s:%d/api/exchanges/%s", uri.Host, uri.Port+10000, url.QueryEscape(uri.Vhost))

	req, err := http.NewRequest("GET", URL, nil)
	req.SetBasicAuth(auth.Username, auth.Password)

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	cli := &http.Client{}
	resp, err := cli.Do(req)
	if err != nil {
		logger.Fatal(err)
	}

	defer resp.Body.Close()

	buff, err := ioutil.ReadAll(resp.Body)

	exchanges := make([]amqpExchangeStruct, 1)
	err = json.Unmarshal(buff, &exchanges)
	if err != nil {
		logger.Fatal(err)
	}

	topics = make([]string, len(exchanges))
	i := 0
	for _, exchange := range exchanges {
		if exchange.Type == exchangeTypeFanout {
			topics[i] = exchange.Name
			i++
		}
	}

	if i == 0 {
		return nil, nil
	}
	return topics[:i], nil
}

// GetChan bala
func (c *AmqpMessageClient) GetChan() <-chan MsgPair {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel == nil {
		logger.Info("Make Chan:", c.URI)
		c.channel = make(chan MsgPair, 1)
		go func() {
			amqChan, err := c.SubChannel().Consume(c.Queue, c.Queue, false, false, true, false, nil)

			if err != nil {
				logger.Info(err)
			}

			for {
				select {
				case msg := <-amqChan:

					if msg.DeliveryMode == amqp.Transient {
						continue
					}
					if msg.Exchange == "" {
						if c.conn.IsClosed() {
							logger.Info("Connection Closed")
							c.Close()
							return
						}
						continue
					}

					c.channel <- &AMQPMessage{
						Message: Message{
							topic: msg.Exchange,
							data:  msg.Body,
						},
						Delivery: &msg,
					}

				}
			}
		}()
	}

	return c.channel
}

// Publish 发布消息
func (c *AmqpMessageClient) Publish(topic string, message interface{}) error {
	b, err := Marshal(message)
	if err != nil {
		return err
	}
	err = c.PubChannel().Publish(topic, "", true, true, amqp.Publishing{
		Body:         b,
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
	})

	return err
}
