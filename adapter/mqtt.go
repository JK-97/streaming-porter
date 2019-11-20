package adapter

import (
	"sync"

	"gitlab.jiangxingai.com/applications/base-modules/internal-sdk/go-utils/logger"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MqttStreamingClient mqtt 协议的 MessageClient 实现
type MqttStreamingClient struct {
	URI    string
	opt    *mqtt.ClientOptions
	client mqtt.Client
	ch     chan MsgPair
	mu     *sync.RWMutex
}

// Connect 连接到 Broker
func (c *MqttStreamingClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	if c.client != nil {
		err = c.Close()
	}
	if err != nil {
		return err
	}
	cli := mqtt.NewClient(c.opt)
	token := cli.Connect()
	token.Wait()
	err = token.Error()
	if err != nil {
		return err
	}
	c.client = cli
	c.ch = make(chan MsgPair, 16)
	return nil
}

// Close 断开与 Broker 的连接
func (c *MqttStreamingClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return nil
	}
	if c.ch != nil {
		close(c.ch)
		c.ch = nil
	}

	c.client.Disconnect(1024)
	return nil
}

// IsClosed 判断当前连接是否断开
func (c *MqttStreamingClient) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return false
	}

	return c.client.IsConnected()
}

func (c *MqttStreamingClient) getClient() mqtt.Client {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.client == nil {
		err := c.Connect()
		if err != nil {
			logger.Info(err)
		}
	}

	return c.client
}

func (c *MqttStreamingClient) handleMqttMessage() mqtt.MessageHandler {
	return func(cli mqtt.Client, msg mqtt.Message) {
		c.ch <- &Message{
			topic: msg.Topic(),
			data:  msg.Payload(),
		}
	}
}

// Subscribe 订阅 Topic
func (c *MqttStreamingClient) Subscribe(topics ...string) error {
	cli := c.getClient()
	filters := make(map[string]byte)
	for _, topic := range topics {
		filters[topic] = 2
	}
	token := cli.SubscribeMultiple(filters, c.handleMqttMessage())
	token.Wait()
	return token.Error()
}

// Unsubscribe 取消订阅 Topic
func (c *MqttStreamingClient) Unsubscribe(topics ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return nil
	}
	if token := c.client.Unsubscribe(topics...); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

// GetChan 获取用于读取的 Channel
func (c *MqttStreamingClient) GetChan() <-chan MsgPair {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.getClient()

	return c.ch
}

// Publish bala
func (c *MqttStreamingClient) Publish(topic string, message interface{}) error {
	result := c.getClient().Publish(topic, 2, true, message)
	if result != nil {
		logger.Info(result)
		return result.Error()
	}
	return nil
}
