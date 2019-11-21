package main

//go:generate go run version_generate.go

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gitlab.jiangxingai.com/applications/base-modules/internal-sdk/go-utils/logger"
	"gitlab.jiangxingai.com/edgenode/synctools/streaming-porter/adapter"

	"github.com/BurntSushi/toml"
	"github.com/gorilla/websocket"
)

var (
	version = "v0.0.1"
	commit  = "?"
	date    = "2019-09-20T13:16:09+08:00"
)

var remoteClient adapter.MessageClient

var localClient adapter.MessageClient

type porterConfig struct {
	WorkerID    string
	LocalURI    string
	RemoteURI   string
	GatewayAddr string
}

func tryUntilConnected(client adapter.MessageClient) {
	var err error
	err = client.Connect()
	for err != nil {
		logger.Info(err)
		time.Sleep(time.Second)
	}
}

// StartSync 开始同步消息队列
func StartSync(local, remote adapter.MessageClient, ch <-chan string) {
	channel := local.GetChan()
	edgeToLocal := remoteClient.GetChan()

	var obj adapter.MsgPair
	for {
		select {
		// 需要同步的主题增加
		case topic := <-ch:
			logger.Info("Sync:", topic)
			if err := local.Subscribe(topic); err != nil {
				logger.Info(err)
			}

		// 云端同步到本地
		case obj = <-edgeToLocal:
			if obj == nil {
				if _, ok := <-channel; !ok {
					// 连接中断
					tryUntilConnected(remote)
					edgeToLocal = remote.GetChan()
					continue
				}
			}

			data := obj.Data()
			var topic string
			var buf []byte
			switch b := data.(type) {
			case []byte:

				ll := len(buf)
				if ll < 2 {
					logger.Info("To Short", ll)
					continue
				}
				length := binary.LittleEndian.Uint16(b[0:])
				if ll < int(length+2) {
					logger.Info("To Short", ll)
					continue
				}
				topic = string(buf[2 : length+2])
				b = b[length+2:]
				logger.Infof("T: %s, D: %s", obj.Topic(), string(b))
			default:
				topic = obj.Topic()
				logger.Infof("T: %s, D: %v", obj.Topic(), data)
			}

			err := local.Publish(topic, buf)
			if err != nil {
				obj.Nack()
				if err == adapter.ErrMaxPings || err == adapter.ErrConnectionClosed {
					local.Close()
					tryUntilConnected(local)
				} else {
					logger.Info(err)
				}
			} else {
				obj.Ack()
			}
			// 本地同步到云端的
		case obj = <-channel:
			if obj == nil {
				if _, ok := <-channel; !ok {
					// 连接中断
					tryUntilConnected(local)
					channel = local.GetChan()
					continue
				}
			}

			data := obj.Data()
			switch data.(type) {
			case []byte:
				logger.Infof("T: %s, D: %s", obj.Topic(), string(data.([]byte)))
			default:
				logger.Infof("T: %s, D: %v", obj.Topic(), data)
			}
			err := remoteClient.Publish(obj.Topic(), obj.Data())
			if err != nil {
				logger.Error(err)
				obj.Nack()
				if err == adapter.ErrMaxPings || err == adapter.ErrConnectionClosed {
					remote.Close()
					tryUntilConnected(remote)
				} else {
					logger.Info(err)
				}
			} else {
				logger.Info("ACK")
				obj.Ack()
			}
		}
	}
}

func parseCommandLine(configPath *string) (string, string) {

	toml.DecodeFile(*configPath, config)

	resp, err := http.Get(config.GatewayAddr + "/api/v1/mq/create")
	if err != nil {
		panic(err)
	} else {
		var mq struct {
			Data struct {
				URI string `json:"uri"`
			}
		}

		defer resp.Body.Close()
		err = json.NewDecoder(resp.Body).Decode(&mq)
		if err != nil {
			panic(err)
		}
		config.LocalURI = mq.Data.URI
	}

	logger.Info(config)

	return config.LocalURI, config.RemoteURI
}

func checkConnection(local, remote adapter.MessageClient) error {
	if err := local.Connect(); err != nil {
		logger.Info(err)
		return err
	}

	// topics, err := GetAllSyncedTopics()
	// if err != nil {
	// 	logger.Info(err)
	// 	return err
	// }
	// if len(topics) > 0 {
	// 	local.Subscribe(topics...)
	// }

	if err := local.Connect(); err != nil {
		logger.Info(err)
		return err
	}

	if err := remote.Connect(); err != nil {
		logger.Info(err)
		return err
	}

	if err := remote.Subscribe("cloud2edge", "cloud2edge-"+config.WorkerID); err != nil {
		logger.Info(err)
	}

	return nil
}

func heartBeat(stopChan chan os.Signal, local, remote adapter.MessageClient) {
	c := time.Tick(5 * time.Second)

	for {
		select {
		case <-c:
			if local.IsClosed() {
				tryUntilConnected(local)
			}
			if remote.IsClosed() {
				tryUntilConnected(remote)
			}
		case s := <-stopChan:
			logger.Info("Recv Signal", s)
			if s == syscall.Signal(0x20) {
				logger.Info("Sync Topics")
				checkConnection(local, remote)
				continue
			}
			os.Exit(0)
		}
	}
}

var configPath *string
var config *porterConfig = &porterConfig{
	GatewayAddr: "http://edgegw.iotedge:9000",
}

type syncHandler struct {
	remoteClient adapter.MessageClient

	localClient adapter.MessageClient
}

type grantTopicRequest struct {
	Topics []string `json:"topics"`
}

func (h *syncHandler) readGrantTopicRequest(r *http.Request) *grantTopicRequest {
	body := r.Body
	defer body.Close()
	buff, err := ioutil.ReadAll(body)
	if err != nil {
		logger.Info(err)
		return nil
	}
	req := grantTopicRequest{}
	if err := json.Unmarshal(buff, &req); err != nil {
		logger.Info(err)
		return nil
	}
	return &req
}

func (h *syncHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/topic" {
		switch r.Method {
		case http.MethodPost:
			req := h.readGrantTopicRequest(r)

			if req == nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			localClient.Subscribe(req.Topics...)
			w.WriteHeader(http.StatusAccepted)
		case http.MethodDelete:
			req := h.readGrantTopicRequest(r)

			if req == nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

type topicToken struct {
	Topic string `json:"topic"`
	Grant bool   `json:"grant"`
}

func fetchTopics(ch chan<- string) error {
	url := config.GatewayAddr + "/api/v1/mq/ws"
	if strings.HasPrefix(url, "https://") {
		url = strings.Replace(url, "https://", "wss://", 1)
	} else {
		url = strings.Replace(url, "http://", "ws://", 1)
	}
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		logger.Error("dial:", err)
		return err
	}
	defer c.Close()
	var token topicToken
	for {
		mt, message, err := c.ReadMessage()
		if err != nil {
			return err
		}
		switch mt {
		case websocket.TextMessage:
			err := json.Unmarshal(message, &token)
			if err != nil {
				continue
			}
			ch <- token.Topic
		}
	}
}

// // GetAllSyncedTopics 获取所有需要同步的 Topic
// func GetAllSyncedTopics() (topics []string, err error) {
// 	resp, err := http.Get(config.GatewayAddr + "/api/v1/mq/topicz")
// 	if err != nil {
// 		return nil, err
// 	}
// 	if resp.StatusCode >= 400 {
// 		return
// 	}
// 	buf, err := ioutil.ReadAll(resp.Body)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer resp.Body.Close()

// 	var s struct {
// 		Data struct {
// 			Topics []string `json:"topics"`
// 		} `json:"data"`
// 	}
// 	err = json.Unmarshal(buf, &s)
// 	if err != nil {
// 		return nil, err
// 	}
// 	topics = s.Data.Topics
// 	return
// }

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" || os.Args[1] == "-v" {
		fmt.Printf("Version: %s, Commit: %s, Date: %s\n", version, commit, date)
		os.Exit(0)
	}

	logger.Info(os.Getpid())

	// config.GetConfig()
	configPath = flag.String("config", "porter.cfg", "Config file Path")
	flag.StringVar(configPath, "c", "porter.cfg", "Config file Path")
	flag.Parse()

	localURI, remoteURI := parseCommandLine(configPath)

	if config.WorkerID == "" {
		config.WorkerID = readWorkerID()
	}

	localClient = adapter.CreateClient(localURI)
	remoteClient = adapter.CreateClient(remoteURI)

	for {
		if err := checkConnection(localClient, remoteClient); err == nil {
			break
		}
	}

	c := make(chan os.Signal)

	signal.Notify(
		c,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGABRT,
		syscall.SIGKILL,
		syscall.SIGTERM,
		syscall.Signal(0x20),
	)
	go heartBeat(c, localClient, remoteClient)

	ch := make(chan string, 16)
	go func() {
		for {
			err := fetchTopics(ch)
			if err != nil {
				logger.Error(err)
			}
		}
	}()

	StartSync(localClient, remoteClient, ch)
}
