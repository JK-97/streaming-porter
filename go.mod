module gitlab.jiangxingai.com/edgenode/synctools/streaming-porter

go 1.13

replace gitlab.jiangxingai.com/applications/base-modules/internal-sdk/go-utils => ./internal/mod/go-utils

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/gorilla/websocket v1.4.1
	github.com/nats-io/nats-server/v2 v2.1.0 // indirect
	github.com/nats-io/nats-streaming-server v0.16.2 // indirect
	github.com/nats-io/stan.go v0.5.0
	github.com/onsi/ginkgo v1.10.3 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	gitlab.jiangxingai.com/applications/base-modules/internal-sdk/go-utils v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.0.0-20191028085509-fe3aa8a45271 // indirect
)
