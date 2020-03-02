module gitlab.jiangxingai.com/edgenode/synctools/streaming-porter

go 1.13

replace gitlab.jiangxingai.com/applications/base-modules/internal-sdk/go-utils => ./internal/mod/go-utils

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/go-redis/redis v6.15.7+incompatible
	github.com/gorilla/websocket v1.4.1
	github.com/nats-io/stan.go v0.6.0
	github.com/streadway/amqp v0.0.0-20200108173154-1c71cc93ed71
	gitlab.jiangxingai.com/applications/base-modules/internal-sdk/go-utils v0.0.0-00010101000000-000000000000
)
