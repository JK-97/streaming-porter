module github.com/JK-97/streaming-porter

go 1.13

replace github.com/JK-97/go-utils => ./internal/mod/go-utils

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/JK-97/go-utils v0.0.0-00010101000000-000000000000
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/go-redis/redis v6.15.7+incompatible
	github.com/golang/protobuf v1.3.5 // indirect
	github.com/gorilla/websocket v1.4.2
	github.com/nats-io/nats-streaming-server v0.17.0 // indirect
	github.com/nats-io/stan.go v0.6.0
	github.com/onsi/ginkgo v1.12.0 // indirect
	github.com/onsi/gomega v1.9.0 // indirect
	github.com/streadway/amqp v0.0.0-20200108173154-1c71cc93ed71
)
