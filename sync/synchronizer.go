package sync

import "github.com/JK-97/streaming-porter/adapter"

// Synchronizer 消息同步器
type Synchronizer interface {
	Sync() error
	Source() adapter.Subscriber
	Destination() adapter.Publisher
}
