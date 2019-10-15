package adapter

import "errors"

// Errors
var (
	ErrConnectReqTimeout = errors.New("porter: connect request timeout")
	ErrCloseReqTimeout   = errors.New("porter: close request timeout")
	ErrSubReqTimeout     = errors.New("porter: subscribe request timeout")
	ErrUnsubReqTimeout   = errors.New("porter: unsubscribe request timeout")
	// ErrConnectionClosed 连接被关闭
	ErrConnectionClosed = errors.New("porter: connection closed")
	ErrTimeout          = errors.New("porter: publish ack timeout")
	ErrBadAck           = errors.New("porter: malformed ack")
	ErrBadSubscription  = errors.New("porter: invalid subscription")
	ErrBadConnection    = errors.New("porter: invalid connection")
	ErrManualAck        = errors.New("porter: cannot manually ack in auto-ack mode")
	ErrNilMsg           = errors.New("porter: nil message")
	ErrNoServerSupport  = errors.New("porter: not supported by server")
	ErrMaxPings         = errors.New("porter: connection lost due to PING failure")
)
