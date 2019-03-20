package ubroker

import (
	"context"
	"io"
	"net/http"
)

type Broker interface {
	io.Closer

	Delivery(ctx context.Context) (<-chan Delivery, error)
	Acknowledge(ctx context.Context, id int) error
	ReQueue(ctx context.Context, id int) error
	Publish(ctx context.Context, message Message) error
}

type HTTPServer interface {
	io.Closer
	http.Handler

	Run() error
}

type Message struct {
	Body string `json:"body"`
}

type Delivery struct {
	Message Message `json:"message"`
	ID      int     `json:"id"`
}
