package ubroker

import (
	"context"
	"io"
	"net/http"
)

// Broker interface defines functionalities of a
// message broker system.
//
// we require our message broker to timeout and requeue
// unacknowledged messages automatically. We also require
// that implementations should be thread-safe.
type Broker interface {
	// Broker interface implements io.Closer interface
	// which supplies us with method `Close() error`.
	// We require following:
	//
	// 1. closing of a closed broker should result in `nil`
	// 2. should be thread-safe
	// 3. all other operations after closing broker should result
	//    in ErrClosed error
	io.Closer

	// Delivery returns a channel which continuously supplies
	// messages to consumers.
	// We require following:
	//
	// 1. Resulting read-only channel is unique (it doesn
	//    not change each time you call it)
	// 2. If `ctx` is canceled or timed out, `ctx.Err()` is
	//    returned
	// 3. If broker is closed, `ErrClosed` is returned
	// 4. should be thread-safe
	Delivery(ctx context.Context) (<-chan *Delivery, error)

	// Acknowledge is called by clients to declare that
	// specified message id has been successfuly processed
	// and should not be requeued to queue and we have to
	// remove it.
	// We demand following:
	//
	// 1. Non-existing ids should cause ErrInvalidID
	// 2. Re-acknowledgement and Requeue of id should cause ErrInvalidID
	// 3. Should prevent requeue due to TTL
	// 4. If `ctx` is canceled or timed out, `ctx.Err()` is
	//    returned
	// 5. If broker is closed, `ErrClosed` is returned
	// 6. should be thread-safe
	Acknowledge(ctx context.Context, id int32) error

	// ReQueue is called by clients to declare that
	// specified message id should be put back in
	// front of the queue.
	// We demand following:
	//
	// 1. Non-existing ids should cause ErrInvalidID
	// 2. Re-acknowledgement and Requeue of id should cause ErrInvalidID
	// 3. Should prevent requeue due to TTL
	// 4. If `ctx` is canceled or timed out, `ctx.Err()` is
	//    returned
	// 5. If broker is closed, `ErrClosed` is returned
	// 6. should be thread-safe
	ReQueue(ctx context.Context, id int32) error

	// Publish is used to enqueue a new message to broker
	// We demand following:
	//
	// 1. If `ctx` is canceled or timed out, `ctx.Err()` is
	//    returned
	// 2. If broker is closed, `ErrClosed` is returned
	// 3. should be thread-safe
	Publish(ctx context.Context, message *Message) error
}

// HTTPServer defines an HTTP‌ API‌ server provider
type HTTPServer interface {
	io.Closer
	http.Handler

	Run() error
}
