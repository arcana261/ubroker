package broker

import (
	"context"
	"time"

	"github.com/arcana261/ubroker/pkg/ubroker"
	"github.com/pkg/errors"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	return &core{}
}

type core struct {
	// TODO: add required fields
	// 1- A message id generation routine
	// 2- A dictionary of message values and id keys

}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	// Delivery returns a channel which continuously supplies
	// messages to consumers.
	// We require following:
	//
	// 1. Resulting read-only channel is unique (it does
	//    not change each time you call it)
	// 2. If `ctx` is canceled or timed out, `ctx.Err()` is
	//    returned
	// 3. If broker is closed, `ErrClosed` is returned
	// 4. should be thread-safe
	return nil, errors.Wrap(ubroker.ErrUnimplemented, "method Delivery is not implemented")
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
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
	return errors.Wrap(ubroker.ErrUnimplemented, "method Acknowledge is not implemented")
}

func (c *core) ReQueue(ctx context.Context, id int) error {
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
	return errors.Wrap(ubroker.ErrUnimplemented, "method ReQueue is not implemented")
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	// Publish is used to enqueue a new message to broker
	// We demand following:
	//
	// 1. If `ctx` is canceled or timed out, `ctx.Err()` is
	//    returned
	// 2. If broker is closed, `ErrClosed` is returned
	// 3. should be thread-safe
	return errors.Wrap(ubroker.ErrUnimplemented, "method Publish is not implemented")
}

func (c *core) Close() error {
	// Broker interface implements io.Closer interface
	// which supplies us with method `Close() error`.
	// We require following:
	//
	// 1. closing of a closed broker should result in `nil`
	// 2. should be thread-safe
	// 3. all other operations after closing broker should result
	//    in ErrClosed error
	return errors.Wrap(ubroker.ErrUnimplemented, "method Close is not implemented")
}
