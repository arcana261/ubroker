package broker

import (
	"container/list"
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sneyes/ubroker/pkg/ubroker"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	result := &core{
		ttl: ttl,
	}
	result.messageList = *list.New()
	a := &coreMsg{}
	result.messageList.PushFront(a)
	result.deliveryChan = make(chan ubroker.Delivery)

	return result
}

type coreMsg struct {
	msgD        ubroker.Delivery
	timeInQueue time.Time
}

type core struct {
	messageList  list.List
	ttl          time.Duration
	deliveryChan <-chan ubroker.Delivery
	isClosed     bool
	// TODO: add required fields
	// 1- A message id generation routine
	// 2- A dictionary of message values and id keys

}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	// Delivery returns a channel which continuously supplies messages to consumers.
	// We require following:
	// 4. should be thread-safe
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if c.isClosed {
		return nil, errors.Wrap(ubroker.ErrClosed, "delivery error, Broker is closed")
	}
	return c.deliveryChan, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	// Acknowledge is called by clients to declare that specified message id has been successfuly processed and should not be requeued to queue and we have to remove it.
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
	// ReQueue is called by clients to declare that specified message id should be put back in front of the queue. We demand following:
	//
	// 1. Non-existing ids should cause ErrInvalidID
	// 2. Re-acknowledgement and Requeue of id should cause ErrInvalidID
	// 3. Should prevent requeue due to TTL
	// 4. If `ctx` is canceled or timed out, `ctx.Err()` is
	//    returned
	// 5. If broker is closed, `ErrClosed` is returned
	// 6. should be thread-safe
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.isClosed {
		return errors.Wrap(ubroker.ErrClosed, "delivery error, Broker is closed")
	}
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
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.isClosed {
		return errors.Wrap(ubroker.ErrClosed, "delivery error, Broker is closed")
	}
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
