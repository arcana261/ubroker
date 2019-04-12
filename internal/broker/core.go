package broker

import (
	"context"
	"time"

	"github.com/meshkati/ubroker/pkg/ubroker"
	"github.com/pkg/errors"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/un-re-queued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	return &core{
		sequenceNumber: -1,
		mainChannel:    make(chan ubroker.Delivery),
		ttl:            ttl,
		closed:         false,
	}
}

type core struct {
	// TODO: add required fields
	sequenceNumber int
	mainChannel    chan ubroker.Delivery

	ttl time.Duration

	closed bool
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	// check if context has error
	if err := filterContextError(ctx); err != nil {
		return nil, ctx.Err()
	}
	// checking the broker
	if c.closed {
		return nil, errors.Wrap(ubroker.ErrClosed, "The broker is closed.")
	}

	return c.mainChannel, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	// TODO:‌ implement me
	return errors.Wrap(ubroker.ErrUnimplemented, "method Acknowledge is not implemented")
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	// TODO:‌ implement me
	return errors.Wrap(ubroker.ErrUnimplemented, "method ReQueue is not implemented")
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	// TODO:‌ implement me
	return errors.Wrap(ubroker.ErrUnimplemented, "method Publish is not implemented")
}

func (c *core) Close() error {
	// TODO:‌ implement me
	c.closed = true

	return nil
}

// Checks if the context has an error, returns true if the deadline has exceeded, or context had canceled
func filterContextError(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
		return ctx.Err()
	}

	return nil
}
