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
	return &core{}
}

type core struct {
	// TODO: add required fields
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	// TODO:‌ implement me
	return nil, errors.Wrap(ubroker.ErrUnimplemented, "method Delivery is not implemented")
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
	return errors.Wrap(ubroker.ErrUnimplemented, "method Close is not implemented")
}

// Checks if the context has an error, returns true if the deadline has exceeded, or context had canceled
func filterContextError(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
		return ctx.Err()
	}

	return nil
}
