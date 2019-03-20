package broker

import (
	"context"
	"time"

	"github.com/arcana261/ubroker/pkg/ubroker"
	"github.com/pkg/errors"
)

func New(ttl time.Duration) ubroker.Broker {
	return &core{}
}

type core struct {
	// TODO: add required fields
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	return nil, errors.Wrap(ubroker.ErrUnimplemented, "method Delivery is not implemented")
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	return errors.Wrap(ubroker.ErrUnimplemented, "method Acknowledge is not implemented")
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	return errors.Wrap(ubroker.ErrUnimplemented, "method ReQueue is not implemented")
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	return errors.Wrap(ubroker.ErrUnimplemented, "method Publish is not implemented")
}

func (c *core) Close() error {
	return errors.Wrap(ubroker.ErrUnimplemented, "method Close is not implemented")
}
