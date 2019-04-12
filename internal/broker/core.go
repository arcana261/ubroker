package broker

import (
	"context"
	"sync"
	"time"

	"github.com/arcana261/ubroker/pkg/ubroker"
)
func New(ttl time.Duration) ubroker.Broker {
	core := &Core{
		deliveryEntry:    make(chan ubroker.Delivery),
		publishEntry:     make(chan request),
		requeueEntry:     make(chan requeue),
		acknowledgeEntry: make(chan acknowledge),
		ttl:              ttl,
		isClosed:         false,
		close:            false,
	}
	return core
}

type Core struct {
	// TODO: add required fields
	ttl              time.Duration
	deliveryEntry    chan ubroker.Delivery
	publishEntry     chan request
	requeueEntry     chan requeue
	acknowledgeEntry chan acknowledge
	isClosed         bool
	close            bool
	requestGroup     sync.WaitGroup
	queueGroup       sync.WaitGroup
	requestHandle    sync.Mutex
}

func (core *Core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	// TODO:‌ implement me
	//return nil, errors.Wrap(ubroker.ErrUnimplemented, "method Delivery is not implemented")
	if core.close == true {
		return nil, ubroker.ErrClosed
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	core.requestGroup.Add(1)
	defer core.requestGroup.Done()
	return core.deliveryEntry, nil
}
func (core *Core) Acknowledge(ctx context.Context, id int) error {
	// TODO:‌ implement me
	//return errors.Wrap(ubroker.ErrUnimplemented, "method Acknowledge is not implemented")
	if err := core.startRequestHandling(); err != nil {
		return err
	}
	defer core.requestGroup.Done()
	r := acknowledge{
		id:    id,
		error: make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case core.acknowledgeEntry <- r:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-r.error:
			return err
		}
	}
}
func (core *Core) ReQueue(ctx context.Context, id int) error {
	// TODO:‌ implement me
	//return errors.Wrap(ubroker.ErrUnimplemented, "method ReQueue is not implemented")
	if err := core.startRequestHandling(); err != nil {
		return err
	}
	defer core.requestGroup.Done()
	r := requeue{
		id:    id,
		error: make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case core.requeueEntry <- r:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-r.error:
			return err
		}
	}
}
func (core *Core) Publish(ctx context.Context, message ubroker.Message) error {
	// TODO:‌ implement me
	//return errors.Wrap(ubroker.ErrUnimplemented, "method Publish is not implemented")
	if err := core.startRequestHandling(); err != nil {
		return err
	}
	defer core.requestGroup.Done()
	pr := request{
		msg:   message,
		error: make(chan error, 1),
	}
	if core.isClosed == true {
		return ubroker.ErrClosed
	}
	select {
	case core.publishEntry <- pr:
		return <-pr.error
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (core *Core) Close() error {
	// TODO:‌ implement me
	//return errors.Wrap(ubroker.ErrUnimplemented, "method Close is not implemented")
	if core.IsClosed() {
		return nil
	}
	core.requestHandle.Lock()
	defer
		core.requestHandle.Unlock()
	core.isClosed = true
	core.queueGroup.Wait()
	close(core.requeueEntry)
	close(core.acknowledgeEntry)
	close(core.deliveryEntry)
	close(core.publishEntry)
	return nil
}

// ***********************************************************
type request struct {
	msg     ubroker.Message
	ttlTime time.Time
	error   chan error
}
type acknowledge struct {
	id    int
	error chan error
}
type requeue struct {
	id    int
	error chan error
}

func (core *Core) startRequestHandling() error {
	core.requestHandle.Lock()
	if core.IsClosed() {
		return ubroker.ErrClosed
	}
	defer core.requestHandle.Unlock()
	core.requestGroup.Add(1)
	return nil
}
func (core *Core) IsClosed() bool {
	if core.close == true {
		return true
	} else {
		return false
	}
}
