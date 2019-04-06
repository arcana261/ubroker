package broker

import (
	"context"
	"math/rand"
	"sync"
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
	result.messageList = make([]coreMsg, 0)
	result.deliveryChan = make(chan ubroker.Delivery, 100000000)
	result.idSet = make(map[int]bool)

	return result
}

type coreMsg struct {
	msgD        ubroker.Delivery
	timeInQueue time.Time
}

type core struct {
	messageList  []coreMsg
	ttl          time.Duration
	deliveryChan chan ubroker.Delivery
	isClosed     bool
	idSet        map[int]bool
	mutex        sync.Mutex
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if c.isClosed {
		return nil, errors.Wrap(ubroker.ErrClosed, "Delivery error, Broker is closed")
	}
	for index, value := range c.messageList {
		_ = index
		c.deliveryChan <- value.msgD
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.isClosed {
		return errors.Wrap(ubroker.ErrClosed, "Delivery error, Broker is closed")
	}
	return errors.Wrap(ubroker.ErrUnimplemented, "method ReQueue is not implemented")
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.isClosed {
		return errors.Wrap(ubroker.ErrClosed, "Delivery error, Broker is closed")
	}

	msg := new(coreMsg)
	msg.msgD.Message = message
	msg.timeInQueue = time.Now()
	msg.msgD.ID = rand.Int()
	for c.idSet[msg.msgD.ID] {
		msg.msgD.ID = rand.Int()
	}
	c.idSet[msg.msgD.ID] = true
	c.messageList = append(c.messageList, *msg)
	return nil
}

func (c *core) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.isClosed = true
	return nil
}
