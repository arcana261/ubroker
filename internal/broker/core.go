package broker

import (
	"context"
	"sync"
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
		mainChannel:    make(chan ubroker.Delivery, 100),
		ackMap:         make(map[int]chan bool),
		pendingMap:     make(map[int]ubroker.Message),

		ttl:    ttl,
		closed: false,
	}
}

type core struct {
	// TODO: add required fields
	sequenceNumber  int
	mainChannel     chan ubroker.Delivery
	ackMap          map[int]chan bool
	pendingMap      map[int]ubroker.Message
	ttl             time.Duration
	sequenceMutex   sync.Mutex
	pendingMutex	sync.Mutex
	closed          bool
	deliveryStarted bool
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

	c.deliveryStarted = true

	return c.mainChannel, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	// check if context has error
	if err := filterContextError(ctx); err != nil {
		return err
	}
	// checking the broker
	if c.closed {
		return errors.Wrap(ubroker.ErrClosed, "Acknowledge:: The broker is closed.")
	}
	// check if delivery started
	if !c.deliveryStarted {
		return errors.Wrap(ubroker.ErrInvalidID, "Acknowledge:: Delivery is not started yet")
	}
	// handling race condition
	c.sequenceMutex.Lock()
	defer c.sequenceMutex.Unlock()
	// check if the id is not exists
	if id > c.sequenceNumber || id < 0 {
		return errors.Wrap(ubroker.ErrInvalidID, "Acknowledge:: Message with id="+string(id)+" is not committed yet.")
	}
	// check if it's going to re-acknowledgment
	if _, ok := c.ackMap[id]; !ok {
		return errors.Wrap(ubroker.ErrInvalidID, "Acknowledge:: Message with id="+string(id)+"has been ACKed before.")
	}
	// everything is "probably" Ok, so we're going to mark the ACK
	c.ackMap[id] <- true

	return nil
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	// check if context has error
	if err := filterContextError(ctx); err != nil {
		return err
	}
	// checking the broker
	if c.closed {
		return errors.Wrap(ubroker.ErrClosed, "ReQueue:: The broker is closed.")
	}
	// check if delivery started
	if !c.deliveryStarted {
		return errors.Wrap(ubroker.ErrInvalidID, "Acknowledge:: Delivery is not started yet")
	}
	// handling race condition
	c.sequenceMutex.Lock()
	defer c.sequenceMutex.Unlock()
	// check if the id is not exists
	if id > c.sequenceNumber || id < 0 {
		return errors.Wrap(ubroker.ErrInvalidID, "ReQueue:: Message with id="+string(id)+" is not committed yet.")
	}
	// check if it's going to re-queue the message
	//if _, ok := c.ackMap[id]; ok {
	//	return errors.Wrap(ubroker.ErrInvalidID, "ReQueue:: Message with id="+string(id)+" is already in the queue.")
	//}

	// everything is "probably" Ok, so we're going to put the message in queue
	tMessage := c.pendingMap[id]

	c.sequenceNumber++
	tDelivery := ubroker.Delivery{
		Message: tMessage,
		ID:      c.sequenceNumber,
	}
	// setting the acknowledge channel
	c.ackMap[c.sequenceNumber] = make(chan bool, 1)

	go c.ttlHandler(ctx, tDelivery)
	// pushing the message into the main channel
	c.mainChannel <- tDelivery
	// invalidate the previous id
	c.ackMap[id] <- false

	return nil
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	// check if context has error
	if err := filterContextError(ctx); err != nil {
		return err
	}
	// checking the broker
	if c.closed {
		return errors.Wrap(ubroker.ErrClosed, "Publish:: The broker is closed.")
	}
	// handling race condition
	c.sequenceMutex.Lock()
	defer c.sequenceMutex.Unlock()
	// Pushing into the channel
	c.sequenceNumber++
	delivery := ubroker.Delivery{
		ID:      c.sequenceNumber,
		Message: message,
	}
	c.ackMap[delivery.ID] = make(chan bool, 1)
	go c.ttlHandler(ctx, delivery)
	// push the message to channel
	c.mainChannel <- delivery

	return nil
}

func (c *core) Close() error {
	// TODO:‌ implement me
	c.closed = true
	close(c.mainChannel)

	return nil
}

// Checks if the context has an error, returns true if the deadline has exceeded, or context had canceled
func filterContextError(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
		return ctx.Err()
	}

	return nil
}

// Sets a timeout for TTL and re-queue the message after that time
func (c *core) ttlHandler(ctx context.Context, delivery ubroker.Delivery) {
	c.pendingMutex.Lock()
	c.pendingMap[delivery.ID] = delivery.Message
	c.pendingMutex.Unlock()
	// TODO: Handle race condition
	select {
	case <-time.After(c.ttl):
		_ = c.ReQueue(ctx, delivery.ID)
	case <-c.ackMap[delivery.ID]:
		// handling race condition
		c.sequenceMutex.Lock()
		defer c.sequenceMutex.Unlock()

		delete(c.ackMap, delivery.ID)
		delete(c.pendingMap, delivery.ID)
		return
	}
}
