package broker

import (
	"context"
	"sync"
	"time"

	"github.com/arcana261/ubroker/pkg/ubroker"
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
		requeueMap:		make(map[int]bool),
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
	requeueMap      map[int]bool
	ttl             time.Duration
	sequenceMutex   sync.Mutex
	closed          bool
	deliveryStarted bool
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	// check if context has error
	if err := filterContextError(ctx); err != nil {
		return nil, ctx.Err()
	}
	// handling race condition
	c.sequenceMutex.Lock()
	defer c.sequenceMutex.Unlock()
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
	// handling race condition
	c.sequenceMutex.Lock()
	defer c.sequenceMutex.Unlock()
	// checking the broker
	if c.closed {
		//return errors.Wrap(ubroker.ErrClosed, "Acknowledge:: The broker is closed.")
		return ubroker.ErrClosed
	}
	// check if delivery started
	if !c.deliveryStarted {
		return errors.Wrap(ubroker.ErrInvalidID, "Acknowledge:: Delivery is not started yet")
	}
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
	// removing the message id from maps
	delete(c.ackMap, id)
	delete(c.pendingMap, id)

	return nil
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	// check if context has error
	if err := filterContextError(ctx); err != nil {
		return err
	}
	// handling race condition
	c.sequenceMutex.Lock()
	defer c.sequenceMutex.Unlock()
	// checking the broker
	if c.closed {
		//return errors.Wrap(ubroker.ErrClosed, "ReQueue:: The broker is closed.")
		return ubroker.ErrClosed
	}
	// check if delivery started
	if !c.deliveryStarted {
		return errors.Wrap(ubroker.ErrInvalidID, "Acknowledge:: Delivery is not started yet")
	}
	// check if the id is not exists
	if id > c.sequenceNumber || id < 0 {
		return errors.Wrap(ubroker.ErrInvalidID, "ReQueue:: Message with id="+string(id)+" is not committed yet.")
	}
	////check if it's going to re-queue the message
	if _, ok := c.requeueMap[id]; ok {
		return errors.Wrap(ubroker.ErrInvalidID, "ReQueue:: Message with id="+string(id)+" is already in the queue.")
	}

	// everything is "probably" Ok, so we're going to put the message in queue
	tMessage := c.pendingMap[id]

	c.sequenceNumber++
	tDelivery := ubroker.Delivery{
		Message: tMessage,
		ID:      c.sequenceNumber,
	}
	// setting the acknowledge channel
	c.ackMap[tDelivery.ID] = make(chan bool, 1)
	c.pendingMap[tDelivery.ID] = tDelivery.Message

	go c.ttlHandler(ctx, tDelivery)
	// invalidate the previous id
	c.ackMap[id] <- false
	// removing the message id from maps
	delete(c.ackMap, id)
	delete(c.pendingMap, id)
	c.requeueMap[id] = true
	// pushing the message into the main channel
	c.mainChannel <- tDelivery

	return nil
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	// check if context has error
	if err := filterContextError(ctx); err != nil {
		return err
	}
	// handling race condition
	c.sequenceMutex.Lock()
	defer c.sequenceMutex.Unlock()
	// checking the broker
	if c.closed {
		//return errors.Wrap(ubroker.ErrClosed, "Publish:: The broker is closed.")
		return ubroker.ErrClosed
	}
	// Pushing into the channel
	c.sequenceNumber++
	delivery := ubroker.Delivery{
		ID:      c.sequenceNumber,
		Message: message,
	}
	c.ackMap[delivery.ID] = make(chan bool, 1)
	c.pendingMap[delivery.ID] = delivery.Message

	go c.ttlHandler(ctx, delivery)
	// push the message to channel
	c.mainChannel <- delivery

	return nil
}

func (c *core) Close() error {
	c.sequenceMutex.Lock()
	defer c.sequenceMutex.Unlock()

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
	c.sequenceMutex.Lock()
	ch := c.ackMap[delivery.ID]
	c.sequenceMutex.Unlock()

	select {
	case <-time.After(c.ttl):
		_ = c.ReQueue(ctx, delivery.ID)
		return
	case <-ch:
		return
	}
}
