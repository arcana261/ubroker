package broker

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/arcana261/ubroker/pkg/ubroker"
)

var (
	lastMessageID = 0
	mutexId = sync.Mutex{}
)
// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	return &core{
		ttl:               ttl,
		isClosed:          make(chan bool, 1),
		deliveryChannel:   make(chan ubroker.Delivery, 1),
		toDeliverChannel:  make(chan ubroker.Delivery, math.MaxInt16),
		messageChannels:   make(map[int]chan bool, math.MaxInt16),
		deliveredMessages: make(map[int]ubroker.Delivery, math.MaxInt16),
		doneMessages:      make(map[int]ubroker.Delivery, math.MaxInt16),
		mutex:             sync.Mutex{},
		publishMutex:      sync.Mutex{},
		deliveryMutex:      sync.Mutex{},

	}
}

type core struct {
	isClosed          chan bool
	ttl               time.Duration
	deliveryChannel   chan ubroker.Delivery
	toDeliverChannel  chan ubroker.Delivery
	messageChannels   map[int]chan bool
	deliveredMessages map[int]ubroker.Delivery
	doneMessages      map[int]ubroker.Delivery
	mutex             sync.Mutex
	publishMutex      sync.Mutex
	deliveryMutex      sync.Mutex
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.isClosed:
		return nil, ubroker.ErrClosed
	default:
	}
	if c.deliveryChannel == nil {
		c.deliveryChannel = make(chan ubroker.Delivery, 1)
	}
	return c.deliveryChannel, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.isClosed:
		return ubroker.ErrClosed
	default:
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.deliveredMessages[id]; !ok {
		return ubroker.ErrInvalidID
	}
	if _, ok := c.doneMessages[id]; ok {
		return ubroker.ErrInvalidID
	}
	c.doneMessages[id] = c.deliveredMessages[id]
	delete(c.deliveredMessages, id)
	close(c.messageChannels[id])
	return nil
	//return errors.Wrap(ubroker.ErrUnimplemented, "method Acknowledge is not implemented")
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.isClosed:
		return ubroker.ErrClosed
	default:
	}
	c.mutex.Lock()
	if _, ok := c.deliveredMessages[id]; !ok {
		c.mutex.Unlock()
		return ubroker.ErrInvalidID
	}
	message := c.deliveredMessages[id].Message
	delete(c.deliveredMessages, id)
	c.mutex.Unlock()
	err := c.Publish(ctx, message)
	return err
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.isClosed:
		return ubroker.ErrClosed
	default:
	}
	id := getID()
	br := ubroker.Delivery{Message: message, ID: id}
	c.mutex.Lock()
	c.messageChannels[id] = make(chan bool)
	c.toDeliverChannel <- br
	c.mutex.Unlock()

	go func() {
		c.publishMutex.Lock()
		defer c.publishMutex.Unlock()
		m := <-c.toDeliverChannel
		c.mutex.Lock()
		c.deliveredMessages[m.ID] = m
			c.mutex.Unlock()
		for ; len(c.deliveryChannel) != 0; {
			select {
			case <-c.isClosed:
				return
			default:

			}
		}
		select {
		case <-c.isClosed:
			return
		default:
			c.deliveryMutex.Lock()
			c.deliveryChannel <- m
			c.deliveryMutex.Unlock()
			go c.WaitAck(ctx, m)
		}
	}()
	return nil
}
func (c *core) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	select {
	case <-c.isClosed:
		return nil
	default:
		close(c.isClosed)
		c.deliveryMutex.Lock()
		close(c.deliveryChannel)
		c.deliveryMutex.Unlock()
		return nil
	}
}
func (c *core) WaitAck(ctx context.Context, message ubroker.Delivery) {
	c.mutex.Lock()
	channel := c.messageChannels[message.ID]
	c.mutex.Unlock()
	d := time.After(c.ttl)
	select {
	case <-channel:
		return
	case <-d:
		_ = c.ReQueue(ctx, message.ID)
		return
	case <-c.isClosed:
		return
	case <-ctx.Done():

	}
}
func getID() int {
	mutexId.Lock()
	defer mutexId.Unlock()
	lastMessageID++
	return lastMessageID
}