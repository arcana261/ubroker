package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mahtabfarrokh/ubroker/pkg/ubroker"
	"github.com/pkg/errors"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	temp := &core{
		closed:          false,
		brokerChan:      make(chan ubroker.Delivery, 1000),
		publishedQueue:  []item{},
		receivedId:      []int{},
		deliveredId:     []int{},
		receivedAck:     []int{},
		receivedRequeue: []int{},
		lastIdValue:     -1,
		deliveryStarted: false,
		wg:              sync.WaitGroup{},
		ttl:             ttl,
	}

	return temp
}

type item struct {
	Message            ubroker.Message
	ID                 int
	Time               time.Time
	receivedAckChannel chan int
}
type core struct {
	closed          bool
	brokerChan      chan ubroker.Delivery
	publishedQueue  []item
	receivedId      []int
	deliveredId     []int
	lastIdValue     int
	receivedAck     []int
	receivedRequeue []int
	wg              sync.WaitGroup
	mut             sync.Mutex
	deliveryStarted bool
	ttl             time.Duration
}

func contextProblem(ctx context.Context) bool {
	if ctx.Err() == context.Canceled {
		return true
	}
	if ctx.Err() == context.DeadlineExceeded {
		return true
	}
	return false
}
func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {

	if contextProblem(ctx) {
		return nil, ctx.Err()
	}
	if c.closed {
		return nil, errors.Wrap(ubroker.ErrClosed, "closed")
	}
	c.deliveryStarted = true
	//c.wg.Done()
	return c.brokerChan, nil
	//return nil, errors.Wrap(ubroker.ErrUnimplemented, "method Delivery is not implemented")
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	if c.closed {
		return errors.Wrap(ubroker.ErrClosed, "closed")
	}
	if contextProblem(ctx) {
		return ctx.Err()
	}
	temp := false
	if c.deliveryStarted {
		temp = true
	}
	for _, element := range c.receivedAck {
		if element == id {
			temp = false
		}
	}
	if !temp {
		return errors.Wrap(ubroker.ErrInvalidID, "invalid Id")
	}
	if c.closed {
		return errors.Wrap(ubroker.ErrClosed, "closed")
	}
	c.mut.Lock()
	c.receivedAck = append(c.receivedAck, id)
	for i, element := range c.publishedQueue {
		if element.ID == id {
			c.publishedQueue[i].receivedAckChannel <- id
			c.publishedQueue = append(c.publishedQueue[:i], c.publishedQueue[i+1:]...)
		}
	}
	c.mut.Unlock()
	//c.wg.Done()
	return nil
}
func (c *core) DoingReQueue(ctx context.Context, id int) {
	c.mut.Lock()
	for i, element := range c.publishedQueue {

		if element.ID == id {
			c.receivedRequeue = append(c.receivedRequeue, id)
			c.receivedAck = append(c.receivedAck, id)
			c.lastIdValue += 1
			c.receivedId = append(c.receivedId, c.lastIdValue)
			v := ubroker.Delivery{Message: element.Message, ID: c.lastIdValue}
			v2 := item{Message: element.Message, ID: c.lastIdValue, Time: time.Now(), receivedAckChannel: make(chan int)}
			fmt.Println(len(c.publishedQueue), id, i)
			c.publishedQueue = append(c.publishedQueue[:i], c.publishedQueue[i+1:]...)
			c.publishedQueue = append(c.publishedQueue, v2)
			c.brokerChan <- v
			go c.HandelingTTL(ctx, v2)
			break
		}

	}
	c.mut.Unlock()
	defer c.wg.Done()
}
func (c *core) HandelingTTL(ctx context.Context, element item) {
	c.wg.Add(1)
	select {
	case <-time.After(c.ttl):
		go c.DoingReQueue(ctx, element.ID)
	case <-element.receivedAckChannel:
		c.wg.Done()
		return
	}
}
func (c *core) ReQueue(ctx context.Context, id int) error {
	c.wg.Add(1)
	if c.closed {
		c.wg.Done()
		return errors.Wrap(ubroker.ErrClosed, "closed")
	}
	if contextProblem(ctx) {
		c.wg.Done()
		return ctx.Err()
	}
	temp := false
	if c.deliveryStarted {
		temp = true
	}
	for _, element := range c.receivedRequeue {
		if element == id {
			temp = false
		}
	}
	for _, element := range c.receivedAck {
		if element == id {
			temp = false
		}
	}
	if !temp {
		c.wg.Done()
		return errors.Wrap(ubroker.ErrInvalidID, "invalid Id")
	}

	c.DoingReQueue(ctx, id)
	return nil
}
func (c *core) DoingPublish(ctx context.Context, message ubroker.Message) {
	c.mut.Lock()
	c.lastIdValue += 1
	c.receivedId = append(c.receivedId, c.lastIdValue)
	v := ubroker.Delivery{Message: message, ID: c.lastIdValue}
	v2 := item{Message: message, ID: c.lastIdValue, Time: time.Now(), receivedAckChannel: make(chan int)}
	c.publishedQueue = append(c.publishedQueue, v2)
	c.brokerChan <- v
	go c.HandelingTTL(ctx, v2)
	c.mut.Unlock()
	defer c.wg.Done()
}
func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	c.wg.Add(1)

	if contextProblem(ctx) {
		c.wg.Done()
		return ctx.Err()
	}
	if c.closed {
		c.wg.Done()
		return errors.Wrap(ubroker.ErrClosed, "closed")
	}
	if contextProblem(ctx) {
		c.wg.Done()
		return ctx.Err()
	}
	c.DoingPublish(ctx, message)
	return nil
}

func (c *core) Close() error {
	if c.closed {
		return nil
	}
	c.wg.Wait()
	close(c.brokerChan)
	//close(c.publishedQueue)
	c.closed = true
	//fmt.Println(e)
	return nil
	//return nil
}
