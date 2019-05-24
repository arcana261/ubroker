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
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	temp := &core{
		closed:          false,
		brokerChan:      make(chan ubroker.Delivery, 1000),
		closedChan:      make(chan bool, 5000),
		publishedQueue:  []item{},
		receivedId:      []int{},
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
	receivedAckChannel chan int
}
type core struct {
	closed          bool
	brokerChan      chan ubroker.Delivery
	closedChan      chan bool
	publishedQueue  []item
	receivedId      []int
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
		return nil, ubroker.ErrClosed
	}
	c.mut.Lock()
	c.deliveryStarted = true
	c.mut.Unlock()
	return c.brokerChan, nil
	//return nil, errors.Wrap(ubroker.ErrUnimplemented, "method Delivery is not implemented")
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	c.mut.Lock()
	if c.closed {
		c.mut.Unlock()
		return ubroker.ErrClosed
	}
	if contextProblem(ctx) {
		c.mut.Unlock()
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
		c.mut.Unlock()
		return errors.Wrap(ubroker.ErrInvalidID, "invalid Id")
	}
	if c.closed {
		c.mut.Unlock()

		return ubroker.ErrClosed
	}
	c.receivedAck = append(c.receivedAck, id)
	for i, element := range c.publishedQueue {
		if element.ID == id {
			c.publishedQueue[i].receivedAckChannel <- id
			c.publishedQueue = append(c.publishedQueue[:i], c.publishedQueue[i+1:]...)
			break
		}
	}
	c.mut.Unlock()

	//c.wg.Done()
	return nil
}
func (c *core) DoingReQueue(ctx context.Context, id int) {
	for i, element := range c.publishedQueue {
		if element.ID == id {
			c.receivedRequeue = append(c.receivedRequeue, id)
			c.receivedAck = append(c.receivedAck, id)
			c.lastIdValue += 1
			c.receivedId = append(c.receivedId, c.lastIdValue)
			v := ubroker.Delivery{Message: element.Message, ID: c.lastIdValue}
			v2 := item{Message: element.Message, ID: c.lastIdValue, receivedAckChannel: make(chan int, 10)}
			//fmt.Println(len(c.publishedQueue), id, i)
			c.publishedQueue = append(c.publishedQueue[:i], c.publishedQueue[i+1:]...)
			c.publishedQueue = append(c.publishedQueue, v2)
			c.brokerChan <- v
			c.mut.Unlock()
			go c.HandelingTTL(ctx, v2)
			break
		}

	}

}
func (c *core) HandelingTTL(ctx context.Context, element item) {
	select {
	case <-time.After(c.ttl):
		c.mut.Lock()
		c.DoingReQueue(ctx, element.ID)
		return
	case <-element.receivedAckChannel:
		return
	case <-c.closedChan:
		return
	}
}
func (c *core) ReQueue(ctx context.Context, id int) error {
	c.mut.Lock()
	if c.closed {
		c.mut.Unlock()
		return ubroker.ErrClosed
	}
	if contextProblem(ctx) {
		c.mut.Unlock()
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
		c.mut.Unlock()

		return errors.Wrap(ubroker.ErrInvalidID, "invalid Id")
	}
	c.DoingReQueue(ctx, id)
	return nil
}
func (c *core) DoingPublish(ctx context.Context, message ubroker.Message) {
	c.lastIdValue += 1
	c.receivedId = append(c.receivedId, c.lastIdValue)
	v := ubroker.Delivery{Message: message, ID: c.lastIdValue}
	v2 := item{Message: message, ID: c.lastIdValue, receivedAckChannel: make(chan int, 10)}
	c.publishedQueue = append(c.publishedQueue, v2)
	c.brokerChan <- v
	c.mut.Unlock()

	c.HandelingTTL(ctx, v2)
	//defer c.wg.Done()
}
func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	if contextProblem(ctx) {
		return ctx.Err()
	}
	c.mut.Lock()
	if c.closed {
		c.mut.Unlock()
		return ubroker.ErrClosed
	}
	go c.DoingPublish(ctx, message)
	return nil
}

func (c *core) Close() error {
	if c.closed {
		return nil
	}
	//c.wg.Wait()
	for i := 0; i < 4000; i++ {
		c.closedChan <- true
	}
	c.mut.Lock()
	close(c.brokerChan)
	c.closed = true
	c.mut.Unlock()
	return nil
}