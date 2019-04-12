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
	sync.Mutex
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if c.isClosed {
		return nil, errors.Wrap(ubroker.ErrClosed, "Delivery error, Broker is closed")
	}
	// for index, value := range c.messageList {
	// 	_ = index
	// 	c.deliveryChan <- value.msgD
	// }
	return c.deliveryChan, nil
}

func removeMessage(slice []coreMsg, s int) []coreMsg {
	return append(slice[:s], slice[s+1:]...)
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.isClosed {
		return errors.Wrap(ubroker.ErrClosed, "Acknowledge error, Broker is closed")
	}
	ackMessageIndex := -1
	for index, value := range c.messageList {
		if value.msgD.ID == id {
			ackMessageIndex = index
			break
		}
	}
	if ackMessageIndex == -1 {
		return errors.Wrap(ubroker.ErrInvalidID, "Acknowledge error, ID not found")
	}

	if time.Now().Sub(c.messageList[ackMessageIndex].timeInQueue) > c.ttl {
		go c.ReQueue(ctx, id)
		return errors.Wrap(ubroker.ErrInvalidID, "Acknowledge error, ID not found")
	}
	c.messageList = removeMessage(c.messageList, ackMessageIndex)
	for len(c.deliveryChan) > 0 {
		<-c.deliveryChan
	}
	for index, value := range c.messageList {
		_ = index
		c.deliveryChan <- value.msgD
	}
	return nil
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.isClosed {
		return errors.Wrap(ubroker.ErrClosed, "Requeue error, Broker is closed")
	}
	requeueMessageIndex := -1
	requeueMessageValue := &coreMsg{}
	for index, value := range c.messageList {
		if value.msgD.ID == id {
			requeueMessageIndex = index
			requeueMessageValue = &value
			break
		}
	}
	if requeueMessageIndex == -1 {
		return errors.Wrap(ubroker.ErrInvalidID, "Requeue error, ID not found")
	}
	requeueMessageValue.timeInQueue = time.Now()
	requeueMessageValue.msgD.ID = rand.Int()
	for c.idSet[requeueMessageValue.msgD.ID] {
		requeueMessageValue.msgD.ID = rand.Int()
	}
	c.idSet[requeueMessageValue.msgD.ID] = true
	c.messageList = removeMessage(c.messageList, requeueMessageIndex)
	c.messageList = append(c.messageList, *requeueMessageValue)

	for len(c.deliveryChan) > 0 {
		<-c.deliveryChan
	}
	for index, value := range c.messageList {
		_ = index
		c.deliveryChan <- value.msgD
	}
	return nil
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.isClosed {
		return errors.Wrap(ubroker.ErrClosed, "Publish error, Broker is closed")
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
	c.deliveryChan <- msg.msgD
	return nil
}

func (c *core) Close() error {
	if c.isClosed {
		return nil
	}
	c.Lock()
	defer c.Unlock()
	c.isClosed = true
	close(c.deliveryChan)
	return nil
}
