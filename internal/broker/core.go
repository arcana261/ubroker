package broker

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sneyes/ubroker/pkg/ubroker"
)

type coreMsg struct {
	msgD        ubroker.Delivery
	timeInQueue time.Time
}

type core struct {
	messageList  []coreMsg
	ttl          time.Duration
	deliveryChan chan ubroker.Delivery
	isClosed     chan bool
	idSet        map[int]bool
	sync.WaitGroup
	sync.Mutex
}

//New broker
func New(ttl time.Duration) ubroker.Broker {
	result := &core{
		ttl: ttl,
	}
	result.messageList = make([]coreMsg, 0)
	result.deliveryChan = make(chan ubroker.Delivery, 100000)
	result.idSet = make(map[int]bool)
	result.isClosed = make(chan bool, 1)

	return result
}

func (c *core) checksAndLocks(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()
	c.Add(1)
	select {
	case <-c.isClosed:
		return ubroker.ErrClosed
	default:
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func removeMessage(slice []coreMsg, s int) []coreMsg {
	return append(slice[:s], slice[s+1:]...)
}
func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	defer c.Done()
	err := c.checksAndLocks(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Delivery error, Broker is closed")
	}
	return c.deliveryChan, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	defer c.Done()
	err := c.checksAndLocks(ctx)
	if err != nil {
		return errors.Wrap(err, "Acknowledge error, Broker is closed")
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
	defer c.Done()
	err := c.checksAndLocks(ctx)
	if err != nil {
		return errors.Wrap(err, "Requeue error, Broker is closed")
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
	ttlErr := time.Now().Sub(c.messageList[requeueMessageIndex].timeInQueue) > c.ttl
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
	if ttlErr {
		return errors.Wrap(ubroker.ErrInvalidID, "Acknowledge error, ID not found")
	}
	return nil
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	defer c.Done()
	err := c.checksAndLocks(ctx)
	if err != nil {
		return errors.Wrap(err, "Publish error, Broker is closed")
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
	c.Lock()
	select {
	case <-c.isClosed:
		_ = c
	default:
		close(c.isClosed)
	}
	c.Unlock()

	c.Wait()
	close(c.deliveryChan)
	return nil
}
