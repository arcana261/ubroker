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
	idSet        []int
	wg           *sync.WaitGroup
	sync.Mutex
}

//New broker
func New(ttl time.Duration) ubroker.Broker {
	result := &core{
		ttl: ttl,
		wg:  &sync.WaitGroup{},
	}
	result.messageList = make([]coreMsg, 0)
	result.deliveryChan = make(chan ubroker.Delivery, 100000)
	result.idSet = make([]int, 0)
	result.isClosed = make(chan bool, 1)

	return result
}
func indexOf(value int, array []int) int {
	for p, v := range array {
		if v == value {
			return p
		}
	}
	return -1
}
func (c *core) errorChecks(ctx context.Context) error {
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
	err := c.errorChecks(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Delivery error")
	}
	return c.deliveryChan, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	err := c.errorChecks(ctx)
	if err != nil {
		return errors.Wrap(err, "Acknowledge error")
	}
	c.Lock()
	defer c.Unlock()
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
	err := c.errorChecks(ctx)
	if err != nil {
		return errors.Wrap(err, "Requeue error")
	}
	c.wg.Add(1)
	c.Lock()
	defer c.Unlock()
	defer c.wg.Done()
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
	for indexOf(requeueMessageValue.msgD.ID, c.idSet) != -1 {
		requeueMessageValue.msgD.ID = rand.Int()
	}
	c.idSet = append(c.idSet, requeueMessageValue.msgD.ID)
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
	err := c.errorChecks(ctx)
	if err != nil {
		return errors.Wrap(err, "Publish error")
	}
	c.wg.Add(1)
	c.Lock()
	defer c.Unlock()
	defer c.wg.Done()
	msg := new(coreMsg)
	msg.msgD.Message = message
	msg.timeInQueue = time.Now()
	msg.msgD.ID = rand.Int()
	for indexOf(msg.msgD.ID, c.idSet) != -1 {
		msg.msgD.ID = rand.Int()
	}
	c.idSet = append(c.idSet, msg.msgD.ID)

	c.messageList = append(c.messageList, *msg)
	c.deliveryChan <- msg.msgD
	return nil
}

func (c *core) Close() error {
	c.wg.Wait()
	close(c.isClosed)
	close(c.deliveryChan)
	return nil
}
