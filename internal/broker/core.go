package broker

import (
	"context"
	"fmt"
	"github.com/arcana261/ubroker/pkg/ubroker"
	"github.com/pkg/errors"
	"sync"
	"time"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	return &core{
		lastMessageId:	 0,
		ttl:			 ttl,
		deliveredOnce:   false,
		isClosed:		 false,
		mutex: 			 sync.Mutex{},
		ackedMap:		 make(map[int]bool),
		messageQueue:	 make([]timedMessage, 900),
		deliveryChannel: make(chan ubroker.Delivery, 900),
	}
}

type timedMessage struct {
	acknowledge chan bool
	buildTime 	 time.Time
	content 	 ubroker.Delivery
}

type core struct {
	lastMessageId	int
	deliveredOnce	bool
	isClosed		bool
	mutex 			sync.Mutex
	ackedMap		map[int]bool
	ttl				time.Duration
	messageQueue	[]timedMessage
	deliveryChannel chan ubroker.Delivery
}

func (c *core) addMessageToQueue(message ubroker.Message) error {

	if c.isClosed {
		c.mutex.Unlock()
		return ubroker.ErrClosed
	}

	c.lastMessageId = c.lastMessageId + 1
	var timedM = constructTimedMessage(c.lastMessageId, message)
	c.messageQueue = append(c.messageQueue, timedM)
	c.ackedMap[timedM.content.ID] = false
	c.deliveryChannel <- timedM.content

	c.mutex.Unlock()
	go c.ensureMessageIsReached(timedM)
	return nil

}

func (c *core) getMessageFromQueueById(id int) *timedMessage {
	var index = c.getMessageFromQueueIndexById(id)
	if index == -1 {
		return nil
	} else {
		return &c.messageQueue[index]
	}
}

func (c *core) getMessageFromQueueIndexById(id int) int {
	for index, m := range c.messageQueue {
		if id == m.content.ID {
			return index
		}
	}
	return -1
}

func (c *core) removeMessageFromQueue(index int) *timedMessage {
	if index < 0 || index >= len(c.messageQueue) {
		return nil
	}
	var m = c.messageQueue[index]
	c.messageQueue[index] = c.messageQueue[len(c.messageQueue)-1]
	c.messageQueue = c.messageQueue[:len(c.messageQueue)-1]
	return &m
}

func (c *core) hasCrossedTTL(m timedMessage) bool {
	return time.Now().Sub(m.buildTime) > c.ttl
}

func (c *core) ensureMessageIsReached(message timedMessage) {
	select {
		case <- time.After(c.ttl):
			c.mutex.Lock()
			// remove from mainQ
			var requeueIndex = c.getMessageFromQueueIndexById(message.content.ID)
			if requeueIndex == -1 {
				// We should never reach here
				fmt.Print("Problem has occurred in ensuring that the message has been reached!")
			}
			c.removeMessageFromQueue(requeueIndex)
			_ = c.addMessageToQueue(message.content.Message)
		case <- message.acknowledge:
			c.mutex.Lock()
			if c.ackedMap[message.content.ID] {
				c.mutex.Unlock()
				return
			}
			c.mutex.Unlock()
	}
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	if checkContext(ctx) {
		return nil, ctx.Err()
	}
	c.mutex.Lock(); defer c.mutex.Unlock()
	if c.isClosed {
		return nil, ubroker.ErrClosed
	}
	c.deliveredOnce = true
	return c.deliveryChannel, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	
	if checkContext(ctx) {
		return ctx.Err()
	}
	
	c.mutex.Lock(); defer c.mutex.Unlock()
	if c.isClosed {
		return ubroker.ErrClosed
	}

	if !c.deliveredOnce {
		return errors.Wrap(ubroker.ErrInvalidID, "Message hasn't been delivered!")
	}
	if c.ackedMap[id] {
		return errors.Wrap(ubroker.ErrInvalidID, "Message with this id is already acked!")
	}

	var m = c.getMessageFromQueueById(id)
	if m == nil {
		return errors.Wrap(ubroker.ErrInvalidID, "No corresponding message to the id exists!")
	}
	if c.hasCrossedTTL(*m) {
		return errors.Wrap(ubroker.ErrInvalidID, "Message with this id has invalid time!")
	}

	c.ackedMap[id] = true
	m.acknowledge <- true
	return nil

}

func (c *core) ReQueue(ctx context.Context, id int) error {
	
	if checkContext(ctx) {
		return ctx.Err()
	}
	
	c.mutex.Lock()
	if c.isClosed {
		c.mutex.Unlock()
		return ubroker.ErrClosed
	}
	if !c.deliveredOnce {
		c.mutex.Unlock()
		return errors.Wrap(ubroker.ErrInvalidID, "Message hasn't been delivered!")
	}

	var requeueIndex = c.getMessageFromQueueIndexById(id)
	if requeueIndex == -1 {
		c.mutex.Unlock()
		return errors.Wrap(ubroker.ErrInvalidID, "No corresponding message to the id exists!")
	}

	var m = *c.removeMessageFromQueue(requeueIndex)
	return c.addMessageToQueue(m.content.Message)

}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	if checkContext(ctx) {
		return ctx.Err()
	}
	c.mutex.Lock()
	if c.isClosed {
		c.mutex.Unlock()
		return ubroker.ErrClosed
	}
	return c.addMessageToQueue(message)
}

func (c *core) Close() error {
	if c.isClosed {
		return nil
	}
	c.mutex.Lock(); defer c.mutex.Unlock()
	c.isClosed = true
	close(c.deliveryChannel)
 	return nil
}

func checkContext(ctx context.Context) bool {
	return map[error]bool {
    	context.Canceled: true,
    	context.DeadlineExceeded: true,
	}[ctx.Err()]
}

func constructTimedMessage(id int, message ubroker.Message) timedMessage {
	return timedMessage {
		acknowledge: make(chan bool, 100),
		buildTime: 	 time.Now(),
		content: 	 ubroker.Delivery {
			Message: message,
			ID: 	 id,
		},
	}
}