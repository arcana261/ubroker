package broker

import (
	"context"
	"time"
	"sync"
	"github.com/mohamad-amin/ubroker/pkg/ubroker"
	"github.com/pkg/errors"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	return &core{
		lastMessageId:	 0,
		deliveredOnce:   false,
		isClosed:		 false,
		mutex: 			 sync.Mutex{},
		ackedMap:		 make(map[int]bool),
		ttl:			 ttl,
		messageQueue:	 make([]timedMessage, 0),
		deliveryChannel: make(chan ubroker.Delivery, 100),
	}
}

type timedMessage struct {
	buildTime 	time.Time
	content 	ubroker.Delivery
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

func (c *core) addMessageToQueue(message ubroker.Message) {
	c.lastMessageId = c.lastMessageId + 1
	var timedM = constructTimedMessage(c.lastMessageId, message)
	c.messageQueue = append(c.messageQueue, timedM)
	c.ackedMap[timedM.content.ID] = false
	c.deliveryChannel <- timedM.content
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

func (c *core) removeMessageFromQueue(index int) timedMessage {
	var m = c.messageQueue[index]
	c.messageQueue[index] = c.messageQueue[len(c.messageQueue)-1]
	c.messageQueue = c.messageQueue[:len(c.messageQueue)-1]
	return m
}

func (c *core) hasCrossedTTL(m timedMessage) bool {
	return time.Now().Sub(m.buildTime) > c.ttl
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
	return nil

}

func (c *core) ReQueue(ctx context.Context, id int) error {
	
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

	var requeueIndex = c.getMessageFromQueueIndexById(id)
	if requeueIndex == -1 {
		return errors.Wrap(ubroker.ErrInvalidID, "No corresponding message to the id exists!")
	}

	var m = c.removeMessageFromQueue(requeueIndex)
	c.addMessageToQueue(m.content.Message)
	if c.hasCrossedTTL(m) {
		return errors.Wrap(ubroker.ErrInvalidID, "Message with this id has invalid time!")
	} else {
		return nil
 	}

}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	if checkContext(ctx) {
		return ctx.Err()
	}
	c.mutex.Lock(); defer c.mutex.Unlock()
	if c.isClosed {
		return ubroker.ErrClosed
	}
	c.addMessageToQueue(message)
	return nil
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
		buildTime: 	 time.Now(),
		content: 	 ubroker.Delivery {
			Message: message,
			ID: 	 id,
		},
	}
}