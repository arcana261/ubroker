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

	c := &core{
		delFlag:        false,
		isClosed:       false,
		delChan:        make(chan ubroker.Delivery, 100),
		mainQ:          make([]messageType, 0),
		lastID:         0,
		ackedMessageID: make([]int, 0),
		ttl:            ttl,
	}
	return c
}

type core struct {
	sync.Mutex
	isClosed       bool
	delChan        chan ubroker.Delivery
	lastID         int
	mainQ          []messageType
	ttl            time.Duration
	delFlag        bool
	ackedMessageID []int
}

type messageType struct {
	msg     ubroker.Delivery
	ttlTime time.Time
	ackChan chan int
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {

	switch ctx.Err() {
	case context.Canceled:
		return nil, ctx.Err()
	case context.DeadlineExceeded:
		return nil, ctx.Err()
	}
	c.Lock()
	defer c.Unlock()
	if c.isClosed == true {
		return nil, ubroker.ErrClosed
	}
	c.delFlag = true
	return c.delChan, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {

	switch ctx.Err() {
	case context.Canceled:
		return ctx.Err()
	case context.DeadlineExceeded:
		return ctx.Err()
	}
	c.Lock()
	defer c.Unlock()
	if c.isClosed == true {
		return ubroker.ErrClosed
	}

	// check delivery done
	if c.delFlag == false {
		return errors.Wrap(ubroker.ErrInvalidID, "id is invalid")
	}

	// published
	var indexID = -1
	for index, message := range c.mainQ {
		if message.msg.ID == id {
			indexID = index
			break
		}
	}

	// acked befor
	var ackIndex = -1
	for index, ids := range c.ackedMessageID {
		if ids == id {
			ackIndex = index
			break
		}
	}

	if indexID == -1 {
		return errors.Wrap(ubroker.ErrInvalidID, "id is invalid")
	}
	if ackIndex != -1 {
		return errors.Wrap(ubroker.ErrInvalidID, "id is invalid")
	}
	// acked
	c.ackedMessageID = append(c.ackedMessageID, id)
	//fmt.Print("-------------->",indexID)
	c.mainQ[indexID].ackChan <- 1
	return nil
}

func (c *core) ReQueue(ctx context.Context, id int) error {

	switch ctx.Err() {
	case context.Canceled:
		return ctx.Err()
	case context.DeadlineExceeded:
		return ctx.Err()
	}

	c.Lock()
	if c.isClosed == true {
		c.Unlock()
		return ubroker.ErrClosed
	}

	//check delivery
	if c.delFlag == false {
		c.Unlock()
		return errors.Wrap(ubroker.ErrInvalidID, "id is invalid")
	}

	// published?
	var indexID = -1
	for index, message := range c.mainQ {
		if message.msg.ID == id {
			indexID = index
			break
		}
	}
	if indexID == -1 {
		c.Unlock()
		return errors.Wrap(ubroker.ErrInvalidID, "id is invalid")
	}

	var msg = c.mainQ[indexID]
	c.mainQ = append(c.mainQ[:indexID], c.mainQ[indexID+1:]...)
	c.doReQ(msg)

	return nil
}

func (c *core) doReQ(msg1 messageType) error {
	if c.isClosed == true {
		c.Unlock()
		return ubroker.ErrClosed
	}
	c.lastID = c.lastID + 1
	var newMsg ubroker.Delivery
	newMsg.Message = msg1.msg.Message
	newMsg.ID = c.lastID
	var newnewmsg = messageType{}
	newnewmsg.msg = newMsg
	newnewmsg.ttlTime = time.Now()
	newnewmsg.ackChan = make(chan int, 2)
	c.mainQ = append(c.mainQ, newnewmsg)

	//send message to channel
	c.delChan <- newMsg
	c.Unlock()

	go c.checkTTL(newnewmsg)
	return nil
}

func (c *core) checkTTL(msg messageType) {
	select {
	case <-time.After(c.ttl):
		c.Lock()
		// remove from mainQ
		var indexID = -1
		for index, message := range c.mainQ {
			if message.msg.ID == msg.msg.ID {
				indexID = index
				break
			}
		}
		if indexID != -1 {
			c.mainQ = append(c.mainQ[:indexID], c.mainQ[indexID+1:]...)
		}
		// call reQ again
		c.doReQ(msg)
		return
	case <-msg.ackChan:
		return
	}
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	switch ctx.Err() {

	case context.Canceled:
		return ctx.Err()
	case context.DeadlineExceeded:
		return ctx.Err()
	}
	c.Lock()
	if c.isClosed == true {
		c.Unlock()
		return ubroker.ErrClosed
	}

	c.lastID = c.lastID + 1
	var newMsg ubroker.Delivery
	newMsg.Message = message
	newMsg.ID = c.lastID
	//send message to channel
	c.delChan <- newMsg
	var newnewmsg = messageType{}
	newnewmsg.msg = newMsg
	newnewmsg.ttlTime = time.Now()
	newnewmsg.ackChan = make(chan int, 2)
	c.mainQ = append(c.mainQ, newnewmsg)
	c.Unlock()

	go c.checkTTL(newnewmsg)
	return nil
}

func (c *core) Close() error {

	if c.isClosed {
		return nil
	}
	c.Lock()
	defer c.Unlock()
	c.isClosed = true
	close(c.delChan)

	return nil
}
