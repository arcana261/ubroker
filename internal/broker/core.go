package broker

import (
	"context"
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
	// checked ttl time
	if time.Now().Sub(c.mainQ[indexID].ttlTime) > c.ttl {
		return errors.Wrap(ubroker.ErrInvalidID, "id is invalid")
	} else {
		// acked
		c.ackedMessageID = append(c.ackedMessageID, id)
		return nil
	}

	return nil
}

func (c *core) ReQueue(ctx context.Context, id int) error {

	//c.wg.Done()
	//defer c.wg.Done()

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

	//check delivery
	if c.delFlag == false {
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
		return errors.Wrap(ubroker.ErrInvalidID, "id is invalid")
	}
	// check ttl time
	if time.Now().Sub(c.mainQ[indexID].ttlTime) > c.ttl {
		c.doReQ(c.mainQ[indexID])
		c.mainQ = append(c.mainQ[:indexID], c.mainQ[indexID+1:]...)
		return errors.Wrap(ubroker.ErrInvalidID, "id is invalid")
	} else {
		c.doReQ(c.mainQ[indexID])
		c.mainQ = append(c.mainQ[:indexID], c.mainQ[indexID+1:]...)
		return nil

	}
	return nil
}

func (c *core) doReQ(msg1 messageType) error {
	//c.wg.Add(1)
	//defer c.wg.Done()
	c.lastID = c.lastID + 1
	var newMsg ubroker.Delivery
	newMsg.Message = msg1.msg.Message
	newMsg.ID = c.lastID
	var newnewmsg = messageType{}
	newnewmsg.msg = newMsg
	newnewmsg.ttlTime = time.Now()

	c.mainQ = append(c.mainQ, newnewmsg)
	//send message to channel
	c.delChan <- newMsg
	return nil
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	//c.wg.Add(1)
	//defer c.wg.Done()

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

	c.lastID = c.lastID + 1
	var newMsg ubroker.Delivery
	newMsg.Message = message
	newMsg.ID = c.lastID
	//send message to channel
	c.delChan <- newMsg

	var newnewmsg = messageType{}
	newnewmsg.msg = newMsg
	newnewmsg.ttlTime = time.Now()
	c.mainQ = append(c.mainQ, newnewmsg)
	return nil
}

func (c *core) Close() error {
	//c.wg.Wait()
	if c.isClosed {
		return nil
	}
	c.Lock()
	defer c.Unlock()
	c.isClosed = true
	close(c.delChan)

	return nil
}
