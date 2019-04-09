package broker

import (
	"context"
	"time"

	"github.com/maedeazad/ubroker/pkg/ubroker"

	"sync"
	"log"
)

func print(s ...interface{}){
	log.Println(s...)

}

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	return &core{
		maxId: 0,
		ttl: ttl,
		msgs: make(map[int]*QueueElement),
		closed: false,
		deliveryChannel: make(chan ubroker.Delivery, 10000),
		deliveryCalled: false,
	}
}

type QueueElement struct {
	sync.Mutex

	id int
	msg string
	core *core
	timerCanceledChan chan struct{}
}

func (e *QueueElement) activateTimer(expTimer *time.Timer)  {
	go func() {
		select {
    case <-expTimer.C:
			e.core.reQueue(e.id)
		case <-e.timerCanceledChan:
    }
	}()
}

func (e *QueueElement) resetTimer(ttl time.Duration)  {
	e.stopTimer()
	e.activateTimer(time.NewTimer(e.core.ttl))
}

func (e *QueueElement) stopTimer()  {
	select {
		case e.timerCanceledChan <- struct{}{}:
    default:
  }
}

type core struct {
	sync.Mutex

	maxId int
	ttl time.Duration
	msgs map[int]*QueueElement
	closed bool
	deliveryChannel chan ubroker.Delivery
	deliveryCalled bool
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, ubroker.ErrClosed
	}

	c.deliveryCalled = true

	return c.deliveryChannel, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ubroker.ErrClosed
	}
	if !c.deliveryCalled{
		return ubroker.ErrInvalidID
	}

	if val, ok := c.msgs[id]; ok {
			val.stopTimer()
			delete(c.msgs, id)
	}else{
		return ubroker.ErrInvalidID
	}

	return nil
}

func (c *core) reQueue(id int) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ubroker.ErrClosed
	}
	if !c.deliveryCalled{
		return ubroker.ErrInvalidID
	}

	if val, ok := c.msgs[id]; ok {
		delete(c.msgs, id)
		val.stopTimer()

		c.addNewMessage(ubroker.Message{Body: val.msg})
	}else{
		return ubroker.ErrInvalidID
	}

	return nil
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.reQueue(id)
}

func (c *core) addNewMessage(message ubroker.Message){
	c.maxId += 1
	c.msgs[c.maxId] = &QueueElement{
		id: c.maxId,
		msg: message.Body,
		core: c,
		timerCanceledChan: make(chan struct{}),
	}
	c.msgs[c.maxId].activateTimer(time.NewTimer(c.ttl))
	c.deliveryChannel <- ubroker.Delivery{
													Message: message,
													ID: c.maxId,
											 }
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ubroker.ErrClosed
	}

	c.addNewMessage(message)
	return nil
}

func (c *core) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ubroker.ErrClosed
	}

	c.closed = true
	close(c.deliveryChannel)

	return nil
}
