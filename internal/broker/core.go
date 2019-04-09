package broker

import (
	"context"
	"time"

	"github.com/maedeazad/ubroker/pkg/ubroker"
	// "github.com/pkg/errors"

	// "container/list"
	"sync"

	"log"
)

func print(s ...interface{}){
	log.Println(s...)

	// br := bufio.NewWriter(os.Stdout)
  // logger := log.New(br, "", log.Ldate)
  // logger.Printf("%s\n", s)
  // br.Flush()
}

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	///print("New")
	// TODO timer for ttl
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
			///print("timer requeue", e.id, err)
		case <-e.timerCanceledChan:
			///print("timer aborted", e.id)
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
	///print("Delivery")
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
	///print("Acknowledge", id)
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
		// ///print("Acknowledge-not-exists")
		return ubroker.ErrInvalidID
	}

	// ///print("Acknowledge-ok")
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
	///print("ReQueue", id)
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
	///print("Publish")
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
	///print("Close")
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ubroker.ErrClosed
	}

	c.closed = true
	close(c.deliveryChannel)

	return nil
}
