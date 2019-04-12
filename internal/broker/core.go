package broker
import (
	"context"
	"github.com/arcana261/ubroker/pkg/ubroker"
	"github.com/pkg/errors"
	"sync"
	"time"
)

func New(ttl time.Duration) ubroker.Broker {
	core := &Core{
		deliveryEntry:    make(chan ubroker.Delivery, 100),
		requeueEntry:     make(chan requeue),
		publishEntry:     make([]request, 0),
		acknowledgeEntry: make([]int, 0),
		id:               0,
		ttl:              ttl,
		delivered:        false,
		isClosed:         false,
		close:            false,
	}
	return core
}

type Core struct {
	ttl              time.Duration
	deliveryEntry    chan ubroker.Delivery
	requeueEntry     chan requeue
	publishEntry     []request
	acknowledgeEntry []int
	delivered        bool
	isClosed         bool
	close            bool
	id               int
	requestGroup     sync.WaitGroup
	queueGroup       sync.WaitGroup
	requestHandle    sync.Mutex
	sync.Mutex
}

type request struct {
	msg     ubroker.Delivery
	ttlTime time.Time
	error   chan error
}
type acknowledge struct {
	id    int
	error chan error
}
type requeue struct {
	id    int
	error chan error
}

func (core *Core) startRequestHandle() error {
	core.requestHandle.Lock()
	if core.IsClosed() {
		return ubroker.ErrClosed
	}
	defer core.requestHandle.Unlock()
	core.requestGroup.Add(1)
	return nil
}
func (core *Core) IsClosed() bool {
	if core.close == true {
		return true
	} else {
		return false
	}
}
func (core *Core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	if ctx.Err() == context.Canceled {
		return nil, ctx.Err()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return nil, ctx.Err()
	}
	core.requestGroup.Add(1)
	defer core.requestGroup.Done()
	core.Lock()
	defer core.Unlock()
	if core.isClosed == true {
		return nil, ubroker.ErrClosed
	}
	core.delivered = true
	return core.deliveryEntry, nil
}

func (core *Core) Acknowledge(ctx context.Context, id int) error {
	if ctx.Err() == context.Canceled {
		return ctx.Err()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ctx.Err()
	}
	if err := core.startRequestHandling(); err != nil {
		return err
	}
	defer core.requestGroup.Done()
	r := acknowledge{
		id:    id,
		error: make(chan error, 1),
	}
	select {
		case <-ctx.Done():
			return ctx.Err()
		case core.acknowledgeEntry <- r:
			select {
				case <-ctx.Done():
					return ctx.Err()
				case err := <-r.error:
					return err
			}
	}
	core.Lock()
	defer core.Unlock()
	
	
	if ackIndex != -1 {
		return errors.Wrap(ubroker.ErrInvalidID, "Error")
	} else {
		core.acknowledgeEntry = append(core.acknowledgeEntry, id)
		return nil
	}

}

func (core *Core) ReQueue(ctx context.Context, id int) error {
	if ctx.Err() == context.Canceled {
		return ctx.Err()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ctx.Err()
	}
	core.Lock()
	defer core.Unlock()
	if core.isClosed == true {
		return ubroker.ErrClosed
	}
	if core.delivered == false {
		return errors.Wrap(ubroker.ErrInvalidID, "Error")
	}
	
	if index == -1 {
		return errors.Wrap(ubroker.ErrInvalidID, "Error")
	}
	if time.Now().Sub(core.publishEntry[index].ttlTime) > core.ttl {
		_ = core.reRequest(core.publishEntry[index])
		core.publishEntry = append(core.publishEntry[:index], core.publishEntry[index+1:]...)
		return errors.Wrap(ubroker.ErrInvalidID, "Error")
	} else {
		_ = core.reRequest(core.publishEntry[index])
		core.publishEntry = append(core.publishEntry[:index], core.publishEntry[index+1:]...)
		return nil

	}
}
func (core *Core) reRequest(req request) error {
	core.id = core.id + 1
	var newMsg ubroker.Delivery
	newMsg.Message = req.msg.Message
	newMsg.ID = core.id
	var newMessage = request{}
	newMessage.msg = newMsg
	newMessage.ttlTime = time.Now()
	core.publishEntry = append(core.publishEntry, newMessage)
	core.deliveryEntry <- newMsg
	return nil
}
func (core *Core) Publish(ctx context.Context, message ubroker.Message) error {

	if ctx.Err() == context.Canceled {
		return ctx.Err()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ctx.Err()
	}
	core.Lock()
	defer core.Unlock()
	if core.isClosed == true {
		return ubroker.ErrClosed
	}
	core.id = core.id + 1
	var newMsg ubroker.Delivery
	newMsg.Message = message
	newMsg.ID = core.id
	core.deliveryEntry <- newMsg
	var newMessage = request{}
	newMessage.msg = newMsg
	newMessage.ttlTime = time.Now()
	core.publishEntry = append(core.publishEntry, newMessage)
	return nil

}
func (core *Core) Close() error {
	if core.isClosed {
		return nil
	}
	core.Lock()
	defer core.Unlock()
	core.isClosed = true
	close(core.deliveryEntry)
	return nil
}
