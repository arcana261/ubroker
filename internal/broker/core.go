package broker

import (
	"context"
	"github.com/arcana261/ubroker/pkg/ubroker"
	"github.com/pkg/errors"
	"sync"
	"time"
)

func New(ttl time.Duration) ubroker.Broker {
	//return &core{}
	core := &Core{
		deliveryEntry:    make(chan ubroker.Delivery, 100),
		requeueEntry:     make(chan requeue),
		publishEntry:     make([]request, 100),
		acknowledgeEntry: make([]int, 100),
		ttl:              ttl,
		delivered:        false,
		isClosed:         false,
		close:            false,
		id:               0,
	}
	return core
}

type Core struct {
	// TODO: add required fields
	ttl              time.Duration
	deliveryEntry    chan ubroker.Delivery
	requeueEntry     chan requeue
	publishEntry     []request
	acknowledgeEntry []int
	delivered        bool
	isClosed         bool
	close            bool
	id               int
	queueGroup       sync.WaitGroup
	syncCore         sync.Mutex
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

func (core *Core) startRequest() error {
	core.syncCore.Lock()
	if core.IsClosed() {
		return ubroker.ErrClosed
	}
	defer core.syncCore.Unlock()
	core.queueGroup.Add(1)
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
	// TODO:‌ implement me
	//return nil, errors.Wrap(ubroker.ErrUnimplemented, "method Delivery is not implemented")
	if ctx.Err() == context.Canceled {
		return nil, ctx.Err()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return nil, ctx.Err()
	}
	core.queueGroup.Add(1)
	defer core.queueGroup.Done()
	core.syncCore.Lock()
	defer core.syncCore.Unlock()
	if core.isClosed == true {
		return nil, ubroker.ErrClosed
	}
	core.delivered = true
	return core.deliveryEntry, nil
}

func (core *Core) Acknowledge(ctx context.Context, id int) error {
	// TODO:‌ implement me
	//return errors.Wrap(ubroker.ErrUnimplemented, "method Acknowledge is not implemented")
	if ctx.Err() == context.Canceled {
		return ctx.Err()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ctx.Err()
	}
	core.syncCore.Lock()
	defer core.syncCore.Unlock()
	if core.isClosed == true {
		return ubroker.ErrClosed
	}
	if core.delivered == false {
		return errors.Wrap(ubroker.ErrInvalidID, "Error")
	}
	var index = -1
	for counter, message := range core.publishEntry {
		if message.msg.ID == id {
			index = counter
			break
		}
	}
	var ackId = -1
	for counter, checkId := range core.acknowledgeEntry {
		if checkId == id {
			ackId = counter
			break
		}
	}
	if index == -1 {
		return errors.Wrap(ubroker.ErrInvalidID, "Error")
	}
	if ackId != -1 {
		return errors.Wrap(ubroker.ErrInvalidID, "Error")
	}
	if time.Now().Sub(core.publishEntry[index].ttlTime) > core.ttl {
		return errors.Wrap(ubroker.ErrInvalidID, "Error")
	} else {
		core.acknowledgeEntry = append(core.acknowledgeEntry, id)
		return nil
	}
}

func (core *Core) ReQueue(ctx context.Context, id int) error {
	// TODO:‌ implement me
	//return errors.Wrap(ubroker.ErrUnimplemented, "method ReQueue is not implemented")
	if ctx.Err() == context.Canceled {
		return ctx.Err()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ctx.Err()
	}
	core.syncCore.Lock()
	defer core.syncCore.Unlock()
	if core.isClosed == true {
		return ubroker.ErrClosed
	}
	if core.delivered == false {
		return errors.Wrap(ubroker.ErrInvalidID, "Error")
	}
	var index = -1
	for counter, message := range core.publishEntry {
		if message.msg.ID == id {
			index = counter
			break
		}
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
	var newMsgBroker ubroker.Delivery
	newMsgBroker.Message = req.msg.Message
	newMsgBroker.ID = core.id
	var newMessage = request{}
	newMessage.msg = newMsgBroker
	newMessage.ttlTime = time.Now()
	core.publishEntry = append(core.publishEntry, newMessage)
	core.deliveryEntry <- newMsgBroker
	return nil
}
func (core *Core) Publish(ctx context.Context, message ubroker.Message) error {
	// TODO:‌ implement me
	//return errors.Wrap(ubroker.ErrUnimplemented, "method Publish is not implemented")
	if ctx.Err() == context.Canceled {
		return ctx.Err()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ctx.Err()
	}
	core.syncCore.Lock()
	defer core.syncCore.Unlock()
	if core.isClosed == true {
		return ubroker.ErrClosed
	}
	//request  {message,0,""}
	//_ = core.reRequest(request{message,0,""})
	core.id = core.id + 1
	var newMsgBroker ubroker.Delivery
	newMsgBroker.Message = message
	newMsgBroker.ID = core.id
	core.deliveryEntry <- newMsgBroker
	var newMessage = request{}
	newMessage.msg = newMsgBroker
	newMessage.ttlTime = time.Now()
	core.publishEntry = append(core.publishEntry, newMessage)
	return nil

}
func (core *Core) Close() error {
	// TODO:‌ implement me
	//return errors.Wrap(ubroker.ErrUnimplemented, "method Close is not implemented")
	if core.isClosed {
		return nil
	}
	core.syncCore.Lock()
	defer core.syncCore.Unlock()
	core.isClosed = true
	close(core.deliveryEntry)
	return nil
}
