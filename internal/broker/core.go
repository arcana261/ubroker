package broker

import (
	"context"
	"sync"
	"time"

	"github.com/erfan-mehraban/ubroker/pkg/ubroker"
)

type core struct {
	ttl               time.Duration
	deliveryChannel   chan ubroker.Delivery
	publishChannel    chan publishRequest
	requeueReqChannel chan requeueRequest
	ackReqChannel     chan ackRequest
	closed            chan struct{}
	closing           chan struct{}
	requestProcGroup  sync.WaitGroup
	queueProcGroup    sync.WaitGroup
	requestHandlingLock sync.Mutex // checking closing channel and add to requestProcGroup should be atomic)
}

type publishRequest struct {
	msg ubroker.Message
	resultErr chan error
}

type ackRequest struct {
	id        int
	resultErr chan error
}

type requeueRequest struct {
	id        int
	resultErr chan error
}

type pendingDelivery struct {
	delivery   ubroker.Delivery
	ackChannel chan struct{}
}

func (c *core) waitForAck(pd pendingDelivery) {
	defer c.queueProcGroup.Done()


	select {
	case <-pd.ackChannel:
		// if ack come from user, queue manager close this channel
		return

	case <-time.After(c.ttl): // timeout
		r := requeueRequest{
			id:        pd.delivery.ID,
			resultErr: make(chan error, 1),
		}

		select {
		case c.requeueReqChannel <- r:
		case <-c.closed:
			return
		}

	case <-c.closed:
		return
	}
}


func first(q []ubroker.Delivery) ubroker.Delivery {
	if len(q) == 0 {
		return ubroker.Delivery{}
	}
	return q[0]
}

func (c *core) startQueueManager() {

	c.queueProcGroup.Add(1)
	var (
		queue           []ubroker.Delivery
		deliveryChannel chan ubroker.Delivery
		lastID          int
	)
	ackPendingDeliveries := make(map[int]pendingDelivery)

	newID := func() int {
		lastID++
		return lastID
	}

	go func() {
		defer c.queueProcGroup.Done()

		for {
			select {

			case publishMassage := <-c.publishChannel: // new message arrived
				msg := publishMassage.msg
				deliveryMessage := ubroker.Delivery{
					ID:      newID(),
					Message: msg,
				}
				if deliveryChannel == nil {
					//open deliveryChannel because queue isn't empty anymore
					deliveryChannel = c.deliveryChannel
				}
				queue = append(queue, deliveryMessage)
				//publishMassage.resultErr <- nil
				close(publishMassage.resultErr)

			case deliveryChannel <- first(queue): // front of queue delivered
				// create goroutine to wait for ack and add to pending map
				deliveredItem := first(queue)
				pd := pendingDelivery{
					delivery: deliveredItem,
					ackChannel: make(chan struct{}, 1),
				}
				ackPendingDeliveries[deliveredItem.ID] = pd
				c.queueProcGroup.Add(1)
				go c.waitForAck(pd)

				//remove front from queue
				if len(queue) > 1 {
					queue = queue[1:]
				} else { // there isn't any item ready to send in deliveryChannel
					queue = nil
					deliveryChannel = nil
				}

			case ack := <-c.ackReqChannel:
				pd, ok := ackPendingDeliveries[ack.id]
				if !ok {
					ack.resultErr <- ubroker.ErrInvalidID
				} else {
					close(pd.ackChannel) // tell relevant waitForAck goroutine to not wait anymore
					delete(ackPendingDeliveries, ack.id)
					ack.resultErr <- nil
				}

			case req := <-c.requeueReqChannel:
				pd, ok := ackPendingDeliveries[req.id]
				if !ok {
					req.resultErr <- ubroker.ErrInvalidID
				} else {
					close(pd.ackChannel)
					delete(ackPendingDeliveries, req.id)

					if deliveryChannel == nil {
						//open deliveryChannel because queue isn't empty anymore
						deliveryChannel = c.deliveryChannel
					}

					pd.delivery.ID = newID()
					// append delivery to queue is wrong according to issue #10 on github
					// correct: queue = append([]ubroker.Delivery{rd}, queue...)
					queue = append(queue, pd.delivery)

					req.resultErr <- nil
				}

			case <-c.closed:
				return
			}
		}
	}()
}

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	c := &core{
		ttl:               ttl,
		deliveryChannel:   make(chan ubroker.Delivery),
		publishChannel:    make(chan publishRequest),
		requeueReqChannel: make(chan requeueRequest),
		ackReqChannel:     make(chan ackRequest),
		closed:            make(chan struct{}),
		closing:           make(chan struct{}),
	}
	c.startQueueManager()
	return c
}

func (c *core) startRequestHandling() error{
	c.requestHandlingLock.Lock()
	defer c.requestHandlingLock.Unlock()
	if c.isClosedBefore() {
		return ubroker.ErrClosed
	}
	c.requestProcGroup.Add(1)
	return nil
}

// is currently closing or closed before
func (c *core) isClosedBefore() bool{
	select {
	case <-c.closing:
		return true
	default:
	}
	return false
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	select {
	case <-c.closing:
		return nil, ubroker.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	c.requestProcGroup.Add(1)
	defer c.requestProcGroup.Done()
	return c.deliveryChannel, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	if err:=c.startRequestHandling(); err!=nil{
		return err
	}
	defer c.requestProcGroup.Done()
	r := ackRequest{
		id:        id,
		resultErr: make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.ackReqChannel <- r:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-r.resultErr:
			return err
		}
	}
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	if err:=c.startRequestHandling(); err!=nil{
		return err
	}
	defer c.requestProcGroup.Done()
	r := requeueRequest{
		id:        id,
		resultErr: make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.requeueReqChannel <- r:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-r.resultErr:
			return err
		}
	}
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	if err:=c.startRequestHandling(); err!=nil{
		return err
	}
	defer c.requestProcGroup.Done()

	pr := publishRequest{
		msg: message,
		resultErr: make(chan error, 1),
	}

	select {
	case c.publishChannel <- pr:
		select {
		case <-pr.resultErr:
			return nil
		case <-c.closed:
			return ubroker.ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-c.closed:
		return ubroker.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *core) Close() error {

	// prevent race condition between requestProcGroup.Wait and requestProcGroup.Add
	c.requestHandlingLock.Lock()
	if !c.isClosedBefore(){
		close(c.closing)
	}
	c.requestHandlingLock.Unlock()

	// in closing phase we shut down request handling and let them all finish
	c.requestProcGroup.Wait()
	close(c.closed)
	// after that we wait for queue manager to end his work and then close related channels
	c.queueProcGroup.Wait()
	close(c.requeueReqChannel)
	close(c.ackReqChannel)
	close(c.deliveryChannel)
	close(c.publishChannel)
	return nil
}
