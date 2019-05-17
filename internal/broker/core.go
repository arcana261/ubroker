package broker

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/erfan-mehraban/ubroker/pkg/ubroker"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	broker := &core{
		ttl:             ttl,
		requests:        make(chan interface{}),
		deliveryChannel: make(chan *ubroker.Delivery),
		closed:          make(chan bool, 1),
		closing:         make(chan bool, 1),
		pending:         make(map[int32]*ubroker.Message),
		messages:        []*ubroker.Delivery{{}},
	}

	broker.wg.Add(1)
	go broker.startDelivery()

	return broker
}

type core struct {
	nextID int32
	ttl    time.Duration

	mutex   sync.Mutex
	working sync.WaitGroup
	wg      sync.WaitGroup

	requests        chan interface{}
	deliveryChannel chan *ubroker.Delivery
	closed          chan bool
	closing         chan bool
	pending         map[int32]*ubroker.Message
	messages        []*ubroker.Delivery
	channel         chan *ubroker.Delivery
}

type acknowledgeRequest struct {
	id       int32
	response chan acknowledgeResponse
}

type acknowledgeResponse struct {
	id  int32
	err error
}

type requeueRequest struct {
	id       int32
	response chan requeueResponse
}

type requeueResponse struct {
	id  int32
	err error
}

type publishRequest struct {
	message  *ubroker.Message
	response chan publishResponse
}

type publishResponse struct {
	err error
}

func (c *core) Delivery(ctx context.Context) (<-chan *ubroker.Delivery, error) {
	if isCanceledContext(ctx) {
		return nil, ctx.Err()
	}

	if !c.startWorking() {
		return nil, ubroker.ErrClosed
	}
	defer c.working.Done()

	return c.deliveryChannel, nil
}

func (c *core) Acknowledge(ctx context.Context, id int32) error {
	if isCanceledContext(ctx) {
		return ctx.Err()
	}

	if !c.startWorking() {
		return ubroker.ErrClosed
	}
	defer c.working.Done()

	request := &acknowledgeRequest{
		id:       id,
		response: make(chan acknowledgeResponse, 1),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.requests <- request:
		select {
		case response := <-request.response:
			return response.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *core) ReQueue(ctx context.Context, id int32) error {
	if isCanceledContext(ctx) {
		return ctx.Err()
	}

	if !c.startWorking() {
		return ubroker.ErrClosed
	}
	defer c.working.Done()

	request := &requeueRequest{
		id:       id,
		response: make(chan requeueResponse, 1),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.requests <- request:
		select {
		case response := <-request.response:
			return response.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *core) Publish(ctx context.Context, message *ubroker.Message) error {
	if isCanceledContext(ctx) {
		return ctx.Err()
	}

	if !c.startWorking() {
		return ubroker.ErrClosed
	}
	defer c.working.Done()

	request := &publishRequest{
		message:  message,
		response: make(chan publishResponse, 1),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return ubroker.ErrClosed
	case c.requests <- request:
		return nil
	}
}

func (c *core) Close() error {
	if !c.startClosing() {
		return errors.New("can not close channel, closing in progress")
	}
	c.working.Wait()
	close(c.closed)
	c.wg.Wait()
	close(c.deliveryChannel)

	return nil
}

func (c *core) startDelivery() {
	defer c.wg.Done()
	for {
		select {
		case <-c.closed:
			return

		case request := <-c.requests:
			if isAcknowledgeRequest(request) {
				c.wg.Add(1)
				req, _ := request.(*acknowledgeRequest)
				req.response <- c.handleAcknowledge(req)
			} else if isRequeueRequest(request) {
				c.wg.Add(1)
				req, _ := request.(*requeueRequest)
				req.response <- c.handleRequeue(req)
			} else if isPublishRequest(request) {
				c.wg.Add(1)
				req, _ := request.(*publishRequest)
				req.response <- c.handlePublish(req)
			} else {
				panic(errors.New("UNKNOWN REQUEST"))
			}

		case c.channel <- c.messages[0]:
			if c.channel != nil {
				c.pending[c.messages[0].Id] = c.messages[0].Message
				c.wg.Add(1)
				go c.snooze(c.messages[0].Id)

				c.messages = c.messages[1:]
				if len(c.messages) == 0 {
					c.channel = nil
					c.messages = []*ubroker.Delivery{{}}
				}
			}
		}
	}
}

func (c *core) startWorking() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	select {
	case <-c.closing:
		return false
	default:
		c.working.Add(1)
		return true
	}
}

func (c *core) startClosing() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	select {
	case <-c.closing:
		return false
	default:
		close(c.closing)
		return true
	}
}

func isCanceledContext(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func isAcknowledgeRequest(request interface{}) bool {
	_, ok := request.(*acknowledgeRequest)
	return ok
}

func isRequeueRequest(request interface{}) bool {
	_, ok := request.(*requeueRequest)
	return ok
}

func isPublishRequest(request interface{}) bool {
	_, ok := request.(*publishRequest)
	return ok
}

func (c *core) handleAcknowledge(request *acknowledgeRequest) acknowledgeResponse {
	defer c.wg.Done()
	_, ok := c.pending[request.id]
	if !ok {
		return acknowledgeResponse{id: request.id, err: ubroker.ErrInvalidID}
	}
	delete(c.pending, request.id)
	return acknowledgeResponse{id: request.id, err: nil}
}

func (c *core) handleRequeue(request *requeueRequest) requeueResponse {
	defer c.wg.Done()
	message, ok := c.pending[request.id]
	if !ok {
		return requeueResponse{id: request.id, err: ubroker.ErrInvalidID}
	}
	delete(c.pending, request.id)
	c.wg.Add(1)
	c.handlePublish(&publishRequest{
		message:  message,
		response: make(chan publishResponse, 1),
	})
	return requeueResponse{id: request.id, err: nil}
}

func (c *core) handlePublish(request *publishRequest) publishResponse {
	defer c.wg.Done()

	if c.channel == nil {
		c.messages = []*ubroker.Delivery{}
		c.channel = c.deliveryChannel
	}

	id := c.nextID
	c.nextID++
	newDelivery := ubroker.Delivery{
		Id:      id,
		Message: request.message,
	}

	c.messages = append(c.messages, &newDelivery)

	return publishResponse{err: nil}
}

func (c *core) snooze(id int32) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.ttl)
	defer ticker.Stop()

	select {
	case <-c.closed:
		return

	case <-ticker.C:
		request := &requeueRequest{
			id:       id,
			response: make(chan requeueResponse, 1),
		}
		select {
		case <-c.closed:
			return

		case c.requests <- request:
		}
	}
}
