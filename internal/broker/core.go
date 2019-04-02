package broker

import (
	"context"
	"sync"
	"time"

	"github.com/miladosos/ubroker/pkg/ubroker"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	broker := &core{
		ttl:                 ttl,
		acknowledgeRequests: make(chan *acknowledgeRequest),
		requeueRequests:     make(chan *requeueRequest),
		messages:            make(chan ubroker.Message),
		delivery:            make(chan ubroker.Delivery),
		closed:              make(chan bool, 1),
		closing:             make(chan bool, 1),
	}

	isReady := make(chan bool, 1)
	broker.wg.Add(1)
	go broker.startDelivery(isReady)
	<-isReady

	return broker
}

type core struct {
	ttl time.Duration

	mutex   sync.Mutex
	working sync.WaitGroup
	wg      sync.WaitGroup

	acknowledgeRequests chan *acknowledgeRequest
	requeueRequests     chan *requeueRequest
	messages            chan ubroker.Message
	delivery            chan ubroker.Delivery
	closed              chan bool
	closing             chan bool
}

type acknowledgeRequest struct {
	id       int
	response chan acknowledgeResponse
}

type acknowledgeResponse struct {
	id  int
	err error
}

type requeueRequest struct {
	id       int
	response chan requeueResponse
}

type requeueResponse struct {
	id  int
	err error
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	if c.isCanceled(ctx) {
		return nil, ctx.Err()
	}

	if !c.start() {
		return nil, ubroker.ErrClosed
	}
	defer c.working.Done()

	return c.delivery, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	if c.isCanceled(ctx) {
		return ctx.Err()
	}

	if !c.start() {
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
	case c.acknowledgeRequests <- request:
		select {
		case response := <-request.response:
			return response.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	if c.isCanceled(ctx) {
		return ctx.Err()
	}

	if !c.start() {
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
	case c.requeueRequests <- request:
		select {
		case response := <-request.response:
			return response.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	if c.isCanceled(ctx) {
		return ctx.Err()
	}

	if !c.start() {
		return ubroker.ErrClosed
	}
	defer c.working.Done()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return ubroker.ErrClosed
	case c.messages <- message:
		return nil
	}
}

func (c *core) Close() error {
	if !c.startClosing() {
		return nil
	}
	c.working.Wait()
	close(c.closed)
	c.wg.Wait()
	close(c.delivery)

	return nil
}

func (c *core) startDelivery(isReady chan bool) {
	defer c.wg.Done()
	nextID := 0
	pending := make(map[int]ubroker.Message)

	var outChannel chan ubroker.Delivery
	outSlice := []ubroker.Delivery{{}}

	messageHandler := func(msg ubroker.Message) {
		id := nextID
		nextID++
		newDelivery := ubroker.Delivery{
			ID:      id,
			Message: msg,
		}
		if outChannel == nil {
			outChannel = c.delivery
			outSlice = []ubroker.Delivery{newDelivery}
		} else {
			outSlice = append(outSlice, newDelivery)
		}
	}

	close(isReady)

	for {
		select {
		case <-c.closed:
			return

		case request := <-c.acknowledgeRequests:
			_, ok := pending[request.id]
			if !ok {
				request.response <- acknowledgeResponse{id: request.id, err: ubroker.ErrInvalidID}
			} else {
				delete(pending, request.id)
				request.response <- acknowledgeResponse{id: request.id, err: nil}
			}

		case request := <-c.requeueRequests:
			msg, ok := pending[request.id]
			if !ok {
				request.response <- requeueResponse{id: request.id, err: ubroker.ErrInvalidID}
			} else {
				delete(pending, request.id)
				messageHandler(msg)
				request.response <- requeueResponse{id: request.id}
			}

		case msg := <-c.messages:
			messageHandler(msg)

		case outChannel <- outSlice[0]:
			pending[outSlice[0].ID] = outSlice[0].Message
			c.wg.Add(1)
			go func(id int) {
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

					case c.requeueRequests <- request:
					}
				}
			}(outSlice[0].ID)

			outSlice = outSlice[1:]
			if len(outSlice) == 0 {
				outChannel = nil
				outSlice = []ubroker.Delivery{{}}
			}
		}
	}
}

func (c *core) start() bool {
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
	c.working.Add(1)
	return true
}

func (c *core) isCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
