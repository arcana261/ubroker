package broker

import (
	"context"
	"sync"
	"time"

	"github.com/arcana261/ubroker/pkg/ubroker"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	result := &core{
		delivery:        make(chan ubroker.Delivery),
		messageDelivery: make(chan ubroker.Message, 1000),
		closed:          make(chan struct{}),
		drain:           make(chan struct{}),
		acknowledge:     make(chan acknowledgeRequest),
		requeue:         make(chan requeueRequest),
		ttl:             ttl,
	}

	started := make(chan struct{})
	result.wg.Add(1)
	go result.beginDelivery(started)
	<-started

	return result
}

type core struct {
	mutex           sync.Mutex
	delivery        chan ubroker.Delivery
	messageDelivery chan ubroker.Message
	closed          chan struct{}
	drain           chan struct{}
	acknowledge     chan acknowledgeRequest
	requeue         chan requeueRequest
	ttl             time.Duration
	operating       sync.WaitGroup
	wg              sync.WaitGroup
}

type acknowledgeRequest struct {
	id     int
	result chan error
}

type requeueRequest struct {
	id     int
	result chan error
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	if !c.begin() {
		return nil, ubroker.ErrClosed
	}
	defer c.operating.Done()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	default:
	}

	return c.delivery, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	if !c.begin() {
		return ubroker.ErrClosed
	}
	defer c.operating.Done()

	request := acknowledgeRequest{
		id:     id,
		result: make(chan error, 1),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case c.acknowledge <- request:
		select {
		case err := <-request.result:
			return err

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	if !c.begin() {
		return ubroker.ErrClosed
	}
	defer c.operating.Done()

	request := requeueRequest{
		id:     id,
		result: make(chan error, 1),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case c.requeue <- request:
		select {
		case err := <-request.result:
			return err

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	if !c.begin() {
		return ubroker.ErrClosed
	}
	defer c.operating.Done()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-c.closed:
		return ubroker.ErrClosed

	case c.messageDelivery <- message:
		return nil
	}
}

func (c *core) Close() error {
	if !c.beginClose() {
		return nil
	}

	c.operating.Wait()
	close(c.closed)
	c.wg.Wait()
	close(c.delivery)

	return nil
}

func (c *core) beginClose() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	select {
	case <-c.drain:
		return false

	default:
		close(c.drain)
		return true
	}
}

func (c *core) begin() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	select {
	case <-c.drain:
		return false

	default:
	}

	c.operating.Add(1)
	return true
}

func (c *core) beginDelivery(started chan struct{}) {
	defer c.wg.Done()

	nextID := 0
	pending := make(map[int]ubroker.Message)

	var requeueChannel chan ubroker.Message
	requeueSlice := []ubroker.Message{ubroker.Message{}}

	var outChannel chan ubroker.Delivery
	var out ubroker.Delivery

	input := c.messageDelivery

	close(started)

	for {
		select {
		case <-c.closed:
			return

		case request := <-c.acknowledge:
			_, ok := pending[request.id]
			if !ok {
				request.result <- ubroker.ErrInvalidID
			} else {
				delete(pending, request.id)
				request.result <- nil
			}

		case request := <-c.requeue:
			msg, ok := pending[request.id]
			if !ok {
				request.result <- ubroker.ErrInvalidID
			} else {
				delete(pending, request.id)
				if requeueChannel == nil {
					requeueChannel = c.messageDelivery
					requeueSlice = []ubroker.Message{msg}
				} else {
					requeueSlice = append(requeueSlice, msg)
				}
				request.result <- nil
			}

		case requeueChannel <- requeueSlice[0]:
			requeueSlice = requeueSlice[1:]
			if len(requeueSlice) == 0 {
				requeueChannel = nil
				requeueSlice = []ubroker.Message{ubroker.Message{}}
			}

		case msg := <-input:
			id := nextID
			nextID++
			out = ubroker.Delivery{
				ID:      id,
				Message: msg,
			}
			outChannel = c.delivery
			input = nil

		case outChannel <- out:
			outChannel = nil
			input = c.messageDelivery
			pending[out.ID] = out.Message
			c.wg.Add(1)
			go func(id int) {
				defer c.wg.Done()

				ticker := time.NewTicker(c.ttl)
				defer ticker.Stop()

				select {
				case <-c.closed:
					return

				case <-ticker.C:
					request := requeueRequest{
						id:     id,
						result: make(chan error, 1),
					}

					select {
					case <-c.closed:
						return

					case c.requeue <- request:
					}
				}
			}(out.ID)
		}
	}
}
