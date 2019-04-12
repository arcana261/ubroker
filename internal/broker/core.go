package broker

import (
	"context"
	"math"
	"time"

	"github.com/amirmh/ubroker/pkg/ubroker"
	"sync"
)

var (
	lastID	int
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	return &core{
		isClosed: 				make(chan bool, 1),
		ttl: 					ttl,
		deliveryChannel: 		make(chan ubroker.Delivery, 1),
		processingQueue: 		make(map[int]waitForAckStruct),
		processingMutex:		&sync.Mutex{},
		publishMutex:			&sync.Mutex{},
		publishOrderMutex:		&sync.Mutex{},
		writeWaitGp: 			&sync.WaitGroup{},
		lastMsg:				ubroker.Delivery{ID:-1},
		publishQueue: 			make(chan ubroker.Message, math.MaxInt16),
		}
}


type core struct {
	isClosed			chan bool
	ttl					time.Duration
	deliveryChannel		chan ubroker.Delivery
	processingQueue		map[int]waitForAckStruct
	processingMutex		*sync.Mutex
	publishMutex		*sync.Mutex
	publishOrderMutex	*sync.Mutex
	lastMsg				ubroker.Delivery
	publishQueue		chan ubroker.Message
	writeWaitGp			*sync.WaitGroup
}

type waitForAckStruct struct {
	message		ubroker.Delivery
	ackChannnel	chan interface{}
}

func (c *core) waitForAck(ctx context.Context, ubrokerMsg waitForAckStruct) {
	select {
	case <-time.After(c.ttl):
		_ = c.ReQueue(ctx, ubrokerMsg.message.ID)
	case <-ctx.Done():
		return
	case <-c.isClosed:
		return
	case <-ubrokerMsg.ackChannnel:
		return
	}
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.isClosed:
		return nil, ubroker.ErrClosed
	default:
	}

	return c.deliveryChannel, nil
}

func (c *core) Acknowledge(ctx context.Context, id int) error {

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.isClosed:
		return ubroker.ErrClosed
	default:
	}

	//fmt.Println("lll")
	c.processingMutex.Lock()
	defer c.processingMutex.Unlock()
	waited, ok := c.processingQueue[id]
	if !ok {
		return ubroker.ErrInvalidID
	}
	close(waited.ackChannnel)
	delete(c.processingQueue, id)
	//fmt.Println("uuu")
	return nil
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.isClosed:
		return ubroker.ErrClosed
	default:
	}

	//fmt.Println("ll")
	c.processingMutex.Lock()
	defer c.processingMutex.Unlock()
	waited, ok := c.processingQueue[id]
	if !ok {
		return ubroker.ErrInvalidID
	}
	close(waited.ackChannnel)
	delete(c.processingQueue, id)
	_ = c.Publish(ctx, waited.message.Message)
	//fmt.Println("uu")
	return nil
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.isClosed:
		return ubroker.ErrClosed
	default:
	}


	c.writeWaitGp.Add(1)
	c.publishQueue <- message
	go func() {
		defer c.writeWaitGp.Done()
		c.publishMutex.Lock()

		lastID++
		brokerMsg := ubroker.Delivery{
			Message: <-c.publishQueue,
			ID:      lastID,
		}
		processingMsg := waitForAckStruct{
			message:     brokerMsg,
			ackChannnel: make(chan interface{}, 1),
		}

		c.processingMutex.Lock()
		c.processingQueue[processingMsg.message.ID] = processingMsg
		c.processingMutex.Unlock()

		select {
		case <-c.isClosed:
			return
		default:
			c.deliveryChannel <- brokerMsg
			go c.waitForAck(ctx, processingMsg)

		}
		c.publishMutex.Unlock()

	}()


	return nil
}

func (c *core) Close() error {
	c.writeWaitGp.Wait()
	close(c.deliveryChannel)
	close(c.isClosed)
	close(c.publishQueue)
	return nil
}
