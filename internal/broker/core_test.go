package broker_test

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/arcana261/ubroker/internal/broker"
	"github.com/arcana261/ubroker/pkg/ubroker"
	"github.com/pkg/errors"

	"github.com/stretchr/testify/suite"
)

type CoreBrokerTestSuite struct {
	suite.Suite

	broker ubroker.Broker
}

func TestCoreBrokerTestSuite(t *testing.T) {
	suite.Run(t, new(CoreBrokerTestSuite))
}

func (s *CoreBrokerTestSuite) TestInitialDeliveryShouldBeEmpty() {
	s.prepareTest(1 * time.Second)
	delivery := s.getDelivery(context.Background())
	s.assertEmpty(delivery)
}

func (s *CoreBrokerTestSuite) TestInitialAcknowledgeShouldReturnErrInvalidID() {
	s.prepareTest(1 * time.Second)
	s.assertErrorEquals(ubroker.ErrInvalidID, s.broker.Acknowledge(context.Background(), -1))
}

func (s *CoreBrokerTestSuite) TestInitialReQueueShouldReturnErrInvalidID() {
	s.prepareTest(1 * time.Second)
	s.assertErrorEquals(ubroker.ErrInvalidID, s.broker.ReQueue(context.Background(), -1))
}

func (s *CoreBrokerTestSuite) TestPublishedMessageShouldAppearInDeliveryOnce() {
	s.prepareTest(1 * time.Second)
	s.publish("hello1")
	delivery := s.getDelivery(context.Background())
	msg := <-delivery
	s.Equal("hello1", msg.Message.Body)
	s.assertEmpty(delivery)
}

func (s *CoreBrokerTestSuite) TestPublishShouldPreserveOrder() {
	s.prepareTest(1 * time.Second)
	s.publish("hello1")
	s.publish("hello2")
	s.publish("hello3")
	delivery := s.getDelivery(context.Background())
	messages := []ubroker.Delivery{<-delivery, <-delivery, <-delivery}
	s.NotEqual(messages[0].ID, messages[1].ID)
	s.NotEqual(messages[0].ID, messages[2].ID)
	s.NotEqual(messages[1].ID, messages[2].ID)
	s.Equal("hello1", messages[0].Message.Body)
	s.Equal("hello2", messages[1].Message.Body)
	s.Equal("hello3", messages[2].Message.Body)
}

func (s *CoreBrokerTestSuite) TestDeliveriesShouldBeUnique() {
	s.prepareTest(1 * time.Second)
	delivery1 := s.getDelivery(context.Background())
	delivery2 := s.getDelivery(context.Background())
	s.Equal(delivery1, delivery2)
}

func (s *CoreBrokerTestSuite) TestMessageShouldNotBeAcknowledgeablePreemptively() {
	s.prepareTest(1 * time.Second)
	s.publish("hello")
	for id := -100; id <= 100; id++ {
		s.assertErrorEquals(ubroker.ErrInvalidID, s.broker.Acknowledge(context.Background(), id))
	}
}

func (s *CoreBrokerTestSuite) TestMessageShouldNotBeQueueablePreemptively() {
	s.prepareTest(1 * time.Second)
	s.publish("hello")
	for id := -100; id <= 100; id++ {
		s.assertErrorEquals(ubroker.ErrInvalidID, s.broker.ReQueue(context.Background(), id))
	}
}

func (s *CoreBrokerTestSuite) TestDeliveryShouldBeAcknowledgeable() {
	s.prepareTest(1 * time.Second)
	s.publish("hello")
	msg := <-s.getDelivery(context.Background())
	s.Nil(s.broker.Acknowledge(context.Background(), msg.ID))
}

func (s *CoreBrokerTestSuite) TestDeliveryShouldNotBeAcknowledgedTwice() {
	s.prepareTest(1 * time.Second)
	s.publish("hello")
	msg := <-s.getDelivery(context.Background())
	s.Nil(s.broker.Acknowledge(context.Background(), msg.ID))
	s.assertErrorEquals(ubroker.ErrInvalidID, s.broker.Acknowledge(context.Background(), msg.ID))
}

func (s *CoreBrokerTestSuite) TestMultipleDeliveriesShouldBeAcknowledgeableIndependently() {
	s.prepareTest(1 * time.Second)
	s.publish("hello1")
	s.publish("hello2")
	s.publish("hello3")
	delivery := s.getDelivery(context.Background())
	messages := []ubroker.Delivery{<-delivery, <-delivery, <-delivery}
	s.Nil(s.broker.Acknowledge(context.Background(), messages[0].ID))
	s.Nil(s.broker.Acknowledge(context.Background(), messages[1].ID))
	s.Nil(s.broker.Acknowledge(context.Background(), messages[2].ID))
}

func (s *CoreBrokerTestSuite) TestAcknowledgedMessageShouldNotAppearInDelivery() {
	s.prepareTest(1 * time.Second)
	s.publish("hello")
	delivery := s.getDelivery(context.Background())
	msg := <-delivery
	s.Nil(s.broker.Acknowledge(context.Background(), msg.ID))
	s.assertEmpty(delivery)
}

func (s *CoreBrokerTestSuite) TestDeliveryShouldBeReQueueable() {
	s.prepareTest(1 * time.Second)
	s.publish("hello")
	delivery := s.getDelivery(context.Background())
	msg1 := <-delivery
	s.Nil(s.broker.ReQueue(context.Background(), msg1.ID))
	msg2 := <-delivery
	s.NotEqual(msg1.ID, msg2.ID)
	s.Equal(msg1.Message.Body, msg2.Message.Body)
}

func (s *CoreBrokerTestSuite) TestDeliveryShouldNotBeReQueueableTwice() {
	s.prepareTest(1 * time.Second)
	s.publish("hello")
	msg := <-s.getDelivery(context.Background())
	s.Nil(s.broker.ReQueue(context.Background(), msg.ID))
	s.assertErrorEquals(ubroker.ErrInvalidID, s.broker.ReQueue(context.Background(), msg.ID))
}

func (s *CoreBrokerTestSuite) TestMultipleDeliveriesShouldBeReQueueableIndependently() {
	s.prepareTest(1 * time.Second)
	s.publish("hello1")
	s.publish("hello2")
	s.publish("hello3")
	delivery := s.getDelivery(context.Background())
	messages := []ubroker.Delivery{<-delivery, <-delivery, <-delivery}
	s.Nil(s.broker.ReQueue(context.Background(), messages[0].ID))
	s.Nil(s.broker.ReQueue(context.Background(), messages[1].ID))
	s.Nil(s.broker.ReQueue(context.Background(), messages[2].ID))
	s.Equal(messages[0].Message.Body, (<-delivery).Message.Body)
	s.Equal(messages[1].Message.Body, (<-delivery).Message.Body)
	s.Equal(messages[2].Message.Body, (<-delivery).Message.Body)
}

func (s *CoreBrokerTestSuite) TestReQueueCouldBreakOrder() {
	s.prepareTest(1 * time.Second)
	s.publish("hello1")
	s.publish("hello2")
	s.publish("hello3")
	delivery := s.getDelivery(context.Background())
	messages := []ubroker.Delivery{<-delivery, <-delivery, <-delivery}
	s.Nil(s.broker.ReQueue(context.Background(), messages[2].ID))
	s.Nil(s.broker.ReQueue(context.Background(), messages[1].ID))
	s.Nil(s.broker.ReQueue(context.Background(), messages[0].ID))
	s.Equal(messages[2].Message.Body, (<-delivery).Message.Body)
	s.Equal(messages[1].Message.Body, (<-delivery).Message.Body)
	s.Equal(messages[0].Message.Body, (<-delivery).Message.Body)
}

func (s *CoreBrokerTestSuite) TestDeliveryShouldReQueueUponHalfSecondTTL() {
	s.prepareTest(500 * time.Millisecond)
	s.publish("hello1")
	delivery := s.getDelivery(context.Background())
	msg1 := <-delivery
	time.Sleep(250 * time.Millisecond)
	s.Nil(s.broker.Acknowledge(context.Background(), msg1.ID))
	s.publish("hello2")
	msg2 := <-delivery
	time.Sleep(750 * time.Millisecond)
	s.assertErrorEquals(ubroker.ErrInvalidID, s.broker.Acknowledge(context.Background(), msg2.ID))
	s.assertErrorEquals(ubroker.ErrInvalidID, s.broker.ReQueue(context.Background(), msg2.ID))
	msg3 := <-delivery
	s.NotEqual(msg1.ID, msg3.ID)
	s.NotEqual(msg2.ID, msg3.ID)
	s.Equal("hello2", msg3.Message.Body)
	s.Nil(s.broker.Acknowledge(context.Background(), msg3.ID))
}

func (s *CoreBrokerTestSuite) TestPublishShouldFailOnClosedBroker() {
	s.prepareClosed()
	s.assertErrorEquals(ubroker.ErrClosed, s.broker.Publish(context.Background(), ubroker.Message{}))
}

func (s *CoreBrokerTestSuite) TestAcknowledgeShouldFailOnClosedBroker() {
	s.prepareClosed()
	s.assertErrorEquals(ubroker.ErrClosed, s.broker.Acknowledge(context.Background(), 1))
}

func (s *CoreBrokerTestSuite) TestReQueueShouldFailOnClosedBroker() {
	s.prepareClosed()
	s.assertErrorEquals(ubroker.ErrClosed, s.broker.ReQueue(context.Background(), 1))
}

func (s *CoreBrokerTestSuite) TestDeliveryShouldFailOnClosedBroker() {
	s.prepareClosed()
	_, err := s.broker.Delivery(context.Background())
	s.assertErrorEquals(ubroker.ErrClosed, err)
}

func (s *CoreBrokerTestSuite) TestCloseShouldCloseDeliveryChannel() {
	s.prepareTest(1 * time.Second)
	delivery := s.getDelivery(context.Background())
	s.Nil(s.broker.Close())
	_, ok := <-delivery
	s.False(ok)
}

func (s *CoreBrokerTestSuite) TestDeliveryShouldFailOnCanceledContext() {
	s.prepareTest(1 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.broker.Delivery(ctx)
	s.assertErrorEquals(ctx.Err(), err)
}

func (s *CoreBrokerTestSuite) TestAcknowledgeShouldFailOnCanceledContext() {
	s.prepareTest(1 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.assertErrorEquals(ctx.Err(), s.broker.Acknowledge(ctx, 1))
}

func (s *CoreBrokerTestSuite) TestReQueueShouldFailOnCanceledContext() {
	s.prepareTest(1 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.assertErrorEquals(ctx.Err(), s.broker.ReQueue(ctx, 1))
}

func (s *CoreBrokerTestSuite) TestPublishShouldFailOnCanceledContext() {
	s.prepareTest(1 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.assertErrorEquals(ctx.Err(), s.broker.Publish(ctx, ubroker.Message{}))
}

func (s *CoreBrokerTestSuite) TestDataRace() {
	var wg sync.WaitGroup
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	s.prepareTest(1 * time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				return

			default:
				err := s.broker.Publish(context.Background(), ubroker.Message{
					Body: fmt.Sprint(rand.Intn(1000)),
				})
				if err == ubroker.ErrClosed {
					return
				}
				s.Nil(err)
				if err != nil {
					return
				}
				runtime.Gosched()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		delivery, err := s.broker.Delivery(context.Background())
		s.Nil(err)
		if err != nil {
			return
		}

		for {
			select {
			case <-ticker.C:
				return

			case msg := <-delivery:
				err = s.broker.Acknowledge(context.Background(), msg.ID)
				if err == ubroker.ErrClosed {
					return
				}
				s.Nil(err)
				if err != nil {
					return
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		delivery, err := s.broker.Delivery(context.Background())
		s.Nil(err)
		if err != nil {
			return
		}

		for {
			select {
			case <-ticker.C:
				return

			case msg := <-delivery:
				err = s.broker.ReQueue(context.Background(), msg.ID)
				if err == ubroker.ErrClosed {
					return
				}
				s.Nil(err)
				if err != nil {
					return
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-ticker.C
		s.Nil(s.broker.Close())
	}()

	wg.Wait()
}

func (s *CoreBrokerTestSuite) publish(body string) {
	s.Nil(s.broker.Publish(context.Background(), ubroker.Message{
		Body: body,
	}))
}

func (s *CoreBrokerTestSuite) assertErrorEquals(expected error, actual error) {
	if expected != actual {
		s.Equal(expected, errors.Cause(actual))
	} else {
		s.Equal(expected, actual)
	}
}

func (s *CoreBrokerTestSuite) getDelivery(ctx context.Context) <-chan ubroker.Delivery {
	result, err := s.broker.Delivery(ctx)
	if err != nil {
		s.FailNow(err.Error(), "could not obtain delivery")
	}

	return result
}

func (s *CoreBrokerTestSuite) assertEmpty(delivery <-chan ubroker.Delivery) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	select {
	case <-delivery:
		s.FailNow("expected delivery to be empty")

	case <-ticker.C:
	}
}

func (s *CoreBrokerTestSuite) prepareTest(ttl time.Duration) {
	s.broker = broker.New(ttl)
}

func (s *CoreBrokerTestSuite) prepareClosed() {
	s.broker = broker.New(1 * time.Second)
	s.Nil(s.broker.Close())
}

func (s *CoreBrokerTestSuite) CleanupTest() {
	s.Nil(s.broker.Close())
}
