package server_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/sneyes/ubroker/internal/server"
	"github.com/sneyes/ubroker/pkg/ubroker"
	"github.com/phayes/freeport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/stretchr/testify/suite"
)

type GRPCServerTestSuite struct {
	suite.Suite
}

func TestGRPCServerTestSuite(t *testing.T) {
	suite.Run(t, new(GRPCServerTestSuite))
}

func (s *GRPCServerTestSuite) TestFetchShouldReturnUnavailableIfClosed() {
	broker := &mockBroker{}
	broker.On("Delivery", mock.Anything).Once().Return(nil, ubroker.ErrClosed)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		stream, err := client.Fetch(ctx, grpc.WaitForReady(true))
		s.Nil(err)

		s.Nil(stream.Send(&ubroker.FetchRequest{}))

		_, err = stream.Recv()
		s.assertStatusCode(codes.Unavailable, err)

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestFetchShouldReturnUnavailableIfDeliveryClosed() {
	broker := &mockBroker{}
	broker.On("Delivery", mock.Anything).Once().Return(s.makeChannel(), nil)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		stream, err := client.Fetch(ctx, grpc.WaitForReady(true))
		s.Nil(err)

		s.Nil(stream.Send(&ubroker.FetchRequest{}))

		_, err = stream.Recv()
		s.assertStatusCode(codes.Unavailable, err)

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestFetchShouldReturnOneItem() {
	broker := &mockBroker{}
	broker.On("Delivery", mock.Anything).Once().Return(s.makeChannel("hello"), nil)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		stream, err := client.Fetch(ctx, grpc.WaitForReady(true))
		s.Nil(err)

		s.Nil(stream.Send(&ubroker.FetchRequest{}))

		result, err := stream.Recv()
		s.Nil(err)

		s.Equal("hello", string(result.Message.Body))

		s.Nil(stream.CloseSend())

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestFetchShouldReturnTwoItems() {
	broker := &mockBroker{}
	broker.On("Delivery", mock.Anything).Once().Return(s.makeChannel("hello", "salam"), nil)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		stream, err := client.Fetch(ctx, grpc.WaitForReady(true))
		s.Nil(err)

		s.Nil(stream.Send(&ubroker.FetchRequest{}))
		result, err := stream.Recv()
		s.Nil(err)
		s.Equal("hello", string(result.Message.Body))

		s.Nil(stream.Send(&ubroker.FetchRequest{}))
		result, err = stream.Recv()
		s.Nil(err)
		s.Equal("salam", string(result.Message.Body))

		s.Nil(stream.CloseSend())

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestFetchShouldNotStreamIfNotRequestedForFirstData() {
	broker := &mockBroker{}
	broker.On("Delivery", mock.Anything).Once().Return(s.makeChannel("salam"), nil)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		stream, err := client.Fetch(ctx, grpc.WaitForReady(true))
		s.Nil(err)

		dataReceived := make(chan struct{})
		go func() {
			defer close(dataReceived)

			result, err := stream.Recv()
			s.Nil(err)
			s.Equal("salam", string(result.Message.Body))
		}()

		time.Sleep(100 * time.Millisecond)
		select {
		case <-dataReceived:
			s.FailNow("Fetch should not delivery if not requested")
			return

		default:
		}

		s.Nil(stream.Send(&ubroker.FetchRequest{}))

		<-dataReceived

		s.Nil(stream.CloseSend())

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestFetchShouldNotStreamIfNotRequestedForMoreData() {
	broker := &mockBroker{}
	broker.On("Delivery", mock.Anything).Once().Return(s.makeChannel("hello", "salam"), nil)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		stream, err := client.Fetch(ctx, grpc.WaitForReady(true))
		s.Nil(err)

		s.Nil(stream.Send(&ubroker.FetchRequest{}))
		result, err := stream.Recv()
		s.Nil(err)
		s.Equal("hello", string(result.Message.Body))

		dataReceived := make(chan struct{})
		go func() {
			defer close(dataReceived)

			result, err = stream.Recv()
			s.Nil(err)
			s.Equal("salam", string(result.Message.Body))
		}()

		time.Sleep(100 * time.Millisecond)
		select {
		case <-dataReceived:
			s.FailNow("Fetch should not delivery if not requested")
			return

		default:
		}

		s.Nil(stream.Send(&ubroker.FetchRequest{}))

		<-dataReceived

		s.Nil(stream.CloseSend())

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestAcknowledgeShouldReturnUnavailableIfClosed() {
	broker := &mockBroker{}
	broker.On("Acknowledge", mock.Anything, int32(10)).Once().Return(ubroker.ErrClosed)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		_, err := client.Acknowledge(ctx, &ubroker.AcknowledgeRequest{
			Id: 10,
		}, grpc.WaitForReady(true))
		s.assertStatusCode(codes.Unavailable, err)

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestAcknowledgeShouldReturnUnavailableIfInvalidID() {
	broker := &mockBroker{}
	broker.On("Acknowledge", mock.Anything, int32(10)).Once().Return(ubroker.ErrInvalidID)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		_, err := client.Acknowledge(ctx, &ubroker.AcknowledgeRequest{
			Id: 10,
		}, grpc.WaitForReady(true))
		s.assertStatusCode(codes.InvalidArgument, err)

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestAcknowledgeShouldReturnOKOnSuccess() {
	broker := &mockBroker{}
	broker.On("Acknowledge", mock.Anything, int32(10)).Once().Return(nil)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		_, err := client.Acknowledge(ctx, &ubroker.AcknowledgeRequest{
			Id: 10,
		}, grpc.WaitForReady(true))
		s.Nil(err)

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestReQueueShouldReturnUnavailableIfClosed() {
	broker := &mockBroker{}
	broker.On("ReQueue", mock.Anything, int32(10)).Once().Return(ubroker.ErrClosed)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		_, err := client.ReQueue(ctx, &ubroker.ReQueueRequest{
			Id: 10,
		}, grpc.WaitForReady(true))
		s.assertStatusCode(codes.Unavailable, err)

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestReQueueShouldReturnUnavailableIfInvalidID() {
	broker := &mockBroker{}
	broker.On("ReQueue", mock.Anything, int32(10)).Once().Return(ubroker.ErrInvalidID)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		_, err := client.ReQueue(ctx, &ubroker.ReQueueRequest{
			Id: 10,
		}, grpc.WaitForReady(true))
		s.assertStatusCode(codes.InvalidArgument, err)

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestReQueueShouldReturnOKOnSuccess() {
	broker := &mockBroker{}
	broker.On("ReQueue", mock.Anything, int32(10)).Once().Return(nil)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		_, err := client.ReQueue(ctx, &ubroker.ReQueueRequest{
			Id: 10,
		}, grpc.WaitForReady(true))
		s.Nil(err)

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestPublishShouldReturnUnavailableIfClosed() {
	broker := &mockBroker{}
	broker.On("Publish", mock.Anything, mock.MatchedBy(func(msg *ubroker.Message) bool {
		s.Equal("hello", string(msg.Body))
		return "hello" == string(msg.Body)
	})).Once().Return(ubroker.ErrClosed)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		_, err := client.Publish(ctx, &ubroker.Message{
			Body: []byte("hello"),
		}, grpc.WaitForReady(true))
		s.assertStatusCode(codes.Unavailable, err)

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) TestPublishShouldReturnOKIfSuccessful() {
	broker := &mockBroker{}
	broker.On("Publish", mock.Anything, mock.MatchedBy(func(msg *ubroker.Message) bool {
		s.Equal("hello", string(msg.Body))
		return "hello" == string(msg.Body)
	})).Once().Return(nil)

	s.runTest(func(ctx context.Context, client ubroker.BrokerClient) {
		_, err := client.Publish(ctx, &ubroker.Message{
			Body: []byte("hello"),
		}, grpc.WaitForReady(true))
		s.Nil(err)

		broker.AssertExpectations(s.T())
	}, broker)
}

func (s *GRPCServerTestSuite) makeChannel(args ...string) <-chan *ubroker.Delivery {
	result := make(chan *ubroker.Delivery, len(args))
	var id int32

	for _, arg := range args {
		result <- &ubroker.Delivery{
			Id: id,
			Message: &ubroker.Message{
				Body: []byte(arg),
			},
		}

		id = id + 1
	}

	close(result)
	return result
}

func (s *GRPCServerTestSuite) assertStatusCode(code codes.Code, err error) {
	if code == codes.OK {
		s.Nil(err)
		return
	}

	grpcStatus, ok := status.FromError(err)
	s.True(ok)

	if grpcStatus != nil {
		s.Equal(code, grpcStatus.Code())
	}
}

func (s *GRPCServerTestSuite) runTest(
	tester func(ctx context.Context, client ubroker.BrokerClient),
	broker ubroker.Broker) {

	port, err := freeport.GetFreePort()
	if err != nil {
		s.FailNow(err.Error(), "failed to obtain free port")
	}

	endpoint := fmt.Sprintf("127.0.0.1:%d", port)
	servicer := server.NewGRPC(broker)

	grpcServer := grpc.NewServer()
	ubroker.RegisterBrokerServer(grpcServer, servicer)

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		s.FailNow(err.Error(), "failed to open listener")
	}

	go func() {
		grpcServer.Serve(listener)
	}()

	dialCtx, dialCtxCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCtxCancel()

	conn, err := grpc.DialContext(dialCtx, endpoint,
		grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		s.FailNow(err.Error(), "failed to dial to gRPC‌ server")
	}

	client := ubroker.NewBrokerClient(conn)

	clientCtx, clientCtxCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer clientCtxCancel()

	tester(clientCtx, client)

	err = conn.Close()
	if err != nil {
		s.FailNow(err.Error(), "failed to properly close gRPC‌ client connection")
	}

	grpcServer.GracefulStop()
}
