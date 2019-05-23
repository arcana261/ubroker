package server

import (
	"context"

	"github.com/arcana261/ubroker/pkg/ubroker"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcServicer struct {
	broker ubroker.Broker
}

func getError(msg error) error {
	switch msg {
	case nil:
		return nil
	case ubroker.ErrClosed:
		return status.Error(codes.Unavailable, "Broker is closed")
	default:
		return status.Error(codes.Unknown, "Unknown Error")
	}
}

func NewGRPC(broker ubroker.Broker) ubroker.BrokerServer {
	return &grpcServicer{
		broker: broker,
	}
}

func (s *grpcServicer) Fetch(stream ubroker.Broker_FetchServer) error {
	delivery, errMsg := s.broker.Delivery(context.Background())
	if errMsg != nil {
		return getError(errMsg)
	}

	for {
		_, streamError := stream.Recv()
		if streamError != nil {
			return getError(streamError)
		}

		delMsg, delSuccess := <-delivery
		if delSuccess {
			stream.Send(delMsg)
		} else {
			return getError(ubroker.ErrClosed)
		}
	}
}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	errMsg := s.broker.Acknowledge(ctx, request.GetId())
	return &empty.Empty{}, getError(errMsg)
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	errMsg := s.broker.ReQueue(ctx, request.GetId())
	return &empty.Empty{}, getError(errMsg)
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	errMsg := s.broker.Publish(ctx, request)
	return &empty.Empty{}, getError(errMsg)
}
