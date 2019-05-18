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

func NewGRPC(broker ubroker.Broker) ubroker.BrokerServer {
	return &grpcServicer{
		broker: broker,
	}
}

func (s *grpcServicer) Fetch(stream ubroker.Broker_FetchServer) error {
	delivery, err := s.broker.Delivery(context.Background())
	if err != nil {
		return toStatus(err)
	}

	for {
		if _, err := stream.Recv(); err != nil {
			return toStatus(err)
		}

		if msg, ok := <-delivery; ok {
			stream.Send(msg)
		} else {
			return toStatus(ubroker.ErrClosed)
		}
	}
}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	err := s.broker.Acknowledge(ctx, request.GetId())
	return &empty.Empty{}, toStatus(err)
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	err := s.broker.ReQueue(ctx, request.GetId())
	return &empty.Empty{}, toStatus(err)
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	err := s.broker.Publish(ctx, request)
	return &empty.Empty{}, toStatus(err)
}

func toStatus(err error) error {
	switch err {
	case nil:
		return nil
	case ubroker.ErrClosed:
		return status.Error(codes.Unavailable, "Unavailable")
	case ubroker.ErrInvalidID:
		return status.Error(codes.InvalidArgument, "Invalid argument")
	case ubroker.ErrUnimplemented:
		return status.Error(codes.Unimplemented, "Unimplemented")
	default:
		return status.Error(codes.Unknown, "Unknown (´∵｀)")
	}
}
