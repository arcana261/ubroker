package server

import (
	"context"
	"github.com/AMIRmh/ubroker/pkg/ubroker"
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
	ctx := stream.Context()
	delivery, err := s.broker.Delivery(ctx)
	if err != nil {
		return whyError(err)
	}

	for {
		_, err := stream.Recv()
		if err != nil {
			return whyError(err)
		}

		if msg, ok := <-delivery; ok {
			err := stream.Send(msg)
			if err != nil {
				return whyError(err)
			}
		} else {
			return whyError(ubroker.ErrClosed)
		}
	}


}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	err := s.broker.Acknowledge(ctx, request.GetId())
	return &empty.Empty{}, whyError(err)

}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	err := s.broker.ReQueue(ctx, request.GetId())
	return &empty.Empty{}, whyError(err)
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	err := s.broker.Publish(ctx, request)
	return &empty.Empty{}, whyError(err)
}


func whyError(err error) error {
	switch err {
	case nil:
		return nil
	case ubroker.ErrClosed:
		return status.Error(codes.Unavailable, "Unavailable")
	case ubroker.ErrInvalidID:
		return status.Error(codes.InvalidArgument, "InvalidArgument")
	default:
		return status.Error(codes.Unknown, "Unknown!")
	}
}