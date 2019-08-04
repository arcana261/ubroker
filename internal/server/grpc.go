package server

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MohammadHossein/ubroker/pkg/ubroker"
	"github.com/golang/protobuf/ptypes/empty"
)

type grpcServicer struct {
	broker ubroker.Broker
}

func toServiceError(err error) error {
	switch err {
	case nil:
		return nil
	case ubroker.ErrClosed:
		return status.Error(codes.Unavailable, "ErrClosed")
	case ubroker.ErrInvalidID:
		return status.Error(codes.InvalidArgument, "ErrInvalidID")
	default:
		return status.Error(codes.Unknown, "Unknown!")
	}
}
func NewGRPC(broker ubroker.Broker) ubroker.BrokerServer {
	return &grpcServicer{
		broker: broker,
	}
}

func (s *grpcServicer) Fetch(stream ubroker.Broker_FetchServer) error {
	deliveryChannel, err := s.broker.Delivery(stream.Context())
	if err != nil {
		return toServiceError(err)
	}

	for ; ; {
		_, err := stream.Recv()
		if err != nil {
			return toServiceError(err)
		}

		msg, ok := <-deliveryChannel;
		if ok {
			err := stream.Send(msg)
			if err != nil {
				return toServiceError(err)
			}
			return nil
		}
		return toServiceError(ubroker.ErrClosed)
	}
}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	return &empty.Empty{}, toServiceError(s.broker.Acknowledge(ctx,request.GetId()))
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	return &empty.Empty{}, toServiceError(s.broker.ReQueue(ctx,request.GetId()))
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	return &empty.Empty{}, toServiceError(s.broker.Publish(ctx,request.GetId()))
}
