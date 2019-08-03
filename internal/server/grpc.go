package server

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/meshkati/ubroker/pkg/ubroker"
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
	deliveryChan, err := s.broker.Delivery(stream.Context())
	if err != nil {
		return convertError(err)
	}
	// it's stream, so the count of incoming messages is unknown
	for {
		_, err := stream.Recv()
		if err != nil {
			return convertError(err)
		}

		message := <- deliveryChan
		if message == nil {
			return convertError(ubroker.ErrClosed)
		}

		err = stream.Send(message)
		if err != nil {
			return convertError(err)
		}

	}
}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	err := s.broker.Acknowledge(ctx, request.GetId())
	serviceError := convertError(err)
	return &empty.Empty{}, serviceError
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	err := s.broker.ReQueue(ctx, request.GetId())
	serviceError := convertError(err)
	return &empty.Empty{}, serviceError
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	err := s.broker.Publish(ctx, request)
	serviceError := convertError(err)
	return &empty.Empty{}, serviceError
}


func convertError(err error) error {
	switch err {
	case ubroker.ErrClosed:
		return status.Error(codes.Unavailable, "Service unavailable")
	case ubroker.ErrInvalidID:
		return status.Error(codes.InvalidArgument, "Invalid argument")
	case ubroker.ErrUnimplemented:
		return status.Error(codes.Unimplemented, "Service unimplemented")
	default:
		if err == nil {
			return nil
		}
		return status.Error(codes.Unknown, "Unknown service error!")
	}
}