package server

import (
	"context"
	"io"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/arcana261/ubroker/pkg/ubroker"
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

	ctx := stream.Context();
	delChan, err := s.broker.Delivery(ctx)

	if err != nil {
		if err == ubroker.ErrClosed {
			return status.Error(codes.Unavailable, "Unavailable")
		}
		return nil;
	}

	for {
		_, recErr := stream.Recv()
		if recErr == io.EOF {
			return nil
		}
		if recErr != nil {
			return nil;
		}

		message := <- delChan
		if message == nil {
			return status.Error(codes.Unavailable, "Unavailable")
		}

		sendErr := stream.Send(message)
		if sendErr != nil {
			return nil
		}

	}
}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {

	err := s.broker.Acknowledge(ctx, request.Id)

	if err != nil {
		if err == ubroker.ErrClosed {
			return &empty.Empty{}, status.Error(codes.Unavailable, "Unavailable")
		}
		if err == ubroker.ErrInvalidID {
			return &empty.Empty{}, status.Error(codes.InvalidArgument, "InvalidID")
		}
		return &empty.Empty{}, nil;
	}
	return &empty.Empty{}, status.Error(codes.OK, "OK")
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	err := s.broker.ReQueue(ctx, request.Id)

	if err != nil {
		if err == ubroker.ErrClosed {
			return &empty.Empty{}, status.Error(codes.Unavailable, "Unavailable")
		}
		if err == ubroker.ErrInvalidID {
			return &empty.Empty{}, status.Error(codes.InvalidArgument, "InvalidID")
		}
		return &empty.Empty{}, nil;
	}
	return &empty.Empty{}, status.Error(codes.OK, "OK")
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	err := s.broker.Publish(ctx, request)

	if err != nil {
		if err == ubroker.ErrClosed {
			return &empty.Empty{}, status.Error(codes.Unavailable, "Unavailable")
		}
		if err == ubroker.ErrInvalidID {
			return &empty.Empty{}, status.Error(codes.InvalidArgument, "InvalidID")
		}
		return &empty.Empty{}, nil;
	}
	return &empty.Empty{}, status.Error(codes.OK, "OK")
}
