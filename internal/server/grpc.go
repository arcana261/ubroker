package server

import (
	"context"
	"io"

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
	delivery, err := s.broker.Delivery(stream.Context())
	if err != nil {
		if err == ubroker.ErrClosed {
			return status.Error(codes.Unavailable, "Unavailable")
		}
		if err == ubroker.ErrInvalidID {
			return status.Error(codes.InvalidArgument, "InvalidID")
		}
		return nil
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			println("heeeeeeeeeeeeeeeeeeeeeer!")
			if err == ubroker.ErrClosed {
				return status.Error(codes.Unavailable, "Unavailable")
			}
			if err == ubroker.ErrInvalidID {
				return status.Error(codes.InvalidArgument, "InvalidID")
			}
			return nil
		}
		delivered := <-delivery
		if delivered == nil {
			return status.Error(codes.Unavailable, "Unavailable")
		}
		err = stream.Send(delivered)
		if err != nil {
			if err == ubroker.ErrClosed {
				return status.Error(codes.Unavailable, "Unavailable")
			}
			if err == ubroker.ErrInvalidID {
				return status.Error(codes.InvalidArgument, "InvalidID")
			}
			return nil
		}
	}
	//return status.Error(codes.OK, "OK")
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
		return &empty.Empty{}, nil
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
		return &empty.Empty{}, nil
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
		return &empty.Empty{}, nil
	}
	return &empty.Empty{}, status.Error(codes.OK, "OK")
}
