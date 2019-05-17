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
	broker   ubroker.Broker
	delivery <-chan *ubroker.Delivery
}

func NewGRPC(broker ubroker.Broker) ubroker.BrokerServer {
	return &grpcServicer{
		broker: broker,
	}
}

func (s *grpcServicer) Fetch(stream ubroker.Broker_FetchServer) error {
	var err error
	ctx := stream.Context()

	select {
	case <-ctx.Done():
		return s.handleError(ctx.Err())
	default:
		s.delivery, err = s.broker.Delivery(ctx)
		if err != nil {
			return s.handleError(err)
		}
	}

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			continue
		}

		select {
		case <-ctx.Done():
			return s.handleError(ctx.Err())
		case msg := <-s.delivery:
			if msg == nil {
				return status.Error(codes.Unavailable, "Empty Queue")
			}
			if err := stream.Send(msg); err != nil {
				return s.handleError(err)
			}
		}
	}
}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	err := s.broker.Acknowledge(ctx, request.Id)
	return &empty.Empty{}, s.handleError(err)
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	err := s.broker.ReQueue(ctx, request.Id)
	return &empty.Empty{}, s.handleError(err)
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	err := s.broker.Publish(ctx, request)
	return &empty.Empty{}, s.handleError(err)
}

func (s *grpcServicer) handleError(err error) error {
	switch err {
	case nil:
		return status.Error(codes.OK, "OK")

	case ubroker.ErrClosed:
		return status.Error(codes.Unavailable, "Unavailble")

	case ubroker.ErrUnimplemented:
		return status.Error(codes.Unimplemented, "Unimplemented")

	case ubroker.ErrInvalidID, errInvalidArgument:
		return status.Error(codes.InvalidArgument, "Invalid Argument")

	case context.Canceled, context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, "Deadline Exceeded")

	default:
		return status.Error(codes.Internal, "Internal Error")
	}
}
