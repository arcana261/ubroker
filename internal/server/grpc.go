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

func mapErrorToStatus(err error) error {
	switch err {
	case io.EOF:
		return nil
	case nil:
		return status.Error(codes.OK, "Ok!")
	case ubroker.ErrClosed:
		return status.Error(codes.Unavailable, "Channel is closed!")
	case context.Canceled, context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, "Deadline Exceeded!")
	case ubroker.ErrUnimplemented:
		return status.Error(codes.Unimplemented, "Method is not implemented yet!")
	case ubroker.ErrInvalidID, errInvalidArgument:
		return status.Error(codes.InvalidArgument, "Argument (or id) is not valid!")
	default:
		return status.Error(codes.Unknown, "Unknown error occurred!")
	}
}

func (s *grpcServicer) Fetch(stream ubroker.Broker_FetchServer) error {

	deliveryChannel, err := s.broker.Delivery(stream.Context())
	if err != nil {
		return mapErrorToStatus(err)
	}

	for true {
		_, err = stream.Recv()
		if err != nil {
			return mapErrorToStatus(err)
		}
		if message, open := <-deliveryChannel; open {
			err := stream.Send(message)
			if err != nil {
				return mapErrorToStatus(err)
			}
		} else {
			return mapErrorToStatus(ubroker.ErrClosed)
		}
	}

	return nil

}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	return &empty.Empty{}, mapErrorToStatus(s.broker.Acknowledge(ctx, request.Id))
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	return &empty.Empty{}, mapErrorToStatus(s.broker.ReQueue(ctx, request.GetId()))
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	return &empty.Empty{}, mapErrorToStatus(s.broker.Publish(ctx, request))
}
