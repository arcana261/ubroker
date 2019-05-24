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
	//return status.Error(codes.Unimplemented, "not implemented")
	deliveryChannel, err := s.broker.Delivery(context.Background())
	if err != nil {
		return status.Error(codes.Unavailable, "service is unavailable")
	}

	for {
		if _, err := stream.Recv(); err != nil {
			return Error(err)
		}
		if msg, ok := <-deliveryChannel; ok {
			_ = stream.Send(msg)
		} else {
			return Error(err)
		}
	}
}
func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	//return &empty.Empty{}, status.Error(codes.Unimplemented, "not implemented")
	err := s.broker.Acknowledge(ctx, request.Id)
	if err == nil {
		return &empty.Empty{}, status.Error(codes.OK, "ok")
	}
	return &empty.Empty{}, Error(err)
}
func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	//return &empty.Empty{}, status.Error(codes.Unimplemented, "not implemented")
	err := s.broker.ReQueue(ctx, request.Id)
	if err != nil {
		return &empty.Empty{}, Error(err)

	}
	return &empty.Empty{}, status.Error(codes.OK, "ok")
}
func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	//return &empty.Empty{}, status.Error(codes.Unimplemented, "not implemented")
	err := s.broker.Publish(ctx, request)
	if err != nil {
		return &empty.Empty{}, Error(err)
	}
	return &empty.Empty{}, status.Error(codes.OK, "ok")
}
func Error(err error) error {
	if err.Error() == "closed" {
		return status.Error(codes.Unavailable, "service is unavailable")
	}
	if err.Error() == "id is invalid" {
		return status.Error(codes.InvalidArgument, "Argument is invalid")
	} else {
		return err
	}
}
