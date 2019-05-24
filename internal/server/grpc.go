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
	deliveryChannel, err := s.broker.Delivery(context.Background())
	if err != nil{
		return status.Error(codes.Unavailable, "unavailable")
	}

	for {
		if _, err := stream.Recv(); err != nil{
			return status.Error(codes.Unavailable, "not requested")
		}
		if msg, ok := <- deliveryChannel; ok {
			stream.Send(msg)
		} else {
			return status.Error(codes.Unavailable, "is closed")
		}
	}

}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	err := s.broker.Acknowledge(ctx, request.Id)
	if err == nil {
		return &empty.Empty{}, status.Error(codes.OK, "ok")
	}
	switch err.Error() {
		case "closed":
			return &empty.Empty{},status.Error(codes.Unavailable, "unavailable")
		case "id is invalid":
			return &empty.Empty{},status.Error(codes.InvalidArgument, "invalid arg")
		default:
			return &empty.Empty{}, err
	}
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	err := s.broker.ReQueue(ctx, request.Id)
	if err != nil{
		switch err.Error() {
			case "closed":
				return &empty.Empty{},status.Error(codes.Unavailable, "unavailable")
			case "id is invalid":
				return &empty.Empty{},status.Error(codes.InvalidArgument, "invalid arg")
			default:
				return &empty.Empty{}, err
		}
	}
	return &empty.Empty{}, status.Error(codes.OK, "ok")
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	err := s.broker.Publish(ctx, request)
	if err != nil{
		switch err.Error() {
			case "closed":
				return &empty.Empty{},status.Error(codes.Unavailable, "unavailable")
			default:
				return &empty.Empty{}, err
		}
	}
	return &empty.Empty{}, status.Error(codes.OK, "ok")
}
