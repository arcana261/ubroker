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
	return status.Error(codes.Unimplemented, "not implemented")
}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	return &empty.Empty{}, status.Error(codes.Unimplemented, "not implemented")
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	return &empty.Empty{}, status.Error(codes.Unimplemented, "not implemented")
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	return &empty.Empty{}, status.Error(codes.Unimplemented, "not implemented")
}
