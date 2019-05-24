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
	for {
		_, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return status.Error(codes.Unknown, err.Error())
		}
		deliveryChan, err := s.broker.Delivery(stream.Context())
		stat := getStatusErrFromBrokerErr(err)
		if stat != nil {
			return stat
		}
		msg, ok := <-deliveryChan
		if !ok {
			return status.Error(codes.Unavailable, "Delivery channel closed")
		}
		stream.Send(msg)
	}
}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	requestId := request.GetId()
	err := s.broker.Acknowledge(ctx, requestId)
	return &empty.Empty{}, getStatusErrFromBrokerErr(err)
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	requestId := request.GetId()
	err := s.broker.ReQueue(ctx, requestId)
	return &empty.Empty{}, getStatusErrFromBrokerErr(err)
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	err := s.broker.Publish(ctx, request)
	return &empty.Empty{}, getStatusErrFromBrokerErr(err)
}

func getStatusErrFromBrokerErr(err error) error {
	if err != nil {
		if err == ubroker.ErrClosed {
			return status.Error(codes.Unavailable, err.Error())
		} else if err == ubroker.ErrInvalidID {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		return status.Error(codes.Unknown, err.Error())
	}
	return nil
}
