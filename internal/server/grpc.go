package server

import (
	"context"
	// "fmt"

	"github.com/maedeazad/ubroker/pkg/ubroker"
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

// Unavailable: status.Error(codes.Unavailable, "Unavailable")
// InvalidArgument: status.Error(codes.InvalidArgument, "InvalidArgument")

// Publish message to Queue
//     OK: on success
//     Unavailable: If broker has been closed
func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	err := s.broker.Publish(ctx, request)

	if err != nil {
		return &empty.Empty{}, status.Error(codes.Unavailable, "Unavailable")
	}

	return &empty.Empty{}, nil
}

// Acknowledge a message
// Should return:
//     OK: on success
//     Unavailable: If broker has been closed
//     InvalidArgument: If requested ID is invalid
func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	err := s.broker.Acknowledge(ctx, request.Id)

	if err != nil {
		if err == ubroker.ErrClosed{
			return &empty.Empty{}, status.Error(codes.Unavailable, "Unavailable")
		}else{
			return &empty.Empty{}, status.Error(codes.InvalidArgument, "InvalidArgument")
		}
	}

	return &empty.Empty{}, nil
}

// ReQueue a message
//     OK: on success
//     Unavailable: If broker has been closed
//     InvalidArgument: If requested ID is invalid
func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	err := s.broker.ReQueue(ctx, request.Id)

	if err != nil {
		if err == ubroker.ErrClosed{
			return &empty.Empty{}, status.Error(codes.Unavailable, "Unavailable")
		}else{
			return &empty.Empty{}, status.Error(codes.InvalidArgument, "InvalidArgument")
		}
	}

	return &empty.Empty{}, nil
}

// Fetch should return a single Delivery per FetchRequest.
// Should return:
//     Unavailable: If broker has been closed
func (s *grpcServicer) Fetch(stream ubroker.Broker_FetchServer) error {
	ctx := stream.Context()

	deliveryChannel, err := s.broker.Delivery(ctx)
	if err != nil {
		return err
	}

	for {
		_, err = stream.Recv()
		if err != nil{
			return status.Error(codes.Unavailable, "Unavailable")
		}

		select {
		case delivery := <-deliveryChannel:
			if delivery != nil{
				return status.Error(codes.Unavailable, "Unavailable")
			}else{
				stream.Send(delivery)
			}

		case <-ctx.Done():
			return status.Error(codes.Unavailable, "Unavailable")
		}
	}
}

// func print(s string){
// 	fmt.Println(s)
// }
