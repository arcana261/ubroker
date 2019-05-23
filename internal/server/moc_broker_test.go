package server_test

import (
	"context"

	"github.com/arcana261/ubroker/pkg/ubroker"
	"github.com/stretchr/testify/mock"
)

type mockBroker struct {
	mock.Mock
}

func (m *mockBroker) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockBroker) Delivery(ctx context.Context) (<-chan *ubroker.Delivery, error) {
	args := m.Called(ctx)

	var res0 <-chan *ubroker.Delivery

	if args.Get(0) != nil {
		res0 = args.Get(0).(<-chan *ubroker.Delivery)
	}

	return res0, args.Error(1)
}

func (m *mockBroker) Acknowledge(ctx context.Context, id int32) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *mockBroker) ReQueue(ctx context.Context, id int32) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *mockBroker) Publish(ctx context.Context, message *ubroker.Message) error {
	args := m.Called(ctx, message)
	return args.Error(0)
}
