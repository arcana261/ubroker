package server_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/amirmh/ubroker/internal/server"
	"github.com/amirmh/ubroker/pkg/ubroker"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type mockBroker struct {
	mock.Mock
}

func (m *mockBroker) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockBroker) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	args := m.Called(ctx)
	return args.Get(0).(<-chan ubroker.Delivery), args.Error(1)
}

func (m *mockBroker) Acknowledge(ctx context.Context, id int) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *mockBroker) ReQueue(ctx context.Context, id int) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *mockBroker) Publish(ctx context.Context, message ubroker.Message) error {
	args := m.Called(ctx, message)
	return args.Error(0)
}

type HTTPServerTestSuite struct {
	suite.Suite
	t      *testing.T
	broker *mockBroker
	server ubroker.HTTPServer
}

func TestHTTPServerTestSuite(t *testing.T) {
	suite.Run(t, &HTTPServerTestSuite{t: t})
}

func (s *HTTPServerTestSuite) prepareTest() {
	s.broker = new(mockBroker)
	s.broker.On("Delivery", mock.Anything).Return(make(<-chan ubroker.Delivery, 0), nil)
	s.server = server.NewHTTP(s.broker, ":0")
	s.server.Run()
}

func (s *HTTPServerTestSuite) TestEmptyFetch() {
	s.broker = new(mockBroker)
	s.broker.On("Delivery", mock.Anything).Return(make(<-chan ubroker.Delivery, 0), nil)
	s.server = server.NewHTTP(s.broker, ":0")
	s.server.Run()

	s.assertHTTPFetch(`{"error": "context deadline exceeded"}`, 408)
}

func (s *HTTPServerTestSuite) TestPublish() {
	s.prepareTest()
	s.broker.On("Publish", mock.Anything, mock.Anything).Return(nil)
	s.httpPublish(`{"body": "hello"}`)
	s.broker.AssertCalled(
		s.t,
		"Publish",
		mock.Anything,
		ubroker.Message{Body: "hello"},
	)
}

func (s *HTTPServerTestSuite) TestFailedReQueue() {
	s.prepareTest()
	s.broker.On("ReQueue", mock.Anything, 123).Return(ubroker.ErrInvalidID)
	body := `{"error": "id is invalid"}`
	s.httpReQueue(123, 400, &body)
	s.broker.AssertCalled(
		s.t,
		"ReQueue",
		mock.Anything,
		123,
	)
}

func (s *HTTPServerTestSuite) TestReQueue() {
	s.prepareTest()
	s.broker.On("ReQueue", mock.Anything, 123).Return(nil)
	s.httpReQueue(123, 200, nil)
	s.broker.AssertCalled(
		s.t,
		"ReQueue",
		mock.Anything,
		123,
	)
}

func (s *HTTPServerTestSuite) TestFailedAcknowledge() {
	s.prepareTest()
	s.broker.On("Acknowledge", mock.Anything, 123).Return(ubroker.ErrInvalidID)
	body := `{"error": "id is invalid"}`
	s.httpAcknowledge(123, 400, &body)
	s.broker.AssertCalled(
		s.t,
		"Acknowledge",
		mock.Anything,
		123,
	)
}

func (s *HTTPServerTestSuite) TestAcknowledge() {
	s.prepareTest()
	s.broker.On("Acknowledge", mock.Anything, 123).Return(nil)
	s.httpAcknowledge(123, 200, nil)
	s.broker.AssertCalled(
		s.t,
		"Acknowledge",
		mock.Anything,
		123,
	)
}

func (s *HTTPServerTestSuite) httpPublish(message string) {
	resp := httptest.NewRecorder()

	req, err := http.NewRequest(
		"POST",
		"/publish",
		strings.NewReader(message),
	)
	if err != nil {
		s.Fail(err.Error())
	}

	s.server.ServeHTTP(resp, req)

	if _, err := ioutil.ReadAll(resp.Body); err != nil {
		s.Fail(err.Error())
	} else {
		s.Equal(200, resp.Code)
	}
}

func (s *HTTPServerTestSuite) httpReQueue(id int, rspCode int, rspBody *string) {
	resp := httptest.NewRecorder()

	req, err := http.NewRequest("POST", fmt.Sprintf("/requeue/%v", id), nil)
	if err != nil {
		s.Fail(err.Error())
	}

	s.server.ServeHTTP(resp, req)

	if p, err := ioutil.ReadAll(resp.Body); err != nil {
		s.Fail(err.Error())
	} else {
		s.Equal(rspCode, resp.Code)
		if rspBody != nil {
			s.JSONEq(*rspBody, string(p))
		}
	}
}

func (s *HTTPServerTestSuite) httpAcknowledge(id int, rspCode int, rspBody *string) {
	resp := httptest.NewRecorder()

	req, err := http.NewRequest("POST", fmt.Sprintf("/acknowledge/%v", id), nil)
	if err != nil {
		s.Fail(err.Error())
	}

	s.server.ServeHTTP(resp, req)

	if p, err := ioutil.ReadAll(resp.Body); err != nil {
		s.Fail(err.Error())
	} else {
		s.Equal(rspCode, resp.Code)
		if rspBody != nil {
			s.JSONEq(*rspBody, string(p))
		}
	}
}

func (s *HTTPServerTestSuite) assertHTTPFetch(body string, code int) {
	resp := httptest.NewRecorder()

	req, err := http.NewRequest("GET", "/fetch", nil)
	if err != nil {
		s.Fail(err.Error())
	}

	s.server.ServeHTTP(resp, req)

	if p, err := ioutil.ReadAll(resp.Body); err != nil {
		s.Fail(err.Error())
	} else {
		s.Equal(code, resp.Code)
		s.JSONEq(body, string(p))
	}
}
