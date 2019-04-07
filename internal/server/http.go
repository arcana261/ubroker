package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"github.com/sirupsen/logrus"

	"github.com/erfan-mehraban/ubroker/pkg/ubroker"
	"github.com/pkg/errors"
)

var (
	trailingSlashRegex *regexp.Regexp
	errInvalidArgument = errors.New("invalid argument")
)

const (
	defaultTimeout = 1 * time.Second
)

func init() {
	trailingSlashRegex = regexp.MustCompile("/+$")
}

type httpServer struct {
	broker   ubroker.Broker
	router   *mux.Router
	delivery <-chan ubroker.Delivery
	endpoint string
	server   *http.Server
	mutex    sync.Mutex
}

type httpError struct {
	Error string `json:"error"`
}

type httpOK struct {
}

func NewHTTP(broker ubroker.Broker, endpoint string) ubroker.HTTPServer {
	result := &httpServer{
		broker:   broker,
		endpoint: endpoint,
	}

	router := mux.NewRouter()
	router.HandleFunc("/publish", result.handlePublish).Methods("POST")
	router.HandleFunc("/acknowledge/{id}", result.handleAcknowledge).Methods("POST")
	router.HandleFunc("/requeue/{id}", result.handleReQueue).Methods("POST")
	router.HandleFunc("/fetch", result.handleFetch).Methods("GET")

	result.router = router

	return result
}

func (s *httpServer) Run() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var err error

	if s.server != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	s.delivery, err = s.broker.Delivery(ctx)
	if err != nil {
		return err
	}

	s.server = &http.Server{
		Addr:    s.endpoint,
		Handler: s,
	}

	ready := make(chan struct{})

	go func() {
		close(ready)

		if err := s.server.ListenAndServe(); err != nil {
			logrus.WithError(err).Panic("failed to start http server")
		}
	}()

	<-ready

	return nil
}

func (s *httpServer) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.server == nil {
		return nil
	}

	if err := s.server.Shutdown(context.Background()); err != nil {
		return err
	}

	s.server = nil
	return nil
}

func (s *httpServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	s.router.ServeHTTP(writer, request)
}

func (s *httpServer) handlePublish(writer http.ResponseWriter, request *http.Request) {
	data, err := ioutil.ReadAll(request.Body)
	if err != nil {
		s.handleError(writer, request, err)
		return
	}

	var msg ubroker.Message
	err = json.Unmarshal(data, &msg)
	if err != nil {
		s.handleError(writer, request, err)
	}

	ctx, cancel := s.makeContext(request)
	defer cancel()

	err = s.broker.Publish(ctx, msg)
	if err != nil {
		s.handleError(writer, request, err)
	}

	s.handleOK(writer, request)
}

func (s *httpServer) handleAcknowledge(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	rawID, ok := vars["id"]
	if !ok {
		s.handleError(writer, request, errors.Wrap(errInvalidArgument, "<id> not provided"))
		return
	}

	id, err := strconv.Atoi(rawID)
	if err != nil {
		s.handleError(writer, request, errors.Wrapf(errInvalidArgument, "<id> should be int: %v", err.Error()))
		return
	}

	ctx, cancel := s.makeContext(request)
	defer cancel()

	err = s.broker.Acknowledge(ctx, id)
	if err != nil {
		s.handleError(writer, request, err)
		return
	}

	s.handleOK(writer, request)
}

func (s *httpServer) handleReQueue(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	rawID, ok := vars["id"]
	if !ok {
		s.handleError(writer, request, errors.Wrap(errInvalidArgument, "<id> not provided"))
		return
	}

	id, err := strconv.Atoi(rawID)
	if err != nil {
		s.handleError(writer, request, errors.Wrapf(errInvalidArgument, "<id> should be int: %v", err.Error()))
		return
	}

	ctx, cancel := s.makeContext(request)
	defer cancel()

	err = s.broker.ReQueue(ctx, id)
	if err != nil {
		s.handleError(writer, request, err)
		return
	}

	s.handleOK(writer, request)
}

func (s *httpServer) handleFetch(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := s.makeContext(request)
	defer cancel()

	select {
	case msg := <-s.delivery:
		s.writeJSON(writer, request, http.StatusOK, msg)

	case <-ctx.Done():
		s.handleError(writer, request, ctx.Err())
	}
}

func (s *httpServer) handleError(writer http.ResponseWriter,
	request *http.Request, err error) {

	s.writeJSON(writer, request, s.errorToStatusCode(err), httpError{Error: err.Error()})
}

func (s *httpServer) handleOK(writer http.ResponseWriter,
	request *http.Request) {

	s.writeJSON(writer, request, s.errorToStatusCode(nil), httpOK{})
}

func (s *httpServer) writeJSON(writer http.ResponseWriter,
	request *http.Request, statusCode int, value interface{}) {

	writer.Header()["content-type"] = []string{"application/json"}

	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		message := fmt.Sprintf(`{"error":"%s"}`, strings.Replace(err.Error(), "\"", "\\\"", -1))
		s.writeString(writer, request, message)
		return
	}

	writer.WriteHeader(statusCode)
	s.writeBytes(writer, request, data)
}

func (s *httpServer) writeString(writer io.Writer,
	request *http.Request, value string) {

	s.writeBytes(writer, request, []byte(value))
}

func (s *httpServer) writeBytes(writer io.Writer,
	request *http.Request, value []byte) {

	bytes := []byte(value)

	n, err := writer.Write(bytes)
	if err != nil {
		s.logErr(request, err, "could not write response %s", value)
		return
	}
	if n != len(bytes) {
		s.log(request, "could not write completely response %s", value)
	}
}

func (s *httpServer) logErr(request *http.Request, err error, format string, args ...interface{}) {
	logrus.WithError(err).Info(fmt.Sprintf("[%v] %s", request.RemoteAddr, fmt.Sprintf(format, args...)))
}

func (s *httpServer) log(request *http.Request, format string, args ...interface{}) {
	logrus.Info(fmt.Sprintf("[%v] %s", request.RemoteAddr, fmt.Sprintf(format, args...)))
}

func (s *httpServer) errorToStatusCode(err error) int {
	if err == nil {
		return http.StatusOK
	}

	result := http.StatusInternalServerError
	var ok bool

	for {
		result, ok = s.tryErrorToStatusCode(err)
		if ok {
			return result
		}

		cause := errors.Cause(err)
		if cause == err {
			return result
		}

		err = cause
	}
}

func (s *httpServer) makeContext(request *http.Request) (context.Context, context.CancelFunc) {
	ctx := context.WithValue(context.Background(), "remoteAddr", request.RemoteAddr)

	return context.WithTimeout(ctx, defaultTimeout)
}

func (s *httpServer) tryErrorToStatusCode(err error) (int, bool) {
	switch err {
	case ubroker.ErrClosed:
		return http.StatusInternalServerError, true

	case ubroker.ErrUnimplemented:
		return http.StatusNotImplemented, true

	case ubroker.ErrInvalidID, errInvalidArgument:
		return http.StatusBadRequest, true

	case context.Canceled, context.DeadlineExceeded:
		return http.StatusRequestTimeout, true

	default:
		return http.StatusInternalServerError, false
	}
}
