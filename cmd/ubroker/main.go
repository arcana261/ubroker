package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/arcana261/ubroker/pkg/ubroker"
	"google.golang.org/grpc"

	"github.com/arcana261/ubroker/internal/broker"
	"github.com/arcana261/ubroker/internal/server"
)

func main() {
	portPtr := flag.Int("port", 8080, "port to listen to")
	ttlPtr := flag.Int("ttl", 20000, "time in milliseconds to expire unacknowledged")

	flag.Parse()

	broker := broker.New(time.Duration(*ttlPtr) * time.Millisecond)
	endpoint := fmt.Sprintf(":%d", *portPtr)
	servicer := server.NewGRPC(broker)

	grpcServer := grpc.NewServer()
	ubroker.RegisterBrokerServer(grpcServer, servicer)

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan struct{})
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		fmt.Printf("\nReceived an interrupt, stopping services...\n\n")
		close(cleanupDone)
	}()

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	fmt.Printf("listening on %s\n", endpoint)
	<-cleanupDone

	grpcServer.GracefulStop()

	if err := broker.Close(); err != nil {
		panic(err.Error())
	}
}
