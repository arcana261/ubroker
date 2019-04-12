package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/AMIRmh/ubroker/internal/broker"
	"github.com/AMIRmh/ubroker/internal/server"
)

func main() {
	portPtr := flag.Int("port", 8080, "port to listen to")
	ttlPtr := flag.Int("ttl", 20000, "time in milliseconds to expire unacknowledged")

	flag.Parse()

	broker := broker.New(time.Duration(*ttlPtr) * time.Millisecond)
	endpoint := fmt.Sprintf(":%d", *portPtr)
	srv := server.NewHTTP(broker, endpoint)

	if err := srv.Run(); err != nil {
		panic(err.Error())
	}

	fmt.Printf("listening on %s\n", endpoint)

	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan struct{})
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		fmt.Printf("\nReceived an interrupt, stopping services...\n\n")
		close(cleanupDone)
	}()
	<-cleanupDone

	if err := srv.Close(); err != nil {
		panic(err.Error())
	}

	if err := broker.Close(); err != nil {
		panic(err.Error())
	}
}
