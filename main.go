package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

func main() {
	// 1. Connect to InfluxDB 3 (HTTP for writes, Flight/gRPC for queries - same port 8181)
	db, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     env("INFLUX_HOST", "http://localhost:8181"),
		Token:    os.Getenv("INFLUX_TOKEN"),
		Database: env("INFLUX_DATABASE", "vending"),
	})
	if err != nil {
		log.Fatalf("influx client: %v", err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Batch writer: 500 points or every 1 second
	writer := NewBatchWriter(db, 500, time.Second)
	writerCtx, stopWriter := context.WithCancel(context.Background())
	go writer.Run(writerCtx)

	// 3. Subscribe MQTT -> parse -> hand to writer
	mq, err := ConnectMQTT(env("MQTT_BROKER", "tcp://localhost:1883"), writer)
	if err != nil {
		log.Fatalf("mqtt: %v", err)
	}

	// 4. HTTP API for queries
	api := &API{db: db}
	srv := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: api.Routes()}
	go func() {
		log.Printf("HTTP API listen %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done() // Ctrl+C
	log.Println("shutting down...")
	mq.Disconnect(500) // 1) stop receiving new messages
	stopWriter()       // 2) tell the writer to stop
	writer.Wait()      // 3) wait for the last batch to flush
	srv.Shutdown(context.Background())
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
