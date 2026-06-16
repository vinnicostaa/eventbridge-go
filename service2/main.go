package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName               = "SERVICE_EVENTS"
	orderCreatedEventSubject = "service2.orders.created"
	workerQueueGroup         = "order-workers"
	workerDurableName        = "service2-order-worker"
	workerDeliverSubject     = "deliver.service2.orders.created"
	service3MessageSubject   = "service3.messages"
)

var streamSubjects = []string{
	orderCreatedEventSubject,
	service3MessageSubject,
}

type orderCreatedEvent struct {
	ID        string    `json:"id"`
	Customer  string    `json:"customer"`
	Product   string    `json:"product"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	natsURL := getenv("NATS_URL", nats.DefaultURL)
	nc, err := connectToNATS(natsURL, "service2-order-worker")
	if err != nil {
		log.Fatal(err)
	}
	defer drainNATS(nc)

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ensureStream(ctx, js); err != nil {
		log.Fatal(err)
	}

	consumer, err := js.CreateOrUpdatePushConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:        workerDurableName,
		FilterSubject:  orderCreatedEventSubject,
		DeliverSubject: workerDeliverSubject,
		DeliverGroup:   workerQueueGroup,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		MaxDeliver:     5,
		IdleHeartbeat:  30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	consumeCtx, err := consumer.Consume(handleOrderCreatedEvent)
	if err != nil {
		log.Fatal(err)
	}
	defer consumeCtx.Stop()

	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		log.Fatal(err)
	}

	log.Printf("service2 waiting for JetStream events on %q with durable %q and queue group %q", orderCreatedEventSubject, workerDurableName, workerQueueGroup)
	waitForShutdown()
}

func handleOrderCreatedEvent(msg jetstream.Msg) {
	var event orderCreatedEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		log.Printf("invalid event on %q: %v; payload=%s", msg.Subject(), err, string(msg.Data()))
		ackMessage(msg)
		return
	}

	log.Printf(
		"event received: order_created id=%s customer=%q product=%q quantity=%d created_at=%s",
		event.ID,
		event.Customer,
		event.Product,
		event.Quantity,
		event.CreatedAt.Format(time.RFC3339),
	)

	ackMessage(msg)
}

func ackMessage(msg jetstream.Msg) {
	if err := msg.Ack(); err != nil {
		log.Printf("failed to ack message on %q: %v", msg.Subject(), err)
	}
}

func ensureStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        streamName,
		Description: "Events and messages from the Gin + NATS example",
		Subjects:    streamSubjects,
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		MaxAge:      24 * time.Hour,
	})
	return err
}

func connectToNATS(url, name string) (*nats.Conn, error) {
	var lastErr error

	for attempt := 1; attempt <= 10; attempt++ {
		nc, err := nats.Connect(url, nats.Name(name))
		if err == nil {
			return nc, nil
		}

		lastErr = err
		log.Printf("NATS unavailable at %s, retrying (%d/10): %v", url, attempt, err)
		time.Sleep(time.Second)
	}

	return nil, fmt.Errorf("could not connect to NATS at %s: %w", url, lastErr)
}

func drainNATS(nc *nats.Conn) {
	if err := nc.Drain(); err != nil {
		log.Printf("failed to drain NATS connection: %v", err)
	}
}

func waitForShutdown() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("service2 shutting down")
}

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
