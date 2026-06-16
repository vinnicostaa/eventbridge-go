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
	service3MessageSubject   = "service3.messages"
	messageDurableName       = "service3-message-consumer"
	messageDeliverSubject    = "deliver.service3.messages"
)

var streamSubjects = []string{
	orderCreatedEventSubject,
	service3MessageSubject,
}

type service3Message struct {
	ID      string    `json:"id"`
	OrderID string    `json:"order_id"`
	Text    string    `json:"text"`
	SentAt  time.Time `json:"sent_at"`
}

func main() {
	natsURL := getenv("NATS_URL", nats.DefaultURL)
	nc, err := connectToNATS(natsURL, "service3-message-consumer")
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
		Durable:        messageDurableName,
		FilterSubject:  service3MessageSubject,
		DeliverSubject: messageDeliverSubject,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		MaxDeliver:     5,
		IdleHeartbeat:  30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	consumeCtx, err := consumer.Consume(handleService3Message)
	if err != nil {
		log.Fatal(err)
	}
	defer consumeCtx.Stop()

	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		log.Fatal(err)
	}

	log.Printf("service3 waiting for JetStream messages on %q with durable %q", service3MessageSubject, messageDurableName)
	waitForShutdown()
}

func handleService3Message(msg jetstream.Msg) {
	var message service3Message
	if err := json.Unmarshal(msg.Data(), &message); err != nil {
		log.Printf("invalid message on %q: %v; payload=%s", msg.Subject(), err, string(msg.Data()))
		ackMessage(msg)
		return
	}

	log.Printf(
		"message received: id=%s order_id=%s text=%q sent_at=%s",
		message.ID,
		message.OrderID,
		message.Text,
		message.SentAt.Format(time.RFC3339),
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
	log.Println("service3 shutting down")
}

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
