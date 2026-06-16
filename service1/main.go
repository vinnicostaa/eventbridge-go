package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName               = "SERVICE_EVENTS"
	orderCreatedEventSubject = "service2.orders.created"
	service3MessageSubject   = "service3.messages"
)

var streamSubjects = []string{
	orderCreatedEventSubject,
	service3MessageSubject,
}

type createOrderRequest struct {
	Customer string `json:"customer" binding:"required"`
	Product  string `json:"product" binding:"required"`
	Quantity int    `json:"quantity" binding:"required,min=1"`
}

type orderCreatedEvent struct {
	ID        string    `json:"id"`
	Customer  string    `json:"customer"`
	Product   string    `json:"product"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}

type service3Message struct {
	ID      string    `json:"id"`
	OrderID string    `json:"order_id"`
	Text    string    `json:"text"`
	SentAt  time.Time `json:"sent_at"`
}

func main() {
	natsURL := getenv("NATS_URL", nats.DefaultURL)
	nc, err := connectToNATS(natsURL, "service1-api")
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

	router := gin.Default()

	router.GET("/ping", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{"message": "pong"})
	})

	router.POST("/orders", func(ctx *gin.Context) {
		var req createOrderRequest
		if err := ctx.ShouldBindJSON(&req); err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		event := orderCreatedEvent{
			ID:        newID("ord"),
			Customer:  strings.TrimSpace(req.Customer),
			Product:   strings.TrimSpace(req.Product),
			Quantity:  req.Quantity,
			CreatedAt: time.Now().UTC(),
		}

		message := service3Message{
			ID:      newID("msg"),
			OrderID: event.ID,
			Text:    fmt.Sprintf("Novo pedido %s criado para %s", event.ID, event.Customer),
			SentAt:  time.Now().UTC(),
		}

		publishCtx, cancel := context.WithTimeout(ctx.Request.Context(), 5*time.Second)
		defer cancel()

		eventAck, err := publishJSON(publishCtx, js, orderCreatedEventSubject, event.ID, event)
		if err != nil {
			log.Printf("failed to publish event to service2: %v", err)
			ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to publish event to service2"})
			return
		}

		messageAck, err := publishJSON(publishCtx, js, service3MessageSubject, message.ID, message)
		if err != nil {
			log.Printf("failed to publish message to service3: %v", err)
			ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to publish message to service3"})
			return
		}

		ctx.JSON(http.StatusAccepted, gin.H{
			"status": "order accepted",
			"published": gin.H{
				"event_to_service2": gin.H{
					"subject":  orderCreatedEventSubject,
					"stream":   eventAck.Stream,
					"sequence": eventAck.Sequence,
				},
				"message_to_service3": gin.H{
					"subject":  service3MessageSubject,
					"stream":   messageAck.Stream,
					"sequence": messageAck.Sequence,
				},
			},
			"order_event":      event,
			"service3_message": message,
		})
	})

	addr := getenv("HTTP_ADDR", ":3000")
	log.Printf("service1 listening on %s and publishing to JetStream at %s", addr, natsURL)
	if err := router.Run(addr); err != nil {
		log.Fatal(err)
	}
}

func publishJSON(ctx context.Context, js jetstream.JetStream, subject, msgID string, value any) (*jetstream.PubAck, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	ack, err := js.Publish(ctx, subject, payload, jetstream.WithMsgID(msgID))
	if err != nil {
		return nil, fmt.Errorf("publish %s: %w", subject, err)
	}

	return ack, nil
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

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func newID(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}
