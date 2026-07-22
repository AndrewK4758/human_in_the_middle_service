package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

type Worker struct {
	nc *nats.Conn
	js jetstream.JetStream
}

func NewWorker() (*Worker, error) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &Worker{
		nc: nc,
		js: js,
	}, nil
}

func (w *Worker) Start(ctx context.Context) error {
	streamName := "WORKERS"
	_, err := w.js.Stream(ctx, streamName)
	if err != nil {
		_, err = w.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{"worker.>"},
		})
		if err != nil {
			return fmt.Errorf("failed to create JetStream stream: %w", err)
		}
		log.Printf("Successfully created JetStream stream: %s", streamName)
	}

	cons, err := w.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "human_validation_worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "worker.human.validation",
		MaxAckPending: 100,
		AckWait:       1 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("failed to create JetStream consumer: %w", err)
	}

	log.Printf("Starting human validation worker...")
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		w.processMessage(ctx, msg)
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	<-ctx.Done()
	cc.Stop()
	return nil
}

func (w *Worker) processMessage(ctx context.Context, msg jetstream.Msg) {
	// Acknowledge the message so it doesn't get redelivered
	if err := msg.Ack(); err != nil {
		log.Printf("Failed to ack msg: %v", err)
	}

	replySubj := ""
	if headers := msg.Headers(); headers != nil {
		replySubj = headers.Get("Reply-Subject")
	}

	if replySubj == "" {
		log.Printf("No reply subject found, dropping message")
		return
	}

	// Immediate deterministic error to orchestrator to trigger ErrPendingHumanValidation
	resp := &pb.PerformActionResponse{
		Success:          false,
		ErrorMessage:     "Explicit human validation requested by workflow node",
		IsTransientError: false, // Ensures orchestrator suspends instantly
	}

	respBytes, err := proto.Marshal(resp)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		return
	}

	if _, err := w.js.Publish(ctx, replySubj, respBytes); err != nil {
		log.Printf("Failed to publish reply to NATS %s: %v", replySubj, err)
	} else {
		log.Printf("Successfully triggered Human Validation suspension via reply to %s", replySubj)
	}
}

func (w *Worker) Close() {
	if w.nc != nil {
		w.nc.Close()
	}
}
