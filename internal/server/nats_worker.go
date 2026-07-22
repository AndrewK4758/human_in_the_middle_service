package server

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	pb "github.com/AndrewK4758/shared_protos"
)

type NatsServer struct {
	nc *nats.Conn
	js jetstream.JetStream
}

func NewNatsServer(nc *nats.Conn, js jetstream.JetStream) *NatsServer {
	return &NatsServer{
		nc: nc,
		js: js,
	}
}

func (s *NatsServer) Start(ctx context.Context) error {
	streamName := "HUMAN_VALIDATION"
	_, err := s.js.Stream(ctx, streamName)
	if err != nil {
		_, err = s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{ "worker.human.validation", "worker.human.validation.>" },
		})
		if err != nil {
			return fmt.Errorf("failed to create JetStream stream: %w", err)
		}
		log.Printf("Successfully created JetStream stream: %s", streamName)
	}

	cons, err := s.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "human_validation_workers",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubjects: []string{"worker.human.validation", "worker.human.validation.>"},
		MaxAckPending: 10,
		AckWait:       1 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("failed to create JetStream consumer: %w", err)
	}

	log.Printf("Starting Human Validation NATS consumer...")
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		s.processMessage(ctx, msg)
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	<-ctx.Done()
	cc.Stop()
	return nil
}

func (s *NatsServer) processMessage(ctx context.Context, msg jetstream.Msg) {
	log.Printf("Received human validation request on subject: %s", msg.Subject())

	var replySubj string
	if headers := msg.Headers(); headers != nil {
		replySubj = headers.Get("Reply-Subject")
	}

	if replySubj != "" {
		resp := &pb.PerformActionResponse{
			Success:          false,
			IsTransientError: false,
			ErrorMessage:     "pending human validation",
		}
		respBytes, _ := proto.Marshal(resp)
		if _, err := s.js.Publish(ctx, replySubj, respBytes); err != nil {
			log.Printf("Failed to publish reply to NATS %s: %v", replySubj, err)
			msg.NakWithDelay(5 * time.Second)
			return
		}
	}

	if err := msg.Ack(); err != nil {
		log.Printf("Failed to ack msg: %v", err)
	} else {
		log.Printf("Successfully acknowledged human validation request and replied to %s", replySubj)
	}
}
