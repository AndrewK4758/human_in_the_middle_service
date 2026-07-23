package server

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"
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
	streamName := "WORKERS"

	var cons jetstream.Consumer
	var err error
	for i := 0; i < 30; i++ {
		cons, err = s.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			Durable:        "human_validation_workers",
			AckPolicy:      jetstream.AckExplicitPolicy,
			FilterSubjects: []string{"worker.human.validation", "worker.human.validation.>"},
			MaxAckPending:  10,
			AckWait:        1 * time.Minute,
		})
		if err == nil {
			break
		}
		logger.Info("HumanValidation", "Waiting for stream %s to be created... (%v)", streamName, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return fmt.Errorf("failed to create JetStream consumer after retries: %w", err)
	}

	logger.Info("HumanValidation", "Starting Human Validation NATS consumer...")
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
	logger.Info("HumanValidation", "Received human validation request on subject: %s", msg.Subject())

	var replySubj string
	if headers := msg.Headers(); headers != nil {
		replySubj = headers.Get("Reply-Subject")
	}

	if replySubj != "" {
		payloadStruct, _ := structpb.NewStruct(map[string]interface{}{
			"detail": "Explicit human validation requested by workflow node",
		})
		
		resp := &pb.PerformActionResponse{
			Success:          false,
			IsTransientError: false,
			ErrorContract: &pb.ErrorContract{
				Code:          "HUMAN_VALIDATION_REQUIRED",
				Source:        "human_validation_service",
				Message:       "pending human validation",
				Timestamp:     timestamppb.Now(),
				ClientPayload: payloadStruct,
			},
		}

		respBytes, err := proto.Marshal(resp)
		if err != nil {
			logger.Info("HumanValidation", "Failed to marshal response: %v", err)
			msg.NakWithDelay(5 * time.Second)
			return
		}
		
		if _, err := s.js.Publish(ctx, replySubj, respBytes); err != nil {
			logger.Info("HumanValidation", "Failed to publish reply to NATS %s: %v", replySubj, err)
			msg.NakWithDelay(5 * time.Second)
			return
		}
	}

	if err := msg.Ack(); err != nil {
		logger.Info("HumanValidation", "Failed to ack msg: %v", err)
	} else {
		logger.Info("HumanValidation", "Successfully acknowledged human validation request and replied to %s", replySubj)
	}
}
