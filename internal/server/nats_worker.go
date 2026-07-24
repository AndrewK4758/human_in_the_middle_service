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
	"github.com/AndrewK4758/shared_utils/natsutil"
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
	for i := 0; i < 90; i++ {
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
	startTime := time.Now()
	meta, _ := msg.Metadata()
	var attempt uint64 = 1
	if meta != nil {
		attempt = meta.NumDelivered
	}

	logger.Info("HumanValidation", "Received human validation request on subject: %s", msg.Subject())

	t, a, j := "default", "default", "default"
	var req pb.ExecuteWorkflowNodeRequest
	if err := proto.Unmarshal(msg.Data(), &req); err == nil && req.Identity != nil {
		t, a, j = natsutil.ExtractIdentity(req.Identity)
	}

	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "HumanValidation",
		Event:     "DEQUEUED",
		Stream:    "WORKERS",
		Subject:   msg.Subject(),
		Consumer:  "human_validation_workers",
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		SizeBytes: len(msg.Data()),
		Attempt:   attempt,
	})

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
			natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
				Component:  "HumanValidation",
				Event:      "NACKED",
				Stream:     "WORKERS",
				Subject:    msg.Subject(),
				Consumer:   "human_validation_workers",
				TenantID:   t,
				AppID:      a,
				JobID:      j,
				Attempt:    attempt,
				DurationMs: time.Since(startTime).Milliseconds(),
				Error:      err,
			})
			msg.NakWithDelay(5 * time.Second)
			return
		}

		_, pubErr := s.js.Publish(ctx, replySubj, respBytes)
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component: "HumanValidation",
			Event:     "ENQUEUED",
			Stream:    "ORCHESTRATOR_RESPONSES",
			Subject:   replySubj,
			TenantID:  t,
			AppID:     a,
			JobID:     j,
			SizeBytes: len(respBytes),
			Error:     pubErr,
		})
		if pubErr != nil {
			logger.Info("HumanValidation", "Failed to publish reply to NATS %s: %v", replySubj, pubErr)
			natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
				Component:  "HumanValidation",
				Event:      "NACKED",
				Stream:     "WORKERS",
				Subject:    msg.Subject(),
				Consumer:   "human_validation_workers",
				TenantID:   t,
				AppID:      a,
				JobID:      j,
				Attempt:    attempt,
				DurationMs: time.Since(startTime).Milliseconds(),
				Error:      pubErr,
			})
			msg.NakWithDelay(5 * time.Second)
			return
		}
	}

	durationMs := time.Since(startTime).Milliseconds()
	if err := msg.Ack(); err != nil {
		logger.Info("HumanValidation", "Failed to ack msg: %v", err)
	} else {
		logger.Info("HumanValidation", "Successfully acknowledged human validation request and replied to %s", replySubj)
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component:  "HumanValidation",
			Event:      "ACKNOWLEDGED",
			Stream:     "WORKERS",
			Subject:    msg.Subject(),
			Consumer:   "human_validation_workers",
			TenantID:   t,
			AppID:      a,
			JobID:      j,
			Attempt:    attempt,
			DurationMs: durationMs,
		})
	}
}
