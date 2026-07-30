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
	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"
	"github.com/AndrewK4758/shared_utils/workflowparser"
)

type TaskCreator interface {
	CreateValidationTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.CreateTaskResponse, error)
}

type NatsServer struct {
	nc               *nats.Conn
	js               jetstream.JetStream
	validationServer TaskCreator
}

func NewNatsServer(nc *nats.Conn, js jetstream.JetStream, validationServer *HumanValidationServer) *NatsServer {
	return &NatsServer{
		nc:               nc,
		js:               js,
		validationServer: validationServer,
	}
}

func (s *NatsServer) Start(ctx context.Context) error {
	streamName := envutil.MustGetString("WORKERS_STREAM")

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
	streamName := envutil.MustGetString("WORKERS_STREAM")
	meta, _ := msg.Metadata()
	var attempt uint64 = 1
	if meta != nil {
		attempt = meta.NumDelivered
	}

	logger.Info("HumanValidation", "Received human validation request on subject: %s", msg.Subject())

	t, a, j := "default", "default", "default"
	var req pb.ExecuteWorkflowNodeRequest
	if err := proto.Unmarshal(msg.Data(), &req); err != nil {
		logger.Error("HumanValidation", "Failed to unmarshal ExecuteWorkflowNodeRequest: %v", err)
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component:  "HumanValidation",
			Event:      "NACKED",
			Stream:     streamName,
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

	parser := workflowparser.NewNodeRequestParser(&req)
	t, a, j = parser.ExtractIdentity()
	if t == "" {
		t = "default"
	}
	if a == "" {
		a = "default"
	}
	if j == "" {
		j = "default"
	}
	jobNameID := parser.GetJobNameId()

	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "HumanValidation",
		Event:     "DEQUEUED",
		Stream:    streamName,
		Subject:   msg.Subject(),
		Consumer:  "human_validation_workers",
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		SizeBytes: len(msg.Data()),
		Attempt:   attempt,
	})

	stepName := parser.GetNodeName()
	systemInstructions := parser.GetSystemInstructions()
	expectedSchema := parser.GetExpectedSchemaStruct()
	stepId := parser.GetStepId()

	humanVal := parser.GetHumanValidationContract()
	hRequired := false
	hBypassFallback := false
	if humanVal != nil {
		hRequired = humanVal.GetRequired()
		hBypassFallback = humanVal.GetBypassFallback()
	}
	logger.Info("HumanValidation", "HumanValidation contract flags - Required: %v, BypassFallback: %v for step '%s' (ID: %s, job_name_id: %d)", hRequired, hBypassFallback, stepName, stepId, jobNameID)

	globalState := parser.GetGlobalState()

	createReq := &pb.CreateTaskRequest{
		Identity:           parser.GetIdentity(),
		StepName:           stepName,
		PromptInstructions: systemInstructions,
		ExpectedSchema:     expectedSchema,
		GlobalState:        globalState,
		StepId:             stepId,
		Metadata:           parser.GetMetadata(),
		ErrorMessage:       fmt.Sprintf("Human validation required for step '%s' (ID: %s)", stepName, stepId),
	}

	if s.validationServer != nil {
		if resp, err := s.validationServer.CreateValidationTask(ctx, createReq); err != nil {
			logger.Error("HumanValidation", "Failed to create validation task: %v", err)
		} else {
			logger.Info("HumanValidation", "Successfully registered pending validation task %s with status %s", resp.GetTaskId(), resp.GetStatus())
		}
	}

	var replySubj string
	if headers := msg.Headers(); headers != nil {
		replySubj = headers.Get("Reply-Subject")
	}

	if replySubj != "" && s.js != nil {
		detailMsg := fmt.Sprintf("Explicit human validation requested for node '%s' (step: %s)", stepName, stepId)
		if stepName == "" && stepId == "" {
			detailMsg = "Explicit human validation requested by workflow node"
		}

		payloadStruct := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"detail": structpb.NewStringValue(detailMsg),
			},
		}

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
				Stream:     streamName,
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
				Stream:     streamName,
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
		logger.Info("HumanValidation", "Successfully acknowledged human validation request and replied to %s (job_name_id: %d)", replySubj, jobNameID)
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component:  "HumanValidation",
			Event:      "ACKNOWLEDGED",
			Stream:     streamName,
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
