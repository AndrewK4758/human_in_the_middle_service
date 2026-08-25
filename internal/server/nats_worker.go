package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/AndrewK4758/shared_utils/identityparser"
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
	consumerName     string
	filterSubjects   []string
	responsesStream  string
}

func NewNatsServer(nc *nats.Conn, js jetstream.JetStream, validationServer TaskCreator) *NatsServer {
	filterSubjectsStr := envutil.GetStringOrDefault("WORKERS_FILTER_SUBJECTS", "worker.human.>")
	return &NatsServer{
		nc:               nc,
		js:               js,
		validationServer: validationServer,
		consumerName:     envutil.GetStringOrDefault("WORKERS_CONSUMER_NAME", "human_validation_workers"),
		filterSubjects:   strings.Split(filterSubjectsStr, ","),
		responsesStream:  envutil.GetStringOrDefault("ORCHESTRATOR_RESPONSES_STREAM", "ORCHESTRATOR_RESPONSES"),
	}
}

func (s *NatsServer) Start(ctx context.Context) error {
	streamName := envutil.MustGetString("WORKERS_STREAM")

	var cons jetstream.Consumer
	var err error
	for range 90 {
		cons, err = s.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			Durable:        s.consumerName,
			AckPolicy:      jetstream.AckExplicitPolicy,
			FilterSubjects: s.filterSubjects,
			MaxAckPending:  envutil.GetIntOrDefault("MAX_ACK_PENDING", 10),
			AckWait:        1 * time.Minute,
			BackOff: []time.Duration{
				5 * time.Second,
				30 * time.Second,
				1 * time.Minute,
				5 * time.Minute,
			},
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
	workerPoolSize := envutil.GetIntOrDefault("WORKER_POOL_SIZE", 10)
	sem := make(chan struct{}, workerPoolSize)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		go s.HandleMessageWithRecovery(ctx, msg, sem)
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	<-ctx.Done()
	cc.Stop()
	return nil
}

func (s *NatsServer) HandleMessageWithRecovery(ctx context.Context, msg jetstream.Msg, sem chan struct{}) {
	sem <- struct{}{}
	defer func() {
		<-sem
		if r := recover(); r != nil {
			var t, a, j = "unknown", "unknown", "unknown"
			var req pb.ExecuteTaskRequest
			if err := proto.Unmarshal(msg.Data(), &req); err == nil && req.Identity != nil {
				t = req.Identity.TenantId
				a = req.Identity.AppId
				j = req.Identity.JobId
			}
			logger.Error("HumanValidation", "Panic recovered in NATS consumer goroutine for TenantID: %s, AppID: %s, JobID: %s. Error: %v", t, a, j, r)
			msg.NakWithDelay(5 * time.Second)
		}
	}()
	s.processMessage(ctx, msg)
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

	t, a, j := "", "", ""
	var req pb.ExecuteTaskRequest
	if err := proto.Unmarshal(msg.Data(), &req); err != nil {
		logger.Error("HumanValidation", "Failed to unmarshal ExecuteTaskRequest: %v", err)
		s.nackMessage(msg, streamName, t, a, j, attempt, startTime, err)
		return
	}

	identityparser.NewIdentityParser(req.GetIdentity()).MustValidate(true, "HumanValidation_NatsWorker")

	if req.GetTaskSubject() == "" {
		err := fmt.Errorf("action failed for job HumanValidation_NatsWorker: missing required TaskSubject")
		s.nackMessage(msg, streamName, t, a, j, attempt, startTime, err)
		return
	}

	t, a, j = natsutil.ExtractIdentity(req.Identity)

	reqBytes, err := protojson.Marshal(&req)
	if err == nil {
		logger.Info("HumanValidation", "DEQUEUED Input Payload: %s", string(reqBytes))
	} else {
		logger.Info("HumanValidation", "DEQUEUED Input Payload (proto.MarshalText): %s", req.String())
	}

	parser := workflowparser.NewTaskRequestParser(&req)
	parser.MustValidateRootPayloadStrict(j)

	executeTaskPayload := parser.MustGetExecuteTaskPayload()

	stepName := parser.MustGetNodeNameStrict(j)
	stepId := req.StepId

	var systemInstructions string
	var expectedSchema *structpb.Struct
	if executeTaskPayload.NodeConfig != nil {
		systemInstructions = executeTaskPayload.NodeConfig.SystemInstructions

		if executeTaskPayload.NodeConfig.ExpectedSchemaStruct != nil {
			expectedSchema = executeTaskPayload.NodeConfig.ExpectedSchemaStruct
		}
	}

	humanVal := executeTaskPayload.HumanValidation

	var globalState *pb.GlobalState
	if executeTaskPayload.GlobalState != nil {
		b, err := executeTaskPayload.GlobalState.MarshalJSON()
		if err != nil {
			logger.Error("HumanValidation", "Failed to marshal GlobalState: %v", err)
			s.nackMessage(msg, streamName, t, a, j, attempt, startTime, err)
			return
		}
		globalState = &pb.GlobalState{}
		if err := protojson.Unmarshal(b, globalState); err != nil {
			logger.Error("HumanValidation", "Failed to unmarshal GlobalState: %v", err)
			s.nackMessage(msg, streamName, t, a, j, attempt, startTime, err)
			return
		}
	}

	metadata := map[string]string{}
	if executeTaskPayload.Metadata != nil {
		for k, v := range executeTaskPayload.Metadata.Fields {
			if v.GetStringValue() != "" {
				metadata[k] = v.GetStringValue()
			}
		}
	}

	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "HumanValidation",
		Event:     "DEQUEUED",
		Stream:    streamName,
		Subject:   msg.Subject(),
		Consumer:  s.consumerName,
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		SizeBytes: len(msg.Data()),
		Attempt:   attempt,
	})

	if humanVal != nil {
		logger.Info("HumanValidation", "HumanValidation contract flags - Required: %v, BypassFallback: %v for step '%s' (ID: %s)", humanVal.GetRequired(), humanVal.GetBypassFallback(), stepName, stepId)
	} else {
		logger.Info("HumanValidation", "HumanValidation contract not provided for step '%s' (ID: %s)", stepName, stepId)
	}

	createReq := &pb.CreateTaskRequest{
		Identity:           req.Identity,
		StepName:           stepName,
		PromptInstructions: systemInstructions,
		ExpectedSchema:     expectedSchema,
		GlobalState:        globalState,
		StepId:             stepId,
		Metadata:           metadata,
		ErrorMessage:       fmt.Sprintf("Human validation required for step '%s' (ID: %s)", stepName, stepId),
		CallbackAddress:    req.GetCallbackAddress(),
	}

	if s.validationServer != nil {
		resp, err := s.validationServer.CreateValidationTask(ctx, createReq)
		if err != nil {
			logger.Error("HumanValidation", "Failed to create validation task: %v", err)
			s.nackMessage(msg, streamName, t, a, j, attempt, startTime, err)
			return
		}
		logger.Info("HumanValidation", "Successfully registered pending validation task %s with status %s", resp.GetTaskId(), resp.GetStatus())
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
		if resp.ErrorContract != nil && resp.ErrorContract.ClientPayload != nil {
			outBytes, err := protojson.Marshal(resp.ErrorContract.ClientPayload)
			if err == nil {
				logger.Info("HumanValidation", "DISPATCHING Output Payload: %s", string(outBytes))
			} else {
				logger.Info("HumanValidation", "DISPATCHING Output Payload (proto): %s", resp.ErrorContract.ClientPayload.String())
			}
		}

		respBytes, err := proto.Marshal(resp)
		if err != nil {
			logger.Info("HumanValidation", "Failed to marshal response: %v", err)
			s.nackMessage(msg, streamName, t, a, j, attempt, startTime, err)
			return
		}

		_, pubErr := s.js.Publish(ctx, replySubj, respBytes)
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component: "HumanValidation",
			Event:     "ENQUEUED",
			Stream:    s.responsesStream,
			Subject:   replySubj,
			TenantID:  t,
			AppID:     a,
			JobID:     j,
			SizeBytes: len(respBytes),
			Error:     pubErr,
		})
		if pubErr != nil {
			logger.Info("HumanValidation", "Failed to publish reply to NATS %s: %v", replySubj, pubErr)
			s.nackMessage(msg, streamName, t, a, j, attempt, startTime, pubErr)
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
			Stream:     streamName,
			Subject:    msg.Subject(),
			Consumer:   s.consumerName,
			TenantID:   t,
			AppID:      a,
			JobID:      j,
			Attempt:    attempt,
			DurationMs: durationMs,
		})
	}
}

func (s *NatsServer) nackMessage(msg jetstream.Msg, streamName, t, a, j string, attempt uint64, startTime time.Time, err error) {
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "HumanValidation",
		Event:      "NACKED",
		Stream:     streamName,
		Subject:    msg.Subject(),
		Consumer:   s.consumerName,
		TenantID:   t,
		AppID:      a,
		JobID:      j,
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
		Error:      err,
	})
	msg.NakWithDelay(5 * time.Second)
}
