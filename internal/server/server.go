package server

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "strings"
    "sync"
    "time"

    "github.com/google/uuid"

    pb "github.com/AndrewK4758/shared_protos"
    "github.com/AndrewK4758/shared_utils/envutil"
    "github.com/AndrewK4758/shared_utils/identityparser"
    "github.com/AndrewK4758/shared_utils/logger"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"

    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "google.golang.org/protobuf/encoding/protojson"
    "google.golang.org/protobuf/proto"
)

const (
	TaskStatusPending    = "PENDING"
	TaskStatusProcessing = "PROCESSING"
	TaskStatusResolved   = "RESOLVED"
	TaskStatusCreated    = "CREATED"
	TaskStatusExists     = "EXISTS"

	EventTypeTaskSuspended = "validation.task_suspended"
	EventTypeTaskClaimed   = "validation.task_claimed"
	EventTypeTaskReleased  = "validation.task_released"
	EventTypeTaskResumed   = "validation.task_resumed"
	EventTypeTaskDeleted   = "validation.task_deleted"
)

type EventPublisher interface {
	Publish(subject string, data []byte) error
}

type NatsEventPublisher struct {
	nc *nats.Conn
}

func NewNatsEventPublisher(nc *nats.Conn) *NatsEventPublisher {
	return &NatsEventPublisher{nc: nc}
}

func (p *NatsEventPublisher) Publish(subject string, data []byte) error {
	if p == nil || p.nc == nil {
		return nil
	}
	return p.nc.Publish(subject, data)
}

type ValidationTracePayload struct {
	EventID      string            `json:"event_id"`
	TenantID     string            `json:"tenant_id"`
	AppID        string            `json:"app_id"`
	JobID        string            `json:"job_id"`
	StepID       string            `json:"step_id"`
	TaskID       string            `json:"task_id"`
	EventType    string            `json:"event_type"`
	Action       string            `json:"action"`
	Status       string            `json:"status"`
	Message      string            `json:"message"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	TargetFields []string          `json:"target_fields,omitempty"`
	ErrorMessage string            `json:"error_message,omitempty"`
	OperatorID   string            `json:"operator_id,omitempty"`
	Timestamp    time.Time         `json:"timestamp"`
}

type HumanValidationServer struct {
	pb.UnimplementedHumanValidationServiceServer
	orchClient pb.OrchestratorServiceClient
	kv         jetstream.KeyValue
	publisher  EventPublisher

	leaseTTL       time.Duration
	reaperInterval time.Duration
	stopReaper     chan struct{}
	reaperOnce     sync.Once
	mu             sync.Mutex
}

func NewHumanValidationServer(js jetstream.JetStream, orchClient pb.OrchestratorServiceClient) *HumanValidationServer {
	return NewHumanValidationServerWithPublisher(js, orchClient, nil)
}

func NewHumanValidationServerWithPublisher(js jetstream.JetStream, orchClient pb.OrchestratorServiceClient, publisher EventPublisher) *HumanValidationServer {
	ctx := context.Background()

	ttlStr := envutil.GetStringOrDefault("HUMAN_VALIDATION_KV_TTL", "168h")
	ttl, err := time.ParseDuration(ttlStr)
	if err != nil {
		ttl = 168 * time.Hour
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "HUMAN_VALIDATION_TASKS",
		TTL:    ttl,
	})
	if err != nil {
		panic(fmt.Errorf("failed to initialize key-value store: %w", err))
	}

	leaseTTLSec := envutil.GetIntOrDefault("HUMAN_VALIDATION_LEASE_TTL_SECONDS", 300)
	reaperIntervalSec := envutil.GetIntOrDefault("HUMAN_VALIDATION_REAPER_INTERVAL_SECONDS", 30)

	srv := &HumanValidationServer{
		orchClient:     orchClient,
		kv:             kv,
		publisher:      publisher,
		leaseTTL:       time.Duration(leaseTTLSec) * time.Second,
		reaperInterval: time.Duration(reaperIntervalSec) * time.Second,
		stopReaper:     make(chan struct{}),
	}

	srv.startLeaseReaper()

	return srv
}

func FormatTaskKey(identity *pb.InfrastructureIdentity, stepID string) string {
	if identity == nil {
		return stepID
	}
	return fmt.Sprintf("%s.%s.%s.%s", identity.GetTenantId(), identity.GetAppId(), identity.GetJobId(), stepID)
}

func (s *HumanValidationServer) SetPublisher(pub EventPublisher) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publisher = pub
}

func (s *HumanValidationServer) emitTraceEvent(tenantID, appID string, event *ValidationTracePayload) {
	s.mu.Lock()
	pub := s.publisher
	s.mu.Unlock()

	if pub == nil {
		return
	}

	subject := fmt.Sprintf("trace.validation.%s.%s", tenantID, appID)
	data, err := json.Marshal(event)
	if err != nil {
		logger.Error("HumanValidation", "Failed to marshal trace event: %v", err)
		return
	}

	if err := pub.Publish(subject, data); err != nil {
		logger.Error("HumanValidation", "Failed to publish trace event to %s: %v", subject, err)
	}
}

func (s *HumanValidationServer) startLeaseReaper() {
	s.reaperOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(s.reaperInterval)
			defer ticker.Stop()

			for {
				select {
				case <-s.stopReaper:
					return
				case <-ticker.C:
					s.reapExpiredLeases(context.Background())
				}
			}
		}()
	})
}

func (s *HumanValidationServer) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.stopReaper:
	default:
		close(s.stopReaper)
	}
}

func (s *HumanValidationServer) reapExpiredLeases(ctx context.Context) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		return
	}

	nowUnix := time.Now().UTC().Unix()

	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}

		var task pb.PendingTask
		if err := proto.Unmarshal(entry.Value(), &task); err != nil {
			continue
		}

		if task.GetStatus() == TaskStatusProcessing && task.GetLeaseExpiresAt() > 0 && task.GetLeaseExpiresAt() < nowUnix {
			logger.Info("HumanValidation", "Lease expired for task %s (claimed by %s). Reverting to PENDING", task.GetTaskId(), task.GetClaimedBy())

			previousOperator := task.GetClaimedBy()
			task.Status = TaskStatusPending
			task.ClaimedBy = ""
			task.LeaseExpiresAt = 0

			taskBytes, err := proto.Marshal(&task)
			if err != nil {
				continue
			}

			if _, err := s.kv.Update(ctx, k, taskBytes, entry.Revision()); err == nil {
				tenantID := task.GetIdentity().GetTenantId()
				appID := task.GetIdentity().GetAppId()
				s.emitTraceEvent(tenantID, appID, &ValidationTracePayload{
					EventID:      uuid.New().String(),
					TenantID:     tenantID,
					AppID:        appID,
					JobID:        task.GetIdentity().GetJobId(),
					StepID:       task.GetStepId(),
					TaskID:       task.GetTaskId(),
					EventType:    EventTypeTaskReleased,
					Action:       "VALIDATION",
					Status:       "RELEASED",
					Message:      fmt.Sprintf("Task lease expired and was released from %s", previousOperator),
					OperatorID:   previousOperator,
					Timestamp:    time.Now().UTC(),
				})
			}
		}
	}
}

func (s *HumanValidationServer) CreateValidationTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.CreateTaskResponse, error) {
	identityparser.NewIdentityParser(req.GetIdentity()).MustValidate(true, "CreateValidationTask")

	reqJson, _ := protojson.Marshal(req)
	logger.Info("HumanValidation", "Received CreateValidationTask request: %s", string(reqJson))

	key := FormatTaskKey(req.GetIdentity(), req.GetStepId())

	// O(1) Idempotency check via deterministic key
	entry, err := s.kv.Get(ctx, key)
	if err == nil {
		var existing pb.PendingTask
		if err := proto.Unmarshal(entry.Value(), &existing); err == nil {
			logger.Info("HumanValidation", "INFO: Idempotent CreateValidationTask: returning existing task %s for Job: %s", existing.GetTaskId(), req.GetIdentity().GetJobId())
			return &pb.CreateTaskResponse{
				TaskId: existing.GetTaskId(),
				Status: TaskStatusExists,
			}, nil
		}
	}

	task := &pb.PendingTask{
		TaskId:             key,
		Identity:           req.GetIdentity(),
		StepName:           req.GetStepName(),
		FailedAiOutput:     req.GetFailedAiOutput(),
		PromptInstructions: req.GetPromptInstructions(),
		ErrorMessage:       req.GetErrorMessage(),
		TargetFields:       req.GetTargetFields(),
		Status:             TaskStatusPending,
		GlobalState:        req.GetGlobalState(),
		StepId:             req.GetStepId(),
		StepContextInput:   req.GetStepContextInput(),
		Metadata:           req.GetMetadata(),
		ExpectedSchema:     req.GetExpectedSchema(),
		CallbackAddress:    req.GetCallbackAddress(),
	}

	taskBytes, err := proto.Marshal(task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal task: %v", err)
	}

	_, err = s.kv.Put(ctx, key, taskBytes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to save task: %v", err)
	}

	logger.Info("HumanValidation", "INFO: Created human validation task %s for Job: %s", key, req.GetIdentity().GetJobId())

	tenantID := req.GetIdentity().GetTenantId()
	appID := req.GetIdentity().GetAppId()
	s.emitTraceEvent(tenantID, appID, &ValidationTracePayload{
		EventID:      uuid.New().String(),
		TenantID:     tenantID,
		AppID:        appID,
		JobID:        req.GetIdentity().GetJobId(),
		StepID:       req.GetStepId(),
		TaskID:       key,
		EventType:    EventTypeTaskSuspended,
		Action:       "VALIDATION",
		Status:       "SUSPENDED",
		Message:      fmt.Sprintf("Workflow suspended at step %s: %s", req.GetStepName(), req.GetErrorMessage()),
		TargetFields: req.GetTargetFields(),
		ErrorMessage: req.GetErrorMessage(),
		Timestamp:    time.Now().UTC(),
	})

	return &pb.CreateTaskResponse{
		TaskId: key,
		Status: TaskStatusCreated,
	}, nil
}

func (s *HumanValidationServer) GetPendingTasks(req *pb.GetPendingTasksRequest, stream pb.HumanValidationService_GetPendingTasksServer) error {
	filter := req.GetFilterStatus()
	tenantID := ""
	appID := ""
	if req.GetIdentity() != nil {
		identityparser.NewIdentityParser(req.GetIdentity()).MustValidate(false, "GetPendingTasks")
		tenantID = req.GetIdentity().GetTenantId()
		appID = req.GetIdentity().GetAppId()
	}

	keys, err := s.kv.Keys(stream.Context())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}
		return status.Errorf(codes.Internal, "failed to read tasks: %v", err)
	}

	for _, k := range keys {
		// Multi-tenant prefix match check if key follows hierarchical structure
		if tenantID != "" && appID != "" {
			prefix := fmt.Sprintf("%s.%s.", tenantID, appID)
			if !strings.HasPrefix(k, prefix) && !strings.Contains(k, tenantID) {
				continue
			}
		}

		entry, err := s.kv.Get(stream.Context(), k)
		if err != nil {
			continue
		}
		var task pb.PendingTask
		if err := proto.Unmarshal(entry.Value(), &task); err == nil {
			if tenantID != "" && task.GetIdentity() != nil && task.GetIdentity().GetTenantId() != tenantID {
				continue
			}
			if appID != "" && task.GetIdentity() != nil && task.GetIdentity().GetAppId() != appID {
				continue
			}

			if filter == "" || task.GetStatus() == filter {
				if err := stream.Send(&task); err != nil {
					return status.Errorf(codes.Internal, "failed to stream pending task %s: %v", task.GetTaskId(), err)
				}
			}
		}
	}

	return nil
}

func (s *HumanValidationServer) getTaskEntry(ctx context.Context, taskID string) (jetstream.KeyValueEntry, *pb.PendingTask, error) {
	entry, err := s.kv.Get(ctx, taskID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil, status.Errorf(codes.NotFound, "task %s not found", taskID)
		}
		return nil, nil, status.Errorf(codes.Internal, "failed to read task: %v", err)
	}

	var task pb.PendingTask
	if err := proto.Unmarshal(entry.Value(), &task); err != nil {
		return nil, nil, status.Errorf(codes.Internal, "failed to parse task: %v", err)
	}

	return entry, &task, nil
}

func (s *HumanValidationServer) ClaimTask(ctx context.Context, req *pb.ClaimTaskRequest) (*pb.ClaimTaskResponse, error) {
	taskID := req.GetTaskId()
	if taskID == "" {
		err := fmt.Errorf("missing required taskId")
		logger.Error("HumanValidation", "FAIL-FAST ASSERTION FAILED (ClaimTask): %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	operatorID := req.GetOperatorId()
	if operatorID == "" {
		err := fmt.Errorf("missing required operatorId")
		logger.Error("HumanValidation", "FAIL-FAST ASSERTION FAILED (ClaimTask): %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	if req.GetIdentity() != nil {
		identityparser.NewIdentityParser(req.GetIdentity()).MustValidate(true, "ClaimTask")
	}

	entry, task, err := s.getTaskEntry(ctx, taskID)
	if err != nil {
		return nil, err
	}

	nowUnix := time.Now().UTC().Unix()

	if task.GetStatus() == TaskStatusResolved {
		return &pb.ClaimTaskResponse{
			Success: false,
			Message: "Task is already resolved",
		}, nil
	}

	// Active lease concurrency check
	if task.GetStatus() == TaskStatusProcessing {
		if task.GetLeaseExpiresAt() > nowUnix && task.GetClaimedBy() != operatorID {
			return &pb.ClaimTaskResponse{
				Success: false,
				Message: fmt.Sprintf("Task is currently claimed by %s", task.GetClaimedBy()),
			}, nil
		}
	}

	leaseSec := req.GetLeaseSeconds()
	if leaseSec <= 0 {
		leaseSec = int32(s.leaseTTL.Seconds())
	}
	leaseExpiresAt := nowUnix + int64(leaseSec)

	task.Status = TaskStatusProcessing
	task.ClaimedBy = operatorID
	task.LeaseExpiresAt = leaseExpiresAt

	taskBytes, err := proto.Marshal(task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal task: %v", err)
	}

	_, err = s.kv.Update(ctx, taskID, taskBytes, entry.Revision())
	if err != nil {
		return &pb.ClaimTaskResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to claim task due to concurrent conflict: %v", err),
		}, nil
	}

	logger.Info("HumanValidation", "Task %s claimed by operator %s until %d", taskID, operatorID, leaseExpiresAt)

	tenantID := task.GetIdentity().GetTenantId()
	appID := task.GetIdentity().GetAppId()
	s.emitTraceEvent(tenantID, appID, &ValidationTracePayload{
		EventID:    uuid.New().String(),
		TenantID:   tenantID,
		AppID:      appID,
		JobID:      task.GetIdentity().GetJobId(),
		StepID:     task.GetStepId(),
		TaskID:     taskID,
		EventType:  EventTypeTaskClaimed,
		Action:     "VALIDATION",
		Status:     "CLAIMED",
		Message:    fmt.Sprintf("Task claimed by operator %s", operatorID),
		OperatorID: operatorID,
		Timestamp:  time.Now().UTC(),
	})

	return &pb.ClaimTaskResponse{
		Success:        true,
		Message:        "Task claimed successfully",
		LeaseExpiresAt: leaseExpiresAt,
	}, nil
}

func (s *HumanValidationServer) ReleaseTask(ctx context.Context, req *pb.ReleaseTaskRequest) (*pb.ReleaseTaskResponse, error) {
	taskID := req.GetTaskId()
	if taskID == "" {
		err := fmt.Errorf("missing required taskId")
		logger.Error("HumanValidation", "FAIL-FAST ASSERTION FAILED (ReleaseTask): %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	if req.GetIdentity() != nil {
		identityparser.NewIdentityParser(req.GetIdentity()).MustValidate(true, "ReleaseTask")
	}

	entry, task, err := s.getTaskEntry(ctx, taskID)
	if err != nil {
		return nil, err
	}

	if task.GetStatus() == TaskStatusResolved {
		return &pb.ReleaseTaskResponse{
			Success: true,
			Message: "Task is already resolved",
		}, nil
	}

	previousOperator := task.GetClaimedBy()
	task.Status = TaskStatusPending
	task.ClaimedBy = ""
	task.LeaseExpiresAt = 0

	taskBytes, err := proto.Marshal(task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal task: %v", err)
	}

	_, err = s.kv.Update(ctx, taskID, taskBytes, entry.Revision())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to release task: %v", err)
	}

	logger.Info("HumanValidation", "Task %s released back to PENDING", taskID)

	tenantID := task.GetIdentity().GetTenantId()
	appID := task.GetIdentity().GetAppId()
	s.emitTraceEvent(tenantID, appID, &ValidationTracePayload{
		EventID:    uuid.New().String(),
		TenantID:   tenantID,
		AppID:      appID,
		JobID:      task.GetIdentity().GetJobId(),
		StepID:     task.GetStepId(),
		TaskID:     taskID,
		EventType:  EventTypeTaskReleased,
		Action:     "VALIDATION",
		Status:     "RELEASED",
		Message:    fmt.Sprintf("Task released back to queue by %s", req.GetOperatorId()),
		OperatorID: previousOperator,
		Timestamp:  time.Now().UTC(),
	})

	return &pb.ReleaseTaskResponse{
		Success: true,
		Message: "Task released successfully",
	}, nil
}

func (s *HumanValidationServer) SubmitCorrection(ctx context.Context, req *pb.SubmitCorrectionRequest) (*pb.SubmitCorrectionResponse, error) {
	if req.GetTaskId() == "" {
		err := fmt.Errorf("missing required taskId")
		logger.Error("HumanValidation", "FAIL-FAST ASSERTION FAILED (SubmitCorrection): %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	if req.GetIdentity() != nil {
		identityparser.NewIdentityParser(req.GetIdentity()).MustValidate(true, "SubmitCorrection")
	}

	entry, task, err := s.getTaskEntry(ctx, req.GetTaskId())
	if err != nil {
		return nil, err
	}

	if task.GetStatus() == TaskStatusResolved {
		logger.Info("HumanValidation", "INFO: Task %s is already RESOLVED, treating as idempotent success", req.GetTaskId())
		return &pb.SubmitCorrectionResponse{
			Success: true,
			Message: "Job already resumed successfully (idempotent)",
		}, nil
	}

	if task.GetStatus() == TaskStatusProcessing && task.GetClaimedBy() == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "task %s is already %s", req.GetTaskId(), task.GetStatus())
	}
	if task.GetStatus() != TaskStatusPending && task.GetStatus() != TaskStatusProcessing {
		return nil, status.Errorf(codes.FailedPrecondition, "task %s is already %s", req.GetTaskId(), task.GetStatus())
	}

	// Atomically transition from PENDING to PROCESSING
	if task.GetStatus() == TaskStatusPending {
		task.Status = TaskStatusProcessing
		taskBytes, err := proto.Marshal(task)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to marshal task: %v", err)
		}
		_, err = s.kv.Update(ctx, req.GetTaskId(), taskBytes, entry.Revision())
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "task %s is already %s: %v", req.GetTaskId(), TaskStatusProcessing, err)
		}
	}

	jobID := task.GetIdentity().GetJobId()
	logger.Info("HumanValidation", "INFO: Human submitted correction for task %s, resuming job %s", req.GetTaskId(), jobID)

	resumeReq := &pb.ResumeJobRequest{
		Identity:        task.GetIdentity(),
		Corrected:       req.GetCorrected(),
		StepId:          task.GetStepId(),
		CallbackAddress: task.GetCallbackAddress(),
	}

	logger.Info("HumanValidation", "INFO: Sending ResumeJob request to orchestrator for Job: %s", jobID)
	if s.orchClient == nil {
		// If no orchestrator client configured, treat as local resolution
		return s.finalizeTaskResolution(ctx, req.GetTaskId(), entry, task)
	}

	resp, err := s.orchClient.ResumeJob(ctx, resumeReq)
	if err != nil {
		s.rollbackTaskStatus(ctx, req.GetTaskId(), TaskStatusPending)
		return nil, status.Errorf(codes.Internal, "failed to resume job: %v", err)
	}

	if !resp.GetSuccess() {
		s.rollbackTaskStatus(ctx, req.GetTaskId(), TaskStatusPending)
		return &pb.SubmitCorrectionResponse{
			Success: false,
			Message: fmt.Sprintf("Orchestrator failed to resume job: %s", resp.GetMessage()),
		}, nil
	}

	return s.finalizeTaskResolution(ctx, req.GetTaskId(), entry, task)
}

func (s *HumanValidationServer) finalizeTaskResolution(ctx context.Context, taskID string, entry jetstream.KeyValueEntry, task *pb.PendingTask) (*pb.SubmitCorrectionResponse, error) {
	currentEntry, err := s.kv.Get(ctx, taskID)
	rev := entry.Revision()
	if err == nil {
		rev = currentEntry.Revision()
	}

	task.Status = TaskStatusResolved
	task.ClaimedBy = ""
	task.LeaseExpiresAt = 0

	tBytes, err := proto.Marshal(task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal resolved task: %v", err)
	}

	if _, err := s.kv.Update(ctx, taskID, tBytes, rev); err != nil {
		logger.Error("HumanValidation", "Failed to update task to RESOLVED: %v", err)
	}

	tenantID := task.GetIdentity().GetTenantId()
	appID := task.GetIdentity().GetAppId()
	s.emitTraceEvent(tenantID, appID, &ValidationTracePayload{
		EventID:   uuid.New().String(),
		TenantID:  tenantID,
		AppID:     appID,
		JobID:     task.GetIdentity().GetJobId(),
		StepID:    task.GetStepId(),
		TaskID:    taskID,
		EventType: EventTypeTaskResumed,
		Action:    "VALIDATION",
		Status:    "RESUMED",
		Message:   "Human correction submitted and workflow resumed successfully",
		Timestamp: time.Now().UTC(),
	})

	return &pb.SubmitCorrectionResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}

func (s *HumanValidationServer) DeleteTask(ctx context.Context, req *pb.DeleteTaskRequest) (*pb.DeleteTaskResponse, error) {
	taskID := req.GetTaskId()
	if taskID == "" {
		err := fmt.Errorf("missing required taskId")
		logger.Error("HumanValidation", "FAIL-FAST ASSERTION FAILED (DeleteTask): %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	if req.GetIdentity() != nil {
		identityparser.NewIdentityParser(req.GetIdentity()).MustValidate(true, "DeleteTask")
	}

	// Retrieve task before deleting to get tenant and app context for telemetry
	entry, err := s.kv.Get(ctx, taskID)
	var task pb.PendingTask
	if err == nil {
		_ = proto.Unmarshal(entry.Value(), &task)
	}

	err = s.kv.Delete(ctx, taskID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", taskID)
		}
		return nil, status.Errorf(codes.Internal, "failed to delete task %s: %v", taskID, err)
	}

	logger.Info("HumanValidation", "INFO: Deleted human validation task %s", taskID)

	tenantID := ""
	appID := ""
	jobID := ""
	stepID := task.GetStepId()
	if task.GetIdentity() != nil {
		tenantID = task.GetIdentity().GetTenantId()
		appID = task.GetIdentity().GetAppId()
		jobID = task.GetIdentity().GetJobId()
	} else if req.GetIdentity() != nil {
		tenantID = req.GetIdentity().GetTenantId()
		appID = req.GetIdentity().GetAppId()
		jobID = req.GetIdentity().GetJobId()
	}

	s.emitTraceEvent(tenantID, appID, &ValidationTracePayload{
		EventID:   uuid.New().String(),
		TenantID:  tenantID,
		AppID:     appID,
		JobID:     jobID,
		StepID:    stepID,
		TaskID:    taskID,
		EventType: EventTypeTaskDeleted,
		Action:    "VALIDATION",
		Status:    "DELETED",
		Message:   fmt.Sprintf("Human validation task %s deleted", taskID),
		Timestamp: time.Now().UTC(),
	})

	return &pb.DeleteTaskResponse{
		Success: true,
	}, nil
}

func (s *HumanValidationServer) rollbackTaskStatus(ctx context.Context, taskID string, statusName string) {
	logger.Info("HumanValidation", "INFO: Rolling back task %s to %s due to Orchestrator failure", taskID, statusName)
	entry, err := s.kv.Get(ctx, taskID)
	if err != nil {
		return
	}
	var task pb.PendingTask
	if err := proto.Unmarshal(entry.Value(), &task); err == nil {
		task.Status = statusName
		task.ClaimedBy = ""
		task.LeaseExpiresAt = 0
		taskBytes, _ := proto.Marshal(&task)
		if _, err := s.kv.Update(ctx, taskID, taskBytes, entry.Revision()); err != nil {
			logger.Error("HumanValidation", "Failed to rollback task status: %v", err)
		}
	}
}
