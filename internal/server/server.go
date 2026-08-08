package server

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/identityparser"
	"github.com/AndrewK4758/shared_utils/logger"
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
)

type HumanValidationServer struct {
	pb.UnimplementedHumanValidationServiceServer
	orchClient pb.OrchestratorServiceClient
	kv         jetstream.KeyValue
}

func NewHumanValidationServer(js jetstream.JetStream, orchClient pb.OrchestratorServiceClient) *HumanValidationServer {
	ctx := context.Background()
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "HUMAN_VALIDATION_TASKS",
	})
	if err != nil {
		panic(fmt.Errorf("failed to initialize key-value store: %w", err))
	}

	return &HumanValidationServer{
		orchClient: orchClient,
		kv:         kv,
	}
}

func equalIdentity(a, b *pb.InfrastructureIdentity) bool {
	if a == nil || b == nil {
		return false
	}
	return a.GetTenantId() == b.GetTenantId() &&
		a.GetAppId() == b.GetAppId() &&
		a.GetJobId() == b.GetJobId()
}

func (s *HumanValidationServer) CreateValidationTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.CreateTaskResponse, error) {
	identityparser.NewIdentityParser(req.GetIdentity()).MustValidate(true, "CreateValidationTask")

	reqJson, _ := protojson.Marshal(req)
	logger.Info("HumanValidation", "Received CreateValidationTask request: %s", string(reqJson))

	// Idempotency check
	keys, err := s.kv.Keys(ctx)
	if err != nil && err != jetstream.ErrNoKeysFound {
		return nil, status.Errorf(codes.Internal, "failed to get keys from kv: %v", err)
	}
	if err == nil {
		for _, k := range keys {
			entry, err := s.kv.Get(ctx, k)
			if err != nil {
				continue
			}
			var t pb.PendingTask
			if err := proto.Unmarshal(entry.Value(), &t); err == nil {
				if equalIdentity(t.GetIdentity(), req.GetIdentity()) && t.GetStepId() == req.GetStepId() {
					logger.Info("HumanValidation", "INFO: Idempotent CreateValidationTask: returning existing task %s for Job: %s", t.TaskId, req.GetIdentity().GetJobId())
					return &pb.CreateTaskResponse{
						TaskId: t.TaskId,
						Status: TaskStatusExists,
					}, nil
				}
			}
		}
	}

	taskID := uuid.New().String()
	task := &pb.PendingTask{
		TaskId:             taskID,
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
	}

	taskBytes, err := proto.Marshal(task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal task: %v", err)
	}

	_, err = s.kv.Put(ctx, taskID, taskBytes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to save task: %v", err)
	}

	logger.Info("HumanValidation", "INFO: Created human validation task %s for Job: %s", taskID, req.GetIdentity().GetJobId())

	return &pb.CreateTaskResponse{
		TaskId: taskID,
		Status: TaskStatusCreated,
	}, nil
}

func (s *HumanValidationServer) GetPendingTasks(req *pb.GetPendingTasksRequest, stream pb.HumanValidationService_GetPendingTasksServer) error {
	filter := req.GetFilterStatus()

	keys, err := s.kv.Keys(stream.Context())
	if err != nil {
		if err == jetstream.ErrNoKeysFound {
			return nil
		}
		return status.Errorf(codes.Internal, "failed to read tasks: %v", err)
	}

	for _, k := range keys {
		entry, err := s.kv.Get(stream.Context(), k)
		if err != nil {
			continue
		}
		var task pb.PendingTask
		if err := proto.Unmarshal(entry.Value(), &task); err == nil {
			if filter == "" || task.Status == filter {
				if err := stream.Send(&task); err != nil {
					return status.Errorf(codes.Internal, "failed to stream pending task %s: %v", task.TaskId, err)
				}
			}
		}
	}

	return nil
}

func (s *HumanValidationServer) SubmitCorrection(ctx context.Context, req *pb.SubmitCorrectionRequest) (*pb.SubmitCorrectionResponse, error) {
	if req.GetTaskId() == "" {
		err := fmt.Errorf("missing required taskId")
		logger.Error("HumanValidation", "FAIL-FAST ASSERTION FAILED (SubmitCorrection): %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	entry, err := s.kv.Get(ctx, req.GetTaskId())
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return nil, status.Errorf(codes.NotFound, "task %s not found", req.GetTaskId())
		}
		return nil, status.Errorf(codes.Internal, "failed to read task: %v", err)
	}

	var task pb.PendingTask
	if err := proto.Unmarshal(entry.Value(), &task); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse task: %v", err)
	}

	if task.Status == TaskStatusResolved {
		logger.Info("HumanValidation", "INFO: Task %s is already RESOLVED, treating as idempotent success", req.GetTaskId())
		return &pb.SubmitCorrectionResponse{
			Success: true,
			Message: "Job already resumed successfully (idempotent)",
		}, nil
	}

	if task.Status != TaskStatusPending {
		return nil, status.Errorf(codes.FailedPrecondition, "task %s is already %s", req.GetTaskId(), task.Status)
	}

	task.Status = TaskStatusProcessing
	taskBytes, _ := proto.Marshal(&task)
	_, err = s.kv.Update(ctx, req.GetTaskId(), taskBytes, entry.Revision())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update task status: %v", err)
	}

	jobID := task.GetIdentity().GetJobId()
	logger.Info("HumanValidation", "INFO: Human submitted correction for task %s, resuming job %s", req.GetTaskId(), jobID)

	resumeReq := &pb.ResumeJobRequest{
		Identity:  task.GetIdentity(),
		Corrected: req.GetCorrected(),
		StepId:    task.GetStepId(),
	}

	logger.Info("HumanValidation", "INFO: Sending ResumeJob request to orchestrator for Job: %s", jobID)
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

	entry2, err := s.kv.Get(ctx, req.GetTaskId())
	if err == nil {
		var t2 pb.PendingTask
		if proto.Unmarshal(entry2.Value(), &t2) == nil {
			t2.Status = TaskStatusResolved
			t2Bytes, _ := proto.Marshal(&t2)
			if _, err := s.kv.Update(ctx, req.GetTaskId(), t2Bytes, entry2.Revision()); err != nil {
				logger.Error("HumanValidation", "Failed to update task to RESOLVED: %v", err)
			}
		}
	}

	return &pb.SubmitCorrectionResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}

func (s *HumanValidationServer) DeleteTask(ctx context.Context, req *pb.DeleteTaskRequest) (*pb.DeleteTaskResponse, error) {
	taskID := req.GetTaskId()
	err := s.kv.Delete(ctx, taskID)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return nil, status.Errorf(codes.NotFound, "task %s not found", taskID)
		}
		return nil, status.Errorf(codes.Internal, "failed to delete task %s: %v", taskID, err)
	}

	logger.Info("HumanValidation", "INFO: Deleted human validation task %s", taskID)

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
		taskBytes, _ := proto.Marshal(&task)
		if _, err := s.kv.Update(ctx, taskID, taskBytes, entry.Revision()); err != nil {
			logger.Error("HumanValidation", "Failed to rollback task status: %v", err)
		}
	}
}
