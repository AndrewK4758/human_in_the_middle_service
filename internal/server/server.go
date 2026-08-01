package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	orchestratorAddr string
	mu               sync.RWMutex
	tasks            map[string]*pb.PendingTask
}

func NewHumanValidationServer(orchestratorAddr string) *HumanValidationServer {
	return &HumanValidationServer{
		orchestratorAddr: orchestratorAddr,
		tasks:            make(map[string]*pb.PendingTask),
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
	s.mu.Lock()
	defer s.mu.Unlock()

	// Idempotency check: if a task already exists for this JobId, TenantId, AppId, and StepId, return it.
	for _, t := range s.tasks {
		if equalIdentity(t.GetIdentity(), req.GetIdentity()) && t.GetStepId() == req.GetStepId() {
			logger.Info("HumanValidation", "INFO: Idempotent CreateValidationTask: returning existing task %s for Job: %s", t.TaskId, req.GetIdentity().GetJobId())
			return &pb.CreateTaskResponse{
				TaskId: t.TaskId,
				Status: TaskStatusExists,
			}, nil
		}
	}

	taskID := fmt.Sprintf("task_%d", time.Now().UnixNano())
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

	s.tasks[taskID] = task
	logger.Info("HumanValidation", "INFO: Created human validation task %s for Job: %s", taskID, req.GetIdentity().GetJobId())

	return &pb.CreateTaskResponse{
		TaskId: taskID,
		Status: TaskStatusCreated,
	}, nil
}

func (s *HumanValidationServer) GetPendingTasks(req *pb.GetPendingTasksRequest, stream pb.HumanValidationService_GetPendingTasksServer) error {
	s.mu.RLock()
	filter := req.GetFilterStatus()
	var tasksToStream []*pb.PendingTask

	for _, task := range s.tasks {
		if filter == "" || task.Status == filter {
			tasksToStream = append(tasksToStream, proto.Clone(task).(*pb.PendingTask))
		}
	}
	s.mu.RUnlock()

	for _, task := range tasksToStream {
		if err := stream.Send(task); err != nil {
			return fmt.Errorf("failed to stream pending task %s: %w", task.TaskId, err)
		}
	}

	return nil
}

func (s *HumanValidationServer) SubmitCorrection(ctx context.Context, req *pb.SubmitCorrectionRequest) (*pb.SubmitCorrectionResponse, error) {
	s.mu.Lock()
	task, exists := s.tasks[req.GetTaskId()]
	if !exists {
		s.mu.Unlock()
		return nil, fmt.Errorf("task %s not found", req.GetTaskId())
	}

	if task.Status == TaskStatusResolved {
		s.mu.Unlock()
		logger.Info("HumanValidation", "INFO: Task %s is already RESOLVED, treating as idempotent success", req.GetTaskId())
		return &pb.SubmitCorrectionResponse{
			Success: true,
			Message: "Job already resumed successfully (idempotent)",
		}, nil
	}

	currentStatus := task.Status
	if currentStatus != TaskStatusPending {
		s.mu.Unlock()
		return nil, fmt.Errorf("task %s is already %s", req.GetTaskId(), currentStatus)
	}

	task.Status = TaskStatusProcessing
	identity := task.GetIdentity()
	stepID := task.GetStepId()
	s.mu.Unlock()

	jobID := identity.GetJobId()
	logger.Info("HumanValidation", "INFO: Human submitted correction for task %s, resuming job %s", req.GetTaskId(), jobID)

	// Call Orchestrator to ResumeJob
	conn, err := grpc.NewClient(s.orchestratorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.rollbackTaskStatus(task, TaskStatusPending)
		return nil, fmt.Errorf("failed to connect to orchestrator at %s: %w", s.orchestratorAddr, err)
	}
	defer conn.Close()

	orchClient := pb.NewOrchestratorServiceClient(conn)
	resumeReq := &pb.ResumeJobRequest{
		Identity:      identity,
		Corrected:     req.GetCorrected(),
		StepId:        stepID,
	}

	logger.Info("HumanValidation", "INFO: Sending ResumeJob request to orchestrator for Job: %s", jobID)
	resp, err := orchClient.ResumeJob(ctx, resumeReq)
	if err != nil {
		s.rollbackTaskStatus(task, TaskStatusPending)
		return nil, fmt.Errorf("failed to resume job: %w", err)
	}

	if !resp.GetSuccess() {
		s.rollbackTaskStatus(task, TaskStatusPending)
		return &pb.SubmitCorrectionResponse{
			Success: false,
			Message: fmt.Sprintf("Orchestrator failed to resume job: %s", resp.GetMessage()),
		}, nil
	}

	s.mu.Lock()
	task.Status = TaskStatusResolved
	s.mu.Unlock()

	return &pb.SubmitCorrectionResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}

func (s *HumanValidationServer) DeleteTask(ctx context.Context, req *pb.DeleteTaskRequest) (*pb.DeleteTaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	taskID := req.GetTaskId()
	_, exists := s.tasks[taskID]
	if !exists {
		return nil, fmt.Errorf("task %s not found", taskID)
	}

	delete(s.tasks, taskID)
	logger.Info("HumanValidation", "INFO: Deleted human validation task %s", taskID)

	return &pb.DeleteTaskResponse{
		Success: true,
	}, nil
}

func (s *HumanValidationServer) rollbackTaskStatus(task *pb.PendingTask, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logger.Info("HumanValidation", "INFO: Rolling back task %s to %s due to Orchestrator failure", task.GetTaskId(), status)
	task.Status = status
}
