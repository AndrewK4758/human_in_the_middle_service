package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/AndrewK4758/shared_protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func (s *HumanValidationServer) CreateValidationTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.CreateTaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Idempotency check: if a task already exists for this JobId, TenantId, AppId, and StepId, return it.
	for _, t := range s.tasks {
		if t.Identity != nil && req.GetIdentity() != nil {
			if t.Identity.GetJobId() == req.GetIdentity().GetJobId() &&
				t.Identity.GetTenantId() == req.GetIdentity().GetTenantId() &&
				t.Identity.GetAppId() == req.GetIdentity().GetAppId() &&
				t.GetStepId() == req.GetStepId() {
				log.Printf("INFO: Idempotent CreateValidationTask: returning existing task %s for Job: %s", t.TaskId, req.GetIdentity().GetJobId())
				return &pb.CreateTaskResponse{
					TaskId: t.TaskId,
					Status: TaskStatusExists,
				}, nil
			}
		}
	}

	taskID := fmt.Sprintf("task_%d", time.Now().UnixNano())
	task := &pb.PendingTask{
		TaskId:             taskID,
		Identity:           req.GetIdentity(),
		StepName:           req.GetStepName(),
		FailedAiOutput:     req.GetFailedAiOutput(),
		ExpectedSchema:     req.GetExpectedSchema(),
		PromptInstructions: req.GetPromptInstructions(),
		ErrorMessage:       req.GetErrorMessage(),
		TargetFields:       req.GetTargetFields(),
		Status:             TaskStatusPending,
		GlobalStateJson:    req.GetGlobalStateJson(),
		JobName:            req.GetJobName(),
		StepId:             req.GetStepId(),
		StepContextInput:   req.GetStepContextInput(),
		Metadata:           req.GetMetadata(),
	}

	s.tasks[taskID] = task
	log.Printf("INFO: Created human validation task %s for Job: %s", taskID, req.GetIdentity().GetJobId())

	return &pb.CreateTaskResponse{
		TaskId: taskID,
		Status: TaskStatusCreated,
	}, nil
}

func (s *HumanValidationServer) GetPendingTasks(req *pb.GetPendingTasksRequest, stream pb.HumanValidationService_GetPendingTasksServer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filter := req.GetFilterStatus()

	for _, task := range s.tasks {
		if filter == "" || task.Status == filter {
			if err := stream.Send(task); err != nil {
				return fmt.Errorf("failed to stream pending task %s: %w", task.TaskId, err)
			}
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
		log.Printf("INFO: Task %s is already RESOLVED, treating as idempotent success", req.GetTaskId())
		return &pb.SubmitCorrectionResponse{
			Success: true,
			Message: "Job already resumed successfully (idempotent)",
		}, nil
	}

	if task.Status != TaskStatusPending {
		s.mu.Unlock()
		return nil, fmt.Errorf("task %s is already %s", req.GetTaskId(), task.Status)
	}

	task.Status = TaskStatusProcessing
	s.mu.Unlock()

	log.Printf("INFO: Human submitted correction for task %s, resuming job %s", req.GetTaskId(), task.GetIdentity().GetJobId())

	// Call Orchestrator to ResumeJob
	conn, err := grpc.NewClient(s.orchestratorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.rollbackTaskStatus(task, TaskStatusPending)
		return nil, fmt.Errorf("failed to connect to orchestrator at %s: %w", s.orchestratorAddr, err)
	}
	defer conn.Close()

	orchClient := pb.NewOrchestratorServiceClient(conn)
	resumeReq := &pb.ResumeJobRequest{
		Identity:      task.GetIdentity(),
		CorrectedJson: req.GetCorrectedJson(),
		StepId:        task.GetStepId(),
	}

	log.Printf("INFO: Sending ResumeJob request to orchestrator for Job: %s", task.GetIdentity().GetJobId())
	resp, err := orchClient.ResumeJob(ctx, resumeReq)
	if err != nil {
		s.rollbackTaskStatus(task, TaskStatusPending)
		return nil, fmt.Errorf("failed to resume job: %w", err)
	}

	if !resp.Success {
		s.rollbackTaskStatus(task, TaskStatusPending)
		return &pb.SubmitCorrectionResponse{
			Success: false,
			Message: fmt.Sprintf("Orchestrator failed to resume job: %s", resp.Message),
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
	log.Printf("INFO: Deleted human validation task %s", taskID)

	return &pb.DeleteTaskResponse{
		Success: true,
	}, nil
}

func (s *HumanValidationServer) rollbackTaskStatus(task *pb.PendingTask, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("INFO: Rolling back task %s to %s due to Orchestrator failure", task.GetTaskId(), status)
	task.Status = status
}
