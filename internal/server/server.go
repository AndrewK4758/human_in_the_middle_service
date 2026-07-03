package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "doc_processor/shared_protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	taskID := fmt.Sprintf("task_%d", time.Now().UnixNano())
	task := &pb.PendingTask{
		TaskId:             taskID,
		JobId:              req.GetJobId(),
		StepName:           req.GetStepName(),
		FailedAiOutput:     req.GetFailedAiOutput(),
		ExpectedSchema:     req.GetExpectedSchema(),
		PromptInstructions: req.GetPromptInstructions(),
		ErrorMessage:       req.GetErrorMessage(),
		TargetFields:       req.GetTargetFields(),
		Status:             "PENDING",
	}

	s.tasks[taskID] = task
	log.Printf("INFO: Created human validation task %s for Job: %s", taskID, req.GetJobId())

	return &pb.CreateTaskResponse{
		TaskId: taskID,
		Status: "CREATED",
	}, nil
}

func (s *HumanValidationServer) GetPendingTasks(ctx context.Context, req *pb.GetPendingTasksRequest) (*pb.GetPendingTasksResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matched []*pb.PendingTask
	filter := req.GetFilterStatus()

	for _, task := range s.tasks {
		if filter == "" || task.Status == filter {
			matched = append(matched, task)
		}
	}

	return &pb.GetPendingTasksResponse{
		Tasks: matched,
	}, nil
}

func (s *HumanValidationServer) SubmitCorrection(ctx context.Context, req *pb.SubmitCorrectionRequest) (*pb.SubmitCorrectionResponse, error) {
	s.mu.Lock()
	task, exists := s.tasks[req.GetTaskId()]
	if !exists {
		s.mu.Unlock()
		return nil, fmt.Errorf("task %s not found", req.GetTaskId())
	}

	if task.Status != "PENDING" {
		s.mu.Unlock()
		return nil, fmt.Errorf("task %s is already %s", req.GetTaskId(), task.Status)
	}

	task.Status = "RESOLVED"
	s.mu.Unlock()

	log.Printf("INFO: Human submitted correction for task %s, resuming job %s", req.GetTaskId(), task.JobId)

	// Call Orchestrator to ResumeJob
	conn, err := grpc.NewClient(s.orchestratorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to orchestrator at %s: %w", s.orchestratorAddr, err)
	}
	defer conn.Close()

	orchClient := pb.NewOrchestratorServiceClient(conn)
	resumeReq := &pb.ResumeJobRequest{
		JobId:         task.JobId,
		CorrectedJson: req.GetCorrectedJson(),
	}

	log.Printf("INFO: Sending ResumeJob request to orchestrator for Job: %s", task.JobId)
	resp, err := orchClient.ResumeJob(ctx, resumeReq)
	if err != nil {
		return nil, fmt.Errorf("failed to resume job: %w", err)
	}

	if !resp.Success {
		return &pb.SubmitCorrectionResponse{
			Success: false,
			Message: fmt.Sprintf("Orchestrator failed to resume job: %s", resp.Message),
		}, nil
	}

	return &pb.SubmitCorrectionResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}
