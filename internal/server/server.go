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
		Identity:           req.GetIdentity(),
		StepName:           req.GetStepName(),
		FailedAiOutput:     req.GetFailedAiOutput(),
		ExpectedSchema:     req.GetExpectedSchema(),
		PromptInstructions: req.GetPromptInstructions(),
		ErrorMessage:       req.GetErrorMessage(),
		TargetFields:       req.GetTargetFields(),
		Status:             "PENDING",
	}

	s.tasks[taskID] = task
	log.Printf("INFO: Created human validation task %s for Job: %s", taskID, req.GetIdentity().GetJobId())

	return &pb.CreateTaskResponse{
		TaskId: taskID,
		Status: "CREATED",
	}, nil
}

func (s *HumanValidationServer) GetPendingTasks(req *pb.GetPendingTasksRequest, stream pb.HumanValidationService_GetPendingTasksServer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filter := req.GetFilterStatus()

	for _, task := range s.tasks {
		if filter == "" || task.Status == filter {
			if err := stream.Send(task); err != nil {
				return err
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

	if task.Status != "PENDING" {
		s.mu.Unlock()
		return nil, fmt.Errorf("task %s is already %s", req.GetTaskId(), task.Status)
	}

	task.Status = "RESOLVED"
	s.mu.Unlock()

	log.Printf("INFO: Human submitted correction for task %s, resuming job %s", req.GetTaskId(), task.GetIdentity().GetJobId())

	// Call Orchestrator to ResumeJob
	conn, err := grpc.NewClient(s.orchestratorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
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
