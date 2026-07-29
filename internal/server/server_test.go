package server

import (
	"context"
	"fmt"
	"net"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/grpc"
)

func TestHumanValidationServer_CreateAndList(t *testing.T) {
	s := NewHumanValidationServer("localhost:50052")

	ctx := context.Background()

	// 1. Create a task
	createReq := &pb.CreateTaskRequest{
		Identity:       &pb.InfrastructureIdentity{JobId: "job_test"},
		StepName:       "Classification",
		FailedAiOutput: `{"classification": ""}`,
		ErrorMessage:   "expected field is empty: classification",
	}

	createResp, err := s.CreateValidationTask(ctx, createReq)
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	taskID := createResp.GetTaskId()
	if taskID == "" {
		t.Fatalf("Expected non-empty TaskId")
	}

	// 2. Get pending tasks
	getReq := &pb.GetPendingTasksRequest{
		FilterStatus: "PENDING",
	}

	mockStream := &mockGetPendingTasksServer{tasks: make([]*pb.PendingTask, 0)}
	err = s.GetPendingTasks(getReq, mockStream)
	if err != nil {
		t.Fatalf("Failed to get pending tasks: %v", err)
	}

	if len(mockStream.tasks) != 1 {
		t.Fatalf("Expected exactly 1 pending task, got %d", len(mockStream.tasks))
	}

	task := mockStream.tasks[0]
	if task.GetTaskId() != taskID {
		t.Errorf("Expected TaskId %s, got %s", taskID, task.GetTaskId())
	}

	if task.GetIdentity() == nil || task.GetIdentity().GetJobId() != "job_test" {
		t.Errorf("Expected JobId 'job_test'")
	}
	if task.GetStatus() != "PENDING" {
		t.Errorf("Expected status 'PENDING', got %s", task.GetStatus())
	}
}

type mockGetPendingTasksServer struct {
	grpc.ServerStream
	tasks []*pb.PendingTask
}

func (m *mockGetPendingTasksServer) Send(task *pb.PendingTask) error {
	m.tasks = append(m.tasks, task)
	return nil
}

func TestHumanValidationServer_DeleteTask(t *testing.T) {
	s := NewHumanValidationServer("localhost:50052")
	ctx := context.Background()

	// 1. Create a task
	createReq := &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_test_delete"},
	}

	createResp, err := s.CreateValidationTask(ctx, createReq)
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	taskID := createResp.GetTaskId()

	// 2. Delete the task
	delReq := &pb.DeleteTaskRequest{
		TaskId: taskID,
	}

	delResp, err := s.DeleteTask(ctx, delReq)
	if err != nil {
		t.Fatalf("Failed to delete task: %v", err)
	}

	if !delResp.GetSuccess() {
		t.Fatalf("Expected success to be true")
	}

	// 3. Verify it's deleted
	if _, exists := s.tasks[taskID]; exists {
		t.Fatalf("Expected task to be deleted from map")
	}

	// 4. Try to delete non-existent task
	_, err = s.DeleteTask(ctx, &pb.DeleteTaskRequest{TaskId: "non_existent"})
	if err == nil {
		t.Fatalf("Expected error when deleting non-existent task")
	}
}

func TestHumanValidationServer_CreateValidationTask_Idempotency(t *testing.T) {
	s := NewHumanValidationServer("localhost:50052")
	ctx := context.Background()

	tests := []struct {
		name          string
		jobId         string
		stepId        string
		useIdentity   bool
		expectedStat  string
		expectedCount int
	}{
		{
			name:          "Happy Path (First Call)",
			jobId:         "job_123",
			stepId:        "step_A",
			useIdentity:   true,
			expectedStat:  "CREATED",
			expectedCount: 1,
		},
		{
			name:          "Idempotency (Exact Duplicate)",
			jobId:         "job_123",
			stepId:        "step_A",
			useIdentity:   true,
			expectedStat:  "EXISTS",
			expectedCount: 1,
		},
		{
			name:          "Edge Case (Different Step)",
			jobId:         "job_123",
			stepId:        "step_B",
			useIdentity:   true,
			expectedStat:  "CREATED",
			expectedCount: 2,
		},
		{
			name:          "Edge Case (Different Job)",
			jobId:         "job_456",
			stepId:        "step_A",
			useIdentity:   true,
			expectedStat:  "CREATED",
			expectedCount: 3,
		},
		{
			name:          "Edge Case (Legacy/Null Identity)",
			useIdentity:   false,
			expectedStat:  "CREATED",
			expectedCount: 4,
		},
	}

	var firstTaskId string

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &pb.CreateTaskRequest{
				StepId: tt.stepId,
			}
			if tt.useIdentity {
				req.Identity = &pb.InfrastructureIdentity{JobId: tt.jobId}
			}

			resp, err := s.CreateValidationTask(ctx, req)
			if err != nil {
				t.Fatalf("Failed to create task: %v", err)
			}

			if resp.Status != tt.expectedStat {
				t.Errorf("Expected status %s, got %s", tt.expectedStat, resp.Status)
			}

			if tt.name == "Happy Path (First Call)" {
				firstTaskId = resp.TaskId
			} else if tt.name == "Idempotency (Exact Duplicate)" {
				if resp.TaskId != firstTaskId {
					t.Errorf("Expected returned TaskId to match first call, got %s", resp.TaskId)
				}
			}

			if len(s.tasks) != tt.expectedCount {
				t.Errorf("Expected map size %d, got %d", tt.expectedCount, len(s.tasks))
			}
		})
	}
}

type mockOrchestratorServer struct {
	pb.UnimplementedOrchestratorServiceServer
	resumeJobFunc func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error)
}

func (m *mockOrchestratorServer) ResumeJob(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
	if m.resumeJobFunc != nil {
		return m.resumeJobFunc(ctx, req)
	}
	return &pb.ResumeJobResponse{Success: true, Message: "resumed successfully"}, nil
}

func startMockOrchestrator(t *testing.T, mock *mockOrchestratorServer) (string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for mock orchestrator: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterOrchestratorServiceServer(grpcServer, mock)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.Stop()
		lis.Close()
	}

	return lis.Addr().String(), cleanup
}

func TestHumanValidationServer_SubmitCorrection(t *testing.T) {
	mockOrch := &mockOrchestratorServer{}
	addr, cleanup := startMockOrchestrator(t, mockOrch)
	defer cleanup()

	s := NewHumanValidationServer(addr)
	ctx := context.Background()

	t.Run("Task Not Found Error Handling", func(t *testing.T) {
		_, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId:        "task_non_existent",
			CorrectedJson: `{"status":"ok"}`,
		})
		if err == nil {
			t.Fatalf("Expected error when submitting correction for non-existent task")
		}
	})

	t.Run("Task Invalid Status (Processing)", func(t *testing.T) {
		createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{JobId: "job_processing"},
			StepId:   "step_proc",
		})
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
		taskId := createResp.GetTaskId()

		s.mu.Lock()
		s.tasks[taskId].Status = TaskStatusProcessing
		s.mu.Unlock()

		_, err = s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId:        taskId,
			CorrectedJson: `{"status":"ok"}`,
		})
		if err == nil {
			t.Fatalf("Expected error when submitting correction for task in PROCESSING status")
		}
	})

	t.Run("Orchestrator ResumeJob Success -> Status Resolved", func(t *testing.T) {
		mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
			if req.GetStepId() != "step_succ" {
				return nil, fmt.Errorf("unexpected step id %s", req.GetStepId())
			}
			return &pb.ResumeJobResponse{Success: true, Message: "Job resumed"}, nil
		}

		createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{JobId: "job_succ"},
			StepId:   "step_succ",
		})
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
		taskId := createResp.GetTaskId()

		resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId:        taskId,
			CorrectedJson: `{"corrected": true}`,
		})
		if err != nil {
			t.Fatalf("Failed to submit correction: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("Expected response success true, got false")
		}

		s.mu.RLock()
		task := s.tasks[taskId]
		s.mu.RUnlock()

		if task.Status != TaskStatusResolved {
			t.Fatalf("Expected task status %s, got %s", TaskStatusResolved, task.Status)
		}
	})

	t.Run("Task Already Resolved (Idempotence)", func(t *testing.T) {
		createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{JobId: "job_already_resolved"},
			StepId:   "step_res",
		})
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
		taskId := createResp.GetTaskId()

		s.mu.Lock()
		s.tasks[taskId].Status = TaskStatusResolved
		s.mu.Unlock()

		resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId:        taskId,
			CorrectedJson: `{"corrected": true}`,
		})
		if err != nil {
			t.Fatalf("Unexpected error for already resolved task: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("Expected success for already resolved task")
		}
	})

	t.Run("Orchestrator ResumeJob Failure (gRPC Error) -> Task Status Rollback to Pending", func(t *testing.T) {
		mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
			return nil, fmt.Errorf("orchestrator connection timeout")
		}

		createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{JobId: "job_fail_grpc"},
			StepId:   "step_fail_grpc",
		})
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
		taskId := createResp.GetTaskId()

		_, err = s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId:        taskId,
			CorrectedJson: `{"corrected": true}`,
		})
		if err == nil {
			t.Fatalf("Expected error when orchestrator ResumeJob returns error")
		}

		s.mu.RLock()
		task := s.tasks[taskId]
		s.mu.RUnlock()

		if task.Status != TaskStatusPending {
			t.Fatalf("Expected status rolled back to %s, got %s", TaskStatusPending, task.Status)
		}
	})

	t.Run("Orchestrator ResumeJob Failure (Success=false) -> Task Status Rollback to Pending", func(t *testing.T) {
		mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
			return &pb.ResumeJobResponse{Success: false, Message: "validation invalid"}, nil
		}

		createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{JobId: "job_fail_resp"},
			StepId:   "step_fail_resp",
		})
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
		taskId := createResp.GetTaskId()

		resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId:        taskId,
			CorrectedJson: `{"corrected": true}`,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp.GetSuccess() {
			t.Fatalf("Expected response success to be false when orchestrator fails")
		}

		s.mu.RLock()
		task := s.tasks[taskId]
		s.mu.RUnlock()

		if task.Status != TaskStatusPending {
			t.Fatalf("Expected status rolled back to %s, got %s", TaskStatusPending, task.Status)
		}
	})
}
