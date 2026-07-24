package server

import (
	"context"
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
