package server

import (
	"context"
	"testing"

	pb "doc_processor/shared_protos"
)

func TestHumanValidationServer_CreateAndList(t *testing.T) {
	s := NewHumanValidationServer("localhost:50052")

	ctx := context.Background()

	// 1. Create a task
	createReq := &pb.CreateTaskRequest{
		JobId:          "job_test",
		StepName:       "Classification",
		FailedAiOutput: `{"classification": ""}`,
		ExpectedSchema: `{"type": "object", "properties": {"classification": {"type": "string"}}}`,
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

	getResp, err := s.GetPendingTasks(ctx, getReq)
	if err != nil {
		t.Fatalf("Failed to get pending tasks: %v", err)
	}

	if len(getResp.GetTasks()) != 1 {
		t.Fatalf("Expected exactly 1 pending task, got %d", len(getResp.GetTasks()))
	}

	task := getResp.GetTasks()[0]
	if task.GetTaskId() != taskID {
		t.Errorf("Expected TaskId %s, got %s", taskID, task.GetTaskId())
	}
	if task.GetJobId() != "job_test" {
		t.Errorf("Expected JobId 'job_test', got %s", task.GetJobId())
	}
	if task.GetStatus() != "PENDING" {
		t.Errorf("Expected status 'PENDING', got %s", task.GetStatus())
	}
}
