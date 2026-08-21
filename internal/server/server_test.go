package server

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestHumanValidationServer_CreateAndList(t *testing.T) {
	mockPub := newMockEventPublisher()
	s := NewHumanValidationServerWithPublisher(&mockJetStreamForServer{}, nil, mockPub)
	defer s.Close()

	ctx := context.Background()

	// 1. Create a task
	createReq := &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_test",
			AppId:    "app_test",
			JobId:    "job_test",
		},
		StepId:         "step_classify",
		StepName:       "Classification",
		FailedAiOutput: `{"classification": ""}`,
		ErrorMessage:   "expected field is empty: classification",
	}

	createResp, err := s.CreateValidationTask(ctx, createReq)
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	taskID := createResp.GetTaskId()
	expectedKey := "tenant_test.app_test.job_test.step_classify"
	if taskID != expectedKey {
		t.Fatalf("Expected TaskId %s, got %s", expectedKey, taskID)
	}
	if createResp.GetStatus() != TaskStatusCreated {
		t.Fatalf("Expected status %s, got %s", TaskStatusCreated, createResp.GetStatus())
	}

	// Verify trace event published
	events := mockPub.GetEvents("trace.validation.tenant_test.app_test")
	if len(events) != 1 {
		t.Fatalf("Expected 1 trace event, got %d", len(events))
	}
	var traceEvt ValidationTracePayload
	if err := json.Unmarshal(events[0], &traceEvt); err != nil {
		t.Fatalf("Failed to unmarshal trace event: %v", err)
	}
	if traceEvt.EventType != EventTypeTaskSuspended || traceEvt.Status != "SUSPENDED" {
		t.Errorf("Expected event_type %s and status SUSPENDED, got %s / %s", EventTypeTaskSuspended, traceEvt.EventType, traceEvt.Status)
	}

	// 2. Get pending tasks with tenant filter
	getReq := &pb.GetPendingTasksRequest{
		FilterStatus: "PENDING",
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_test",
			AppId:    "app_test",
			JobId:    "job_test",
		},
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

func (m *mockGetPendingTasksServer) Context() context.Context {
	return context.Background()
}

func TestHumanValidationServer_DeleteTask(t *testing.T) {
	mockPub := newMockEventPublisher()
	s := NewHumanValidationServerWithPublisher(&mockJetStreamForServer{}, nil, mockPub)
	defer s.Close()
	ctx := context.Background()

	// 1. Create a task
	createReq := &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_test",
			AppId:    "app_test",
			JobId:    "job_test_delete",
		},
		StepId: "step_del",
	}

	createResp, err := s.CreateValidationTask(ctx, createReq)
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	taskID := createResp.GetTaskId()

	// 2. Delete the task
	delReq := &pb.DeleteTaskRequest{
		TaskId: taskID,
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_test",
			AppId:    "app_test",
			JobId:    "job_test_delete",
		},
	}

	delResp, err := s.DeleteTask(ctx, delReq)
	if err != nil {
		t.Fatalf("Failed to delete task: %v", err)
	}

	if !delResp.GetSuccess() {
		t.Fatalf("Expected success to be true")
	}

	// 3. Verify it's deleted from KV
	if _, err := s.kv.Get(context.Background(), taskID); err == nil {
		t.Fatalf("Expected task to be deleted from map")
	}

	// Verify trace event
	events := mockPub.GetEvents("trace.validation.tenant_test.app_test")
	foundDeleted := false
	for _, e := range events {
		var evt ValidationTracePayload
		if err := json.Unmarshal(e, &evt); err == nil && evt.EventType == EventTypeTaskDeleted {
			foundDeleted = true
			break
		}
	}
	if !foundDeleted {
		t.Errorf("Expected TASK_DELETED trace event to be published")
	}

	// 4. Try to delete non-existent task
	_, err = s.DeleteTask(ctx, &pb.DeleteTaskRequest{
		TaskId: "non_existent",
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_test",
			AppId:    "app_test",
			JobId:    "job_test_delete",
		},
	})
	if err == nil {
		t.Fatalf("Expected error when deleting non-existent task")
	}
}

func TestHumanValidationServer_CreateValidationTask_Idempotency(t *testing.T) {
	mockPub := newMockEventPublisher()
	s := NewHumanValidationServerWithPublisher(&mockJetStreamForServer{}, nil, mockPub)
	defer s.Close()
	ctx := context.Background()

	tests := []struct {
		name          string
		jobId         string
		stepId        string
		useIdentity   bool
		expectedStat  string
		expectedPanic bool
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
			name:          "Edge Case (Null Identity Fail Fast)",
			useIdentity:   false,
			expectedPanic: true,
			expectedCount: 3,
		},
	}

	var firstTaskId string

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &pb.CreateTaskRequest{
				StepId: tt.stepId,
			}
			if tt.useIdentity {
				req.Identity = &pb.InfrastructureIdentity{
					TenantId: "tenant_idemp",
					AppId:    "app_idemp",
					JobId:    tt.jobId,
				}
			}

			var resp *pb.CreateTaskResponse
			var err error

			if tt.expectedPanic {
				func() {
					defer func() {
						if r := recover(); r == nil {
							t.Fatalf("Expected panic for null identity, got none")
						}
					}()
					resp, err = s.CreateValidationTask(ctx, req)
				}()
				return
			} else {
				resp, err = s.CreateValidationTask(ctx, req)
			}

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

			if countTasks(s) != tt.expectedCount {
				t.Errorf("Expected map size %d, got %d", tt.expectedCount, countTasks(s))
			}
		})
	}
}

func TestHumanValidationServer_MultiTenantStreaming(t *testing.T) {
	s := NewHumanValidationServer(&mockJetStreamForServer{}, nil)
	defer s.Close()
	ctx := context.Background()

	// Create 2 tasks for tenant A
	_, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "tenant-A", AppId: "app-1", JobId: "job-1"},
		StepId:   "step-1",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	_, err = s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "tenant-A", AppId: "app-1", JobId: "job-2"},
		StepId:   "step-1",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	// Create 1 task for tenant B
	_, err = s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "tenant-B", AppId: "app-1", JobId: "job-3"},
		StepId:   "step-1",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	// Stream for tenant A
	streamA := &mockGetPendingTasksServer{tasks: make([]*pb.PendingTask, 0)}
	err = s.GetPendingTasks(&pb.GetPendingTasksRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "tenant-A", AppId: "app-1"},
	}, streamA)
	if err != nil {
		t.Fatalf("Failed to get pending tasks: %v", err)
	}

	if len(streamA.tasks) != 2 {
		t.Fatalf("Expected exactly 2 tasks for tenant-A, got %d", len(streamA.tasks))
	}
	for _, task := range streamA.tasks {
		if task.GetIdentity().GetTenantId() != "tenant-A" {
			t.Errorf("Expected tenant-A, got %s", task.GetIdentity().GetTenantId())
		}
	}
}

func TestHumanValidationServer_PessimisticLeaseClaimAndRelease(t *testing.T) {
	mockPub := newMockEventPublisher()
	s := NewHumanValidationServerWithPublisher(&mockJetStreamForServer{}, nil, mockPub)
	defer s.Close()
	ctx := context.Background()

	// 1. Create a task
	createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "tenant-1", AppId: "app-1", JobId: "job-lease"},
		StepId:   "step-lease",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	taskId := createResp.GetTaskId()

	identity := &pb.InfrastructureIdentity{TenantId: "tenant-1", AppId: "app-1", JobId: "job-lease"}

	// 2. Operator 1 claims the task
	claimResp1, err := s.ClaimTask(ctx, &pb.ClaimTaskRequest{
		TaskId:       taskId,
		OperatorId:   "operator-alice",
		LeaseSeconds: 60,
		Identity:     identity,
	})
	if err != nil {
		t.Fatalf("Failed to claim task: %v", err)
	}
	if !claimResp1.GetSuccess() {
		t.Fatalf("Expected claim to succeed")
	}
	if claimResp1.GetLeaseExpiresAt() <= time.Now().Unix() {
		t.Errorf("Expected future lease expiration timestamp")
	}

	task := getTask(s, taskId)
	if task.GetStatus() != TaskStatusProcessing {
		t.Errorf("Expected status %s, got %s", TaskStatusProcessing, task.GetStatus())
	}
	if task.GetClaimedBy() != "operator-alice" {
		t.Errorf("Expected claimed_by 'operator-alice', got %s", task.GetClaimedBy())
	}

	// 3. Operator 2 attempts to claim while active -> MUST FAIL
	claimResp2, err := s.ClaimTask(ctx, &pb.ClaimTaskRequest{
		TaskId:       taskId,
		OperatorId:   "operator-bob",
		LeaseSeconds: 60,
		Identity:     identity,
	})
	if err != nil {
		t.Fatalf("Unexpected error for conflicting claim: %v", err)
	}
	if claimResp2.GetSuccess() {
		t.Fatalf("Expected conflicting claim by operator-bob to fail while alice holds active lease")
	}

	// 4. Operator 1 releases the task
	relResp, err := s.ReleaseTask(ctx, &pb.ReleaseTaskRequest{
		TaskId:     taskId,
		OperatorId: "operator-alice",
		Identity:   identity,
	})
	if err != nil {
		t.Fatalf("Failed to release task: %v", err)
	}
	if !relResp.GetSuccess() {
		t.Fatalf("Expected release to succeed")
	}

	taskAfterRel := getTask(s, taskId)
	if taskAfterRel.GetStatus() != TaskStatusPending {
		t.Errorf("Expected status %s after release, got %s", TaskStatusPending, taskAfterRel.GetStatus())
	}
	if taskAfterRel.GetClaimedBy() != "" {
		t.Errorf("Expected empty claimed_by after release, got %s", taskAfterRel.GetClaimedBy())
	}

	// 5. Operator 2 can now claim the released task
	claimRespBob, err := s.ClaimTask(ctx, &pb.ClaimTaskRequest{
		TaskId:       taskId,
		OperatorId:   "operator-bob",
		LeaseSeconds: 60,
		Identity:     identity,
	})
	if err != nil {
		t.Fatalf("Failed to claim released task: %v", err)
	}
	if !claimRespBob.GetSuccess() {
		t.Fatalf("Expected operator-bob to succeed in claiming released task")
	}
}

func TestHumanValidationServer_LeaseReaper_AutoRecovery(t *testing.T) {
	mockPub := newMockEventPublisher()
	s := NewHumanValidationServerWithPublisher(&mockJetStreamForServer{}, nil, mockPub)
	defer s.Close()
	ctx := context.Background()

	createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "tenant-1", AppId: "app-1", JobId: "job-expire"},
		StepId:   "step-expire",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	taskId := createResp.GetTaskId()

	// Manually set task in PROCESSING status with expired lease (in the past)
	task := getTask(s, taskId)
	task.Status = TaskStatusProcessing
	task.ClaimedBy = "operator-crashed"
	task.LeaseExpiresAt = time.Now().Add(-10 * time.Second).Unix()
	taskBytes, _ := proto.Marshal(task)
	s.kv.Put(ctx, taskId, taskBytes)

	// Run reaper pass
	s.reapExpiredLeases(ctx)

	// Verify task reverted to PENDING
	reapedTask := getTask(s, taskId)
	if reapedTask.GetStatus() != TaskStatusPending {
		t.Errorf("Expected reaped task status %s, got %s", TaskStatusPending, reapedTask.GetStatus())
	}
	if reapedTask.GetClaimedBy() != "" {
		t.Errorf("Expected reaped task claimed_by to be empty, got %s", reapedTask.GetClaimedBy())
	}
}

func TestHumanValidationServer_SubmitCorrection(t *testing.T) {
	mockOrch := &mockOrchClient{}
	mockPub := newMockEventPublisher()

	s := NewHumanValidationServerWithPublisher(&mockJetStreamForServer{}, mockOrch, mockPub)
	defer s.Close()
	ctx := context.Background()

	t.Run("Task Not Found Error Handling", func(t *testing.T) {
		_, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId: "task_non_existent",
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_test",
			},
			Corrected: unmarshalStruct(`{"status":"ok"}`),
		})
		if err == nil {
			t.Fatalf("Expected error when submitting correction for non-existent task")
		}
	})

	t.Run("Claimed Task in PROCESSING State -> SubmitCorrection Succeeds -> Status RESOLVED", func(t *testing.T) {
		createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_proc_sub",
			},
			StepId: "step_proc",
		})
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
		taskId := createResp.GetTaskId()

		// Claim task
		_, err = s.ClaimTask(ctx, &pb.ClaimTaskRequest{
			TaskId:     taskId,
			OperatorId: "alice",
			Identity:   &pb.InfrastructureIdentity{TenantId: "tenant_test", AppId: "app_test", JobId: "job_proc_sub"},
		})
		if err != nil {
			t.Fatalf("Failed to claim task: %v", err)
		}

		// Submit correction
		resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId: taskId,
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_proc_sub",
			},
			Corrected: unmarshalStruct(`{"status":"ok"}`),
		})
		if err != nil {
			t.Fatalf("Failed to submit correction on claimed task: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("Expected submit to succeed")
		}

		task := getTask(s, taskId)
		if task.GetStatus() != TaskStatusResolved {
			t.Errorf("Expected status %s, got %s", TaskStatusResolved, task.GetStatus())
		}
	})

	t.Run("Orchestrator ResumeJob Success -> Status Resolved", func(t *testing.T) {
		mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest, opts ...grpc.CallOption) (*pb.ResumeJobResponse, error) {
			if req.GetStepId() != "step_succ" {
				return nil, fmt.Errorf("unexpected step id %s", req.GetStepId())
			}
			return &pb.ResumeJobResponse{Success: true, Message: "Job resumed"}, nil
		}

		createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_succ",
			},
			StepId: "step_succ",
		})
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
		taskId := createResp.GetTaskId()

		resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId: taskId,
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_succ",
			},
			Corrected: unmarshalStruct(`{"corrected": true}`),
		})
		if err != nil {
			t.Fatalf("Failed to submit correction: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("Expected response success true, got false")
		}

		task := getTask(s, taskId)
		if task.Status != TaskStatusResolved {
			t.Fatalf("Expected task status %s, got %s", TaskStatusResolved, task.Status)
		}
	})

	t.Run("Task Already Resolved (Idempotence)", func(t *testing.T) {
		createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_already_resolved",
			},
			StepId: "step_res",
		})
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
		taskId := createResp.GetTaskId()

		setTaskStatus(s, taskId, TaskStatusResolved)

		resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId: taskId,
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_already_resolved",
			},
			Corrected: unmarshalStruct(`{"corrected": true}`),
		})
		if err != nil {
			t.Fatalf("Unexpected error for already resolved task: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("Expected success for already resolved task")
		}
	})

	t.Run("Orchestrator ResumeJob Failure (gRPC Error) -> Task Status Rollback to Pending", func(t *testing.T) {
		mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest, opts ...grpc.CallOption) (*pb.ResumeJobResponse, error) {
			return nil, fmt.Errorf("orchestrator connection timeout")
		}

		createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_fail_grpc",
			},
			StepId: "step_fail_grpc",
		})
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
		taskId := createResp.GetTaskId()

		_, err = s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId: taskId,
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_fail_grpc",
			},
			Corrected: unmarshalStruct(`{"corrected": true}`),
		})
		if err == nil {
			t.Fatalf("Expected error when orchestrator ResumeJob returns error")
		}

		task := getTask(s, taskId)
		if task.Status != TaskStatusPending {
			t.Fatalf("Expected status rolled back to %s, got %s", TaskStatusPending, task.Status)
		}
	})

	t.Run("Orchestrator ResumeJob Failure (Success=false) -> Task Status Rollback to Pending", func(t *testing.T) {
		mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest, opts ...grpc.CallOption) (*pb.ResumeJobResponse, error) {
			return &pb.ResumeJobResponse{Success: false, Message: "validation invalid"}, nil
		}

		createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_fail_resp",
			},
			StepId: "step_fail_resp",
		})
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
		taskId := createResp.GetTaskId()

		resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId: taskId,
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_test",
				AppId:    "app_test",
				JobId:    "job_fail_resp",
			},
			Corrected: unmarshalStruct(`{"corrected": true}`),
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp.GetSuccess() {
			t.Fatalf("Expected response success to be false when orchestrator fails")
		}

		task := getTask(s, taskId)
		if task.Status != TaskStatusPending {
			t.Fatalf("Expected status rolled back to %s, got %s", TaskStatusPending, task.Status)
		}
	})
}

func TestHumanValidationServer_FailFastPanics(t *testing.T) {
	s := NewHumanValidationServer(&mockJetStreamForServer{}, nil)
	defer s.Close()
	ctx := context.Background()

	t.Run("CreateValidationTask nil identity panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Expected panic for nil identity")
			}
		}()
		_, _ = s.CreateValidationTask(ctx, &pb.CreateTaskRequest{StepId: "step1"})
	})

	t.Run("CreateValidationTask empty tenant panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Expected panic for empty tenant")
			}
		}()
		_, _ = s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: &pb.InfrastructureIdentity{TenantId: "", AppId: "app", JobId: "job"},
			StepId:   "step1",
		})
	})

	t.Run("ClaimTask invalid identity panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Expected panic for invalid identity")
			}
		}()
		_, _ = s.ClaimTask(ctx, &pb.ClaimTaskRequest{
			TaskId:     "t1",
			OperatorId: "op1",
			Identity:   &pb.InfrastructureIdentity{TenantId: "", AppId: "app", JobId: "job"},
		})
	})

	t.Run("ReleaseTask invalid identity panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Expected panic for invalid identity")
			}
		}()
		_, _ = s.ReleaseTask(ctx, &pb.ReleaseTaskRequest{
			TaskId:     "t1",
			OperatorId: "op1",
			Identity:   &pb.InfrastructureIdentity{TenantId: "tenant", AppId: "", JobId: "job"},
		})
	})

	t.Run("SubmitCorrection invalid identity panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Expected panic for invalid identity")
			}
		}()
		_, _ = s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId:   "t1",
			Identity: &pb.InfrastructureIdentity{TenantId: "tenant", AppId: "app", JobId: ""},
		})
	})

	t.Run("DeleteTask invalid identity panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Expected panic for invalid identity")
			}
		}()
		_, _ = s.DeleteTask(ctx, &pb.DeleteTaskRequest{
			TaskId:   "t1",
			Identity: &pb.InfrastructureIdentity{TenantId: "", AppId: "", JobId: ""},
		})
	})
}
