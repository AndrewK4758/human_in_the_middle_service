package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/proto"
)

// TestStress_ConcurrentTaskCreation tests creation of 500 tasks concurrently to check for task ID collisions and race conditions.
func TestStress_ConcurrentTaskCreation(t *testing.T) {
	s := NewHumanValidationServer("localhost:50052")
	ctx := context.Background()

	const numGoroutines = 100
	const tasksPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	taskIDs := sync.Map{}
	var createdCount int64
	var existsCount int64

	for i := 0; i < numGoroutines; i++ {
		go func(gID int) {
			defer wg.Done()
			for j := 0; j < tasksPerGoroutine; j++ {
				req := &pb.CreateTaskRequest{
					Identity: &pb.InfrastructureIdentity{
						TenantId: fmt.Sprintf("tenant_%d", gID),
						AppId:    "app_stress",
						JobId:    fmt.Sprintf("job_%d_%d", gID, j),
					},
					StepId:       fmt.Sprintf("step_%d", j),
					StepName:     "StressStep",
					ErrorMessage: "Stress testing",
				}
				resp, err := s.CreateValidationTask(ctx, req)
				if err != nil {
					t.Errorf("CreateValidationTask error: %v", err)
					return
				}
				if resp.Status == TaskStatusCreated {
					atomic.AddInt64(&createdCount, 1)
					if _, loaded := taskIDs.LoadOrStore(resp.TaskId, true); loaded {
						t.Errorf("DUPLICATE TASK ID GENERATED: %s", resp.TaskId)
					}
				} else if resp.Status == TaskStatusExists {
					atomic.AddInt64(&existsCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Created: %d, Exists: %d, Map Size: %d", createdCount, existsCount, len(s.tasks))
	if createdCount != int64(numGoroutines*tasksPerGoroutine) {
		t.Errorf("Expected %d created tasks, got %d", numGoroutines*tasksPerGoroutine, createdCount)
	}
}

// TestStress_ConcurrentReadWriteDelete tests concurrent Create, GetPendingTasks, and Delete operations.
func TestStress_ConcurrentReadWriteDelete(t *testing.T) {
	s := NewHumanValidationServer("localhost:50052")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	const numWriters = 20
	const numReaders = 20
	const numDeleters = 10

	// 1. Writers adding tasks
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			count := 0
			for {
				select {
					case <-ctx.Done():
						return
					default:
						req := &pb.CreateTaskRequest{
							Identity: &pb.InfrastructureIdentity{
								TenantId: "tenant_rw",
								AppId:    "app_rw",
								JobId:    fmt.Sprintf("job_rw_%d_%d", id, count),
							},
							StepId: "step_rw",
						}
						_, _ = s.CreateValidationTask(ctx, req)
						count++
						time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	// 2. Readers streaming pending tasks
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					stream := &mockGetPendingTasksServer{tasks: make([]*pb.PendingTask, 0)}
					_ = s.GetPendingTasks(&pb.GetPendingTasksRequest{FilterStatus: "PENDING"}, stream)
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}

	// 3. Deleters removing tasks
	for i := 0; i < numDeleters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					s.mu.Lock()
					var idToDelete string
					for id := range s.tasks {
						idToDelete = id
						break
					}
					s.mu.Unlock()

					if idToDelete != "" {
						_, _ = s.DeleteTask(ctx, &pb.DeleteTaskRequest{TaskId: idToDelete})
					}
					time.Sleep(3 * time.Millisecond)
				}
			}
		}()
	}

	wg.Wait()
}

// TestStateMachine_ConcurrentSubmitCorrection tests state machine transitions and concurrent submissions for the same task.
func TestStateMachine_ConcurrentSubmitCorrection(t *testing.T) {
	mockOrch := &mockOrchestratorServer{}
	addr, cleanup := startMockOrchestrator(t, mockOrch)
	defer cleanup()

	s := NewHumanValidationServer(addr)
	ctx := context.Background()

	// Setup slow orchestrator response
	var orchCallCount int64
	mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
		atomic.AddInt64(&orchCallCount, 1)
		time.Sleep(50 * time.Millisecond) // simulate work
		return &pb.ResumeJobResponse{Success: true, Message: "resumed"}, nil
	}

	// Create 1 task
	createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_state_machine"},
		StepId:   "step_state",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	taskId := createResp.GetTaskId()

	// Launch 20 concurrent goroutines calling SubmitCorrection for the SAME task
	const numCallers = 20
	var wg sync.WaitGroup
	wg.Add(numCallers)

	type result struct {
		resp *pb.SubmitCorrectionResponse
		err  error
	}
	results := make([]result, numCallers)

	for i := 0; i < numCallers; i++ {
		go func(idx int) {
			defer wg.Done()
			r, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
				TaskId:        taskId,
				Corrected: unmarshalStruct(`{"fixed": true}`),
			})
			results[idx] = result{resp: r, err: err}
		}(i)
	}

	wg.Wait()

	// Analyze results
	var successCount int
	var errAlreadyProcessingCount int
	var idempotentSuccessCount int

	for _, r := range results {
		if r.err != nil {
			if r.err.Error() == fmt.Sprintf("task %s is already PROCESSING", taskId) {
				errAlreadyProcessingCount++
			} else {
				t.Errorf("Unexpected error: %v", r.err)
			}
		} else if r.resp.GetSuccess() {
			successCount++
			if r.resp.GetMessage() == "Job already resumed successfully (idempotent)" {
				idempotentSuccessCount++
			}
		}
	}

	t.Logf("Orchestrator calls: %d, Successes: %d (Idempotent: %d), Already Processing Errors: %d",
		orchCallCount, successCount, idempotentSuccessCount, errAlreadyProcessingCount)

	if orchCallCount != 1 {
		t.Errorf("Expected Orchestrator to be called exactly once, but called %d times!", orchCallCount)
	}

	s.mu.RLock()
	finalStatus := s.tasks[taskId].Status
	s.mu.RUnlock()

	if finalStatus != TaskStatusResolved {
		t.Errorf("Expected final status RESOLVED, got %s", finalStatus)
	}
}

// TestStateMachine_RollbackAndRetry verifies full PENDING -> PROCESSING -> PENDING (rollback) -> PROCESSING -> RESOLVED loop.
func TestStateMachine_RollbackAndRetry(t *testing.T) {
	mockOrch := &mockOrchestratorServer{}
	addr, cleanup := startMockOrchestrator(t, mockOrch)
	defer cleanup()

	s := NewHumanValidationServer(addr)
	ctx := context.Background()

	createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_rollback_retry"},
		StepId:   "step_rr",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	taskId := createResp.GetTaskId()

	// Phase 1: Fail Orchestrator call
	mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
		return nil, fmt.Errorf("temporary network error")
	}

	_, err = s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:        taskId,
		Corrected: unmarshalStruct(`{"fixed": false}`),
	})
	if err == nil {
		t.Fatalf("Expected error on orchestrator failure")
	}

	// Verify rollback to PENDING
	s.mu.RLock()
	statusAfterFail := s.tasks[taskId].Status
	s.mu.RUnlock()

	if statusAfterFail != TaskStatusPending {
		t.Fatalf("Expected status to roll back to PENDING, got %s", statusAfterFail)
	}

	// Phase 2: Fix Orchestrator and retry
	mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
		return &pb.ResumeJobResponse{Success: true, Message: "resumed after retry"}, nil
	}

	resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:        taskId,
		Corrected: unmarshalStruct(`{"fixed": true}`),
	})
	if err != nil {
		t.Fatalf("Unexpected error on retry: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Expected success on retry")
	}

	// Verify final status RESOLVED
	s.mu.RLock()
	statusAfterSuccess := s.tasks[taskId].Status
	s.mu.RUnlock()

	if statusAfterSuccess != TaskStatusResolved {
		t.Fatalf("Expected final status RESOLVED, got %s", statusAfterSuccess)
	}
}

// TestNatsServer_ConcurrentProcessing tests NATS worker handling concurrent JetStream messages.
func TestNatsServer_ConcurrentProcessing(t *testing.T) {
	valServer := NewHumanValidationServer("localhost:50052")
	mockJS := newMockJetStream()
	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	const numMsgs = 50
	var wg sync.WaitGroup
	wg.Add(numMsgs)

	ctx := context.Background()

	for i := 0; i < numMsgs; i++ {
		go func(idx int) {
			defer wg.Done()
			req := &pb.ExecuteWorkflowNodeRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "t_nats",
					AppId:    "a_nats",
					JobId:    fmt.Sprintf("job_nats_%d", idx),
				},
				StepId: fmt.Sprintf("step_nats_%d", idx),
				NodeConfig: &pb.NodeConfiguration{
					NodeName: "NatsStep",
				},
			}
			data, _ := proto.Marshal(req)
			msg := &mockMsg{
				data:    data,
				subject: "worker.human.validation",
			}
			natsServer.processMessage(ctx, msg)
			if !msg.acked {
				t.Errorf("Message %d was not acked", idx)
			}
		}(i)
	}

	wg.Wait()

	valServer.mu.RLock()
	taskCount := len(valServer.tasks)
	valServer.mu.RUnlock()

	if taskCount != numMsgs {
		t.Errorf("Expected %d pending tasks created via NATS, got %d", numMsgs, taskCount)
	}
}
