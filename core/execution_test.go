package core

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type managedTestPlatform struct{ stubPlatformEngine }

func (p *managedTestPlatform) ReconstructReplyCtx(string) (any, error) { return "reply", nil }

type managedTestRuntime struct {
	*resultAgentSession
	barrier chan struct{}
}

func (s *managedTestRuntime) TerminateExecution(ctx context.Context) error {
	if s.barrier == nil {
		return nil
	}
	select {
	case <-s.barrier:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func executionTestServer(t *testing.T, path string, agent Agent) (*WebhookServer, *Engine, *managedTestPlatform) {
	t.Helper()
	if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	p := &managedTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("project", agent, []Platform{p}, "", LangEnglish)
	ws := NewWebhookServer(0, "secret", "/hook")
	ws.RegisterEngine("project", e)
	if err := ws.EnableExecution(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.execution.cancel(); ws.execution.wg.Wait(); ws.execution.lock.Close(); _ = e.Stop() })
	return ws, e, p
}
func managedRequest(task, attempt string, n int) executionRequest {
	raw, _ := json.Marshal(map[string]any{"task_id": task, "attempt_id": attempt, "attempt": n, "repository": "acme/project", "pull_number": 42, "role": "reviewer", "sha": "abc"})
	return executionRequest{Version: 1, Operation: "deliver", Route: executionRoute{Provider: "cc-connect", Target: "project", Session: "test:thread"}, Task: raw, Prompt: "review task"}
}
func managedCall(t *testing.T, ws *WebhookServer, req executionRequest, want int) executionReceipt {
	t.Helper()
	raw, _ := json.Marshal(req)
	r := httptest.NewRequest("POST", "/hook/execution", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	ws.handleExecution(w, r)
	if w.Code != want {
		t.Fatalf("%s: got %d want %d: %s", req.Operation, w.Code, want, w.Body)
	}
	var result struct {
		Receipt executionReceipt `json:"receipt"`
	}
	if want == 200 {
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
	}
	return result.Receipt
}
func awaitManagedState(t *testing.T, b *executionInbox, key, state string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		r := b.state.Records[key]
		matches := r != nil && r.Receipt.State == state
		b.mu.Unlock()
		if matches {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("execution did not reach %s", state)
}
func TestManagedExecutionQueuesWhileBusyAndDeduplicatesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.json")
	runtime := &managedTestRuntime{resultAgentSession: newResultAgentSession("review done")}
	ws, e, p := executionTestServer(t, path, &resultAgent{session: runtime})
	session := e.sessions.GetOrCreateActive("test:thread")
	gen, ok := session.TryLock()
	if !ok {
		t.Fatal("lock")
	}
	req := managedRequest("task", "attempt", 1)
	first := managedCall(t, ws, req, 200)
	if first.State != "queued" {
		t.Fatal(first)
	}
	ws.execution.tick()
	if len(p.getSent()) != 0 {
		t.Fatal("busy session received task")
	}
	again := managedCall(t, ws, req, 200)
	if again != first {
		t.Fatal("duplicate delivery changed receipt")
	}
	ws.execution.lock.Close()
	reopened := NewWebhookServer(0, "secret", "/hook")
	reopened.RegisterEngine("project", e)
	if err := reopened.EnableExecution(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.execution.cancel(); reopened.execution.wg.Wait(); reopened.execution.lock.Close() })
	if receipt := managedCall(t, reopened, req, 200); receipt != first {
		t.Fatal("restart lost admission", receipt)
	}
	session.Unlock(gen)
	reopened.execution.tick()
	awaitManagedState(t, reopened.execution, "attempt:attempt", "succeeded")
	if sent := p.getSent(); len(sent) == 0 || !strings.Contains(strings.Join(sent, "\n"), "review done") {
		t.Fatal("queued task never delivered", sent)
	}
	// Replaying after completion cannot run the model again.
	managedCall(t, reopened, req, 200)
	reopened.execution.tick()
	if len(runtime.sentPrompts) != 1 {
		t.Fatal("duplicate execution", runtime.sentPrompts)
	}
}
func TestManagedStopTombstonePreventsDelayedDelivery(t *testing.T) {
	ws, _, _ := executionTestServer(t, filepath.Join(t.TempDir(), "inbox.json"), &stubAgent{})
	req := managedRequest("task", "attempt", 1)
	req.Operation = "stop"
	req.Prompt = ""
	first := managedCall(t, ws, req, 200)
	if first.State != "stopped" {
		t.Fatal(first)
	}
	req.Operation = "deliver"
	req.Prompt = "review task"
	if r := managedCall(t, ws, req, 200); r.State != "stopped" {
		t.Fatal("delayed request resurrected", r)
	}
	next := managedRequest("task", "next", 2)
	managedCall(t, ws, next, 200)
	changed := next
	changed.Prompt = "different"
	managedCall(t, ws, changed, 409)
}
func TestManagedExecutionHoldsSessionUntilTerminationConfirmed(t *testing.T) {
	barrier := make(chan struct{})
	runtime := &managedTestRuntime{resultAgentSession: newResultAgentSession("done"), barrier: barrier}
	ws, e, _ := executionTestServer(t, filepath.Join(t.TempDir(), "inbox.json"), &resultAgent{session: runtime})
	req := managedRequest("task", "attempt", 1)
	managedCall(t, ws, req, 200)
	ws.execution.tick()
	awaitManagedState(t, ws.execution, "attempt:attempt", "running")
	session := e.sessions.GetOrCreateActive(req.Route.Session)
	if _, ok := session.TryLock(); ok {
		t.Fatal("normal turn bypassed managed fence")
	}
	if session.ForceUnlock() {
		t.Fatal("force unlock bypassed managed fence")
	}
	if _, ok := session.BreakStaleLock(-time.Second); ok {
		t.Fatal("stale recovery bypassed managed fence")
	}
	close(barrier)
	awaitManagedState(t, ws.execution, "attempt:attempt", "succeeded")
	deadline := time.Now().Add(time.Second)
	for session.Busy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if session.Busy() {
		t.Fatal("completed execution did not release session")
	}
}
func TestManagedOrphanBlocksNewWorkAndRequiresExplicitResolution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.json")
	ws, e, p := executionTestServer(t, path, &stubAgent{})
	req := managedRequest("task", "attempt", 1)
	managedCall(t, ws, req, 200)
	b := ws.execution
	b.mu.Lock()
	r := b.state.Records["attempt:attempt"]
	r.Receipt.State = "running"
	r.Receipt.StartedAt = time.Now().UTC()
	r.Receipt.Sequence++
	if err := b.saveLocked(); err != nil {
		t.Fatal(err)
	}
	b.mu.Unlock()
	b.lock.Close()
	newer := NewWebhookServer(0, "secret", "/hook")
	newer.RegisterEngine("project", e)
	if err := newer.EnableExecution(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { newer.execution.lock.Close() })
	managedCall(t, newer, managedRequest("task", "next", 2), 409)
	e.ReceiveMessage(p, &Message{SessionKey: req.Route.Session, Content: "/new", ReplyCtx: "reply"})
	if sent := p.getSent(); len(sent) == 0 || !strings.Contains(sent[len(sent)-1], "managed execution") {
		t.Fatal("orphan did not block normal session reset", sent)
	}
	req.Operation = "resolve"
	req.Prompt = ""
	managedCall(t, newer, req, 400)
	req.Reason = "operator killed and joined orphan runtime"
	receipt := managedCall(t, newer, req, 200)
	if receipt.State != "stopped" {
		t.Fatal(receipt)
	}
	managedCall(t, newer, managedRequest("task", "next", 2), 200)
}
func TestManagedInboxRequiresAuthenticationAndExclusiveStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.json")
	ws, _, _ := executionTestServer(t, path, &stubAgent{})
	other := NewWebhookServer(0, "secret", "/hook")
	if err := other.EnableExecution(path); err == nil {
		t.Fatal("two owners opened inbox")
	}
	noToken := NewWebhookServer(0, "", "/hook")
	if err := noToken.EnableExecution(filepath.Join(t.TempDir(), "state")); err == nil {
		t.Fatal("unauthenticated managed inbox enabled")
	}
	raw, _ := json.Marshal(managedRequest("task", "attempt", 1))
	r := httptest.NewRequest("POST", "/hook/execution?token=secret", bytes.NewReader(raw))
	w := httptest.NewRecorder()
	ws.handleExecution(w, r)
	if w.Code != 401 {
		t.Fatal("query token authenticated native operation", w.Code)
	}
}
func TestManagedInboxPersistenceFailureNeverStartsAgent(t *testing.T) {
	ws, _, _ := executionTestServer(t, filepath.Join(t.TempDir(), "inbox.json"), &stubAgent{})
	ws.execution.path = filepath.Join(t.TempDir(), "absent", "state")
	managedCall(t, ws, managedRequest("task", "attempt", 1), 503)
	if ws.execution.fault == nil {
		t.Fatal("persistence fault not retained")
	}
	ws.execution.tick()
	if len(ws.execution.active) != 0 {
		t.Fatal("execution started without durable receipt")
	}
}

// Runtime barrier stub supports checking adapter-neutral engine ownership.
var _ ExecutionTerminator = (*managedTestRuntime)(nil)

func (a *resultAgent) SupportsManagedExecution() bool { return true }
func (a *stubAgent) SupportsManagedExecution() bool   { return true }

type sendAcceptanceRuntime struct {
	*managedTestRuntime
	entered, release chan struct{}
}

func (s *sendAcceptanceRuntime) Send(prompt, id string, images []ImageAttachment, files []FileAttachment) error {
	close(s.entered)
	<-s.release
	return s.resultAgentSession.Send(prompt, id, images, files)
}
func TestManagedStartACKWaitsForRuntimeSendAcceptance(t *testing.T) {
	runtime := &sendAcceptanceRuntime{managedTestRuntime: &managedTestRuntime{resultAgentSession: newResultAgentSession("done")}, entered: make(chan struct{}), release: make(chan struct{})}
	ws, _, _ := executionTestServer(t, filepath.Join(t.TempDir(), "state.json"), &resultAgent{session: runtime})
	defer func() {
		select {
		case <-runtime.release:
		default:
			close(runtime.release)
		}
	}()
	req := managedRequest("task", "attempt", 1)
	managedCall(t, ws, req, 200)
	ws.execution.tick()
	select {
	case <-runtime.entered:
	case <-time.After(time.Second):
		t.Fatal("runtime Send never called")
	}
	req.Operation = "status"
	req.Prompt = ""
	receipt := managedCall(t, ws, req, 200)
	if receipt.State != "queued" || !receipt.StartedAt.IsZero() {
		t.Error("admission was misreported as actual start", receipt)
	}
	close(runtime.release)
	awaitManagedState(t, ws.execution, "attempt:attempt", "succeeded")
	receipt = managedCall(t, ws, req, 200)
	if receipt.StartedAt.IsZero() {
		t.Fatal("accepted Send did not produce a start ACK")
	}
}
func TestManagedStopWaitsForRuntimeJoin(t *testing.T) {
	barrier := make(chan struct{})
	runtime := &managedTestRuntime{resultAgentSession: newResultAgentSession("done"), barrier: barrier}
	ws, _, _ := executionTestServer(t, filepath.Join(t.TempDir(), "state.json"), &resultAgent{session: runtime})
	defer func() {
		select {
		case <-barrier:
		default:
			close(barrier)
		}
	}()
	req := managedRequest("task", "attempt", 1)
	managedCall(t, ws, req, 200)
	ws.execution.tick()
	awaitManagedState(t, ws.execution, "attempt:attempt", "running")
	req.Operation = "stop"
	req.Prompt = ""
	if receipt := managedCall(t, ws, req, 200); terminalExecution(receipt.State) {
		t.Fatal("stop acknowledged before runtime joined", receipt)
	}
	managedCall(t, ws, managedRequest("new-task", "next", 1), 409)
	close(barrier)
	awaitManagedState(t, ws.execution, "attempt:attempt", "stopped")
	managedCall(t, ws, managedRequest("new-task", "next", 1), 200)
}

func TestManagedNotificationDoesNotOwnConversation(t *testing.T) {
	ws, e, p := executionTestServer(t, filepath.Join(t.TempDir(), "state.json"), &stubAgent{})
	req := executionRequest{Version: 1, Operation: "notify", Route: executionRoute{Provider: "cc-connect", Target: "project", Session: "test:thread"}, NoticeID: "alert-1", Prompt: "workflow diagnostic"}
	managedCall(t, ws, req, 200)
	ws.execution.tick()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ws.execution.mu.Lock()
		sent := ws.execution.state.Records["notice:alert-1"].Sent
		ws.execution.mu.Unlock()
		if sent {
			break
		}
		time.Sleep(time.Millisecond)
	}
	managedCall(t, ws, req, 200)
	ws.execution.tick()
	e.ReceiveMessage(p, &Message{SessionKey: req.Route.Session, Content: "/new", ReplyCtx: "reply"})
	text := strings.Join(p.getSent(), "\n")
	if strings.Count(text, "workflow diagnostic") != 1 || strings.Contains(text, "managed execution owns") {
		t.Fatal("notification duplicated or quarantined the conversation", text)
	}
}
