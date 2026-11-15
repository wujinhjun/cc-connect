package core

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type executionRoute struct {
	Provider string `json:"provider"`
	Target   string `json:"target"`
	Session  string `json:"session"`
	Parent   string `json:"parent,omitempty"`
}
type executionIdentity struct {
	TaskID     string `json:"task_id"`
	AttemptID  string `json:"attempt_id"`
	Attempt    int    `json:"attempt"`
	Repository string `json:"repository"`
	Number     int    `json:"pull_number"`
}
type executionReceipt struct {
	TaskID      string    `json:"task_id"`
	AttemptID   string    `json:"attempt_id"`
	Sequence    int64     `json:"sequence"`
	State       string    `json:"state"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	HeartbeatAt time.Time `json:"heartbeat_at,omitempty"`
	FinishedAt  time.Time `json:"finished_at,omitempty"`
}
type executionRequest struct {
	Reason    string          `json:"reason,omitempty"`
	Version   int             `json:"version"`
	Operation string          `json:"operation"`
	Route     executionRoute  `json:"route"`
	Task      json.RawMessage `json:"task,omitempty"`
	Prompt    string          `json:"prompt,omitempty"`
	NoticeID  string          `json:"notice_id,omitempty"`
}
type executionRecord struct {
	OperatorReason string            `json:"operator_reason,omitempty"`
	Dispatching    bool              `json:"dispatching,omitempty"`
	Request        executionRequest  `json:"request"`
	Identity       executionIdentity `json:"identity"`
	Digest         string            `json:"digest"`
	Receipt        executionReceipt  `json:"receipt"`
	CreatedAt      time.Time         `json:"created_at"`
	StopRequested  bool              `json:"stop_requested,omitempty"`
	Orphaned       bool              `json:"orphaned,omitempty"`
	Error          string            `json:"error,omitempty"`
	Sent           bool              `json:"sent,omitempty"`
	RetryAt        time.Time         `json:"retry_at,omitempty"`
}
type executionState struct {
	Version int                         `json:"version"`
	Records map[string]*executionRecord `json:"records"`
}
type executionHold struct {
	session *Session
	gen     uint64
}

type liveExecution struct {
	cancel func()
	once   sync.Once
}
type executionInbox struct {
	holds     map[string]executionHold
	ws        *WebhookServer
	path      string
	lock      *os.File
	mu        sync.Mutex
	state     executionState
	fault     error
	active    map[string]*liveExecution
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	startOnce sync.Once
	closeOnce sync.Once
}

var errExecutionConflict = errors.New("execution identity or ownership conflict")

// EnableExecution is opt-in. The inbox requires private storage and mandatory
// Bearer authentication, independent of legacy webhook authentication settings.
func (ws *WebhookServer) EnableExecution(path string) error {
	if ws.token == "" {
		return fmt.Errorf("managed execution requires a webhook token")
	}
	if ws.execution != nil {
		return fmt.Errorf("managed execution already enabled")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("execution directory must be private")
	}
	lock, err := lockExecutionFile(path + ".lock")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &executionInbox{ws: ws, path: path, lock: lock, ctx: ctx, cancel: cancel, active: make(map[string]*liveExecution), holds: make(map[string]executionHold), state: executionState{Version: 1, Records: make(map[string]*executionRecord)}}
	raw, err := os.ReadFile(path)
	if err == nil {
		info, err = os.Lstat(path)
		if err == nil && (!info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0) {
			err = fmt.Errorf("execution state must be a private regular file")
		}
		if err == nil {
			err = json.Unmarshal(raw, &b.state)
		}
		if err == nil && (b.state.Version != 1 || b.state.Records == nil) {
			err = fmt.Errorf("invalid execution state")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		cancel()
		lock.Close()
		return err
	}
	for _, r := range b.state.Records {
		if r == nil || r.Request.Version != 1 || (r.Request.Operation != "notify" && (r.Identity.AttemptID == "" || r.Receipt.TaskID != r.Identity.TaskID || r.Receipt.AttemptID != r.Identity.AttemptID)) {
			cancel()
			lock.Close()
			return fmt.Errorf("invalid execution record")
		}
		if r.Receipt.State == "running" || r.Dispatching {
			r.Orphaned = true
			r.Error = "previous runtime must be verified stopped by an operator"
		}
	}
	if err = b.saveLocked(); err != nil {
		cancel()
		lock.Close()
		return err
	}
	ws.execution = b
	for name, e := range ws.engines {
		b.installGuard(name, e)
	}
	return nil
}
func (b *executionInbox) saveLocked() error {
	if b.fault != nil {
		return b.fault
	}
	raw, err := json.Marshal(b.state)
	var f *os.File
	if err == nil {
		f, err = os.OpenFile(b.path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	}
	if err == nil {
		_, err = f.Write(raw)
		if err == nil {
			err = f.Sync()
		}
		if e := f.Close(); err == nil {
			err = e
		}
	}
	if err == nil {
		err = os.Rename(b.path+".tmp", b.path)
	}
	if err == nil {
		var d *os.File
		d, err = os.Open(filepath.Dir(b.path))
		if err == nil {
			err = d.Sync()
			d.Close()
		}
	}
	if err != nil {
		b.fault = err
		slog.Error("execution persistence failed; admission disabled", "error", err)
	}
	return err
}
func terminalExecution(s string) bool { return s == "succeeded" || s == "failed" || s == "stopped" }
func requestDigest(req executionRequest) string {
	req.Reason = ""
	req.Operation = ""
	req.Prompt = "" // stop/status do not repeat the rendered prompt
	raw, _ := json.Marshal(req)
	var value any
	_ = json.Unmarshal(raw, &value)
	raw, _ = json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func parseExecution(req executionRequest) (executionIdentity, error) {
	var d executionIdentity
	if err := json.Unmarshal(req.Task, &d); err != nil {
		return d, err
	}
	if d.TaskID == "" || d.AttemptID == "" || d.Attempt < 1 || d.Repository == "" || d.Number < 1 {
		return d, fmt.Errorf("missing execution identity")
	}
	return d, nil
}
func executionKey(req executionRequest, d executionIdentity) string {
	if req.Operation == "notify" {
		return "notice:" + req.NoticeID
	}
	return "attempt:" + d.AttemptID
}
func (b *executionInbox) admit(req executionRequest) (*executionRecord, error) {
	d := executionIdentity{}
	var err error
	if req.Operation != "notify" {
		d, err = parseExecution(req)
		if err != nil {
			return nil, err
		}
	}
	key := executionKey(req, d)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fault != nil {
		return nil, b.fault
	}
	if r := b.state.Records[key]; r != nil {
		if r.Digest != requestDigest(req) || (req.Operation == "deliver" && r.Request.Prompt != "" && req.Prompt != r.Request.Prompt) {
			return nil, errExecutionConflict
		}
		return r, nil
	}
	if req.Operation == "status" {
		return nil, os.ErrNotExist
	}
	if req.Operation == "deliver" {
		queued := 0
		for _, r := range b.state.Records {
			if r.Request.Operation == "notify" {
				continue
			}
			if r.Identity.TaskID == d.TaskID && r.Identity.Attempt >= d.Attempt {
				return nil, errExecutionConflict
			}
			if !terminalExecution(r.Receipt.State) {
				queued++
				if strings.EqualFold(r.Identity.Repository, d.Repository) && r.Identity.Number == d.Number {
					return nil, errExecutionConflict
				}
			}
		}
		if queued >= 256 {
			return nil, fmt.Errorf("execution inbox capacity reached")
		}
	}
	now := time.Now().UTC()
	r := &executionRecord{Request: req, Identity: d, Digest: requestDigest(req), CreatedAt: now, Receipt: executionReceipt{TaskID: d.TaskID, AttemptID: d.AttemptID, Sequence: 1, State: "queued", HeartbeatAt: now}}
	if req.Operation == "stop" || req.Operation == "resolve" {
		r.StopRequested = true
		if req.Operation == "resolve" {
			r.OperatorReason = req.Reason
		}
		r.Receipt.State = "stopped"
		r.Receipt.FinishedAt = now
	}
	b.state.Records[key] = r
	if err = b.saveLocked(); err != nil {
		return nil, err
	}
	return r, nil
}
func (ws *WebhookServer) handleExecution(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	expected := "Bearer " + ws.token
	if ws.token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(expected)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}
	b := ws.execution
	if b == nil {
		http.Error(w, "managed execution is disabled", 503)
		return
	}
	var req executionRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid execution request", 400)
		return
	}
	if dec.Decode(new(any)) != io.EOF || req.Version != 1 || req.Route.Provider != "cc-connect" || req.Route.Target == "" || ((req.Operation == "deliver" || req.Operation == "notify") && req.Route.Session == "") {
		http.Error(w, "invalid execution envelope", 400)
		return
	}
	if req.Operation != "deliver" && req.Operation != "status" && req.Operation != "stop" && req.Operation != "resolve" && req.Operation != "notify" {
		http.Error(w, "unknown operation", 400)
		return
	}
	if (req.Operation == "deliver" || req.Operation == "notify") && strings.TrimSpace(req.Prompt) == "" {
		http.Error(w, "prompt required", 400)
		return
	}
	if req.Operation == "notify" && req.NoticeID == "" {
		http.Error(w, "notice_id required", 400)
		return
	}
	engine, engineErr := ws.resolveEngine(req.Route.Target)
	if engineErr != nil {
		http.Error(w, "unknown execution target", 400)
		return
	}
	if req.Operation == "resolve" && strings.TrimSpace(req.Reason) == "" {
		http.Error(w, "operator termination reason required", 400)
		return
	}
	if req.Operation == "deliver" {
		capability, ok := engine.agent.(ManagedExecutionAgent)
		if !ok || !capability.SupportsManagedExecution() {
			http.Error(w, "agent has no confirmed execution termination capability", 409)
			return
		}
	}
	record, err := b.admit(req)
	if err != nil {
		status := 409
		if req.Operation == "status" && errors.Is(err, os.ErrNotExist) {
			status = 404
		}
		b.mu.Lock()
		faulted := b.fault != nil
		b.mu.Unlock()
		if faulted {
			status = 503
		}
		http.Error(w, err.Error(), status)
		return
	}
	response, err := b.control(req, record)
	if err != nil {
		status := 503
		if errors.Is(err, errExecutionConflict) {
			status = 409
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (b *executionInbox) control(req executionRequest, record *executionRecord) (map[string]any, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var err error
	if req.Operation == "stop" && !terminalExecution(record.Receipt.State) {
		record.StopRequested = true
		if record.Receipt.State == "queued" && !record.Dispatching {
			record.Receipt.Sequence++
			record.Receipt.State = "stopped"
			record.Receipt.FinishedAt = time.Now().UTC()
		}
		err = b.saveLocked()
	}
	if req.Operation == "resolve" && !terminalExecution(record.Receipt.State) {
		key := executionKey(req, record.Identity)
		if b.active[key] != nil {
			return nil, fmt.Errorf("%w: runtime is still active; stop it before manual resolution", errExecutionConflict)
		}
		record.Receipt.Sequence++
		record.Receipt.State = "stopped"
		record.Receipt.FinishedAt = time.Now().UTC()
		record.Error = ""
		record.OperatorReason = req.Reason
		record.Orphaned = false
		record.Dispatching = false
		err = b.saveLocked()
		if err == nil {
			if hold, ok := b.holds[key]; ok {
				hold.session.releaseManaged(hold.gen)
				delete(b.holds, key)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	live := b.active[executionKey(req, record.Identity)]
	if req.Operation == "stop" && live != nil && live.cancel != nil {
		go live.once.Do(live.cancel)
	}
	return map[string]any{"status": "accepted", "receipt": record.Receipt, "error": record.Error}, nil
}

func (b *executionInbox) start() {
	b.startOnce.Do(func() {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				b.tick()
				select {
				case <-b.ctx.Done():
					return
				case <-t.C:
				}
			}
		}()
	})
}
func (b *executionInbox) close() {
	b.closeOnce.Do(func() { b.cancel(); go func() { b.wg.Wait(); _ = b.lock.Close() }() })
}
func (b *executionInbox) tick() {
	b.mu.Lock()
	if b.fault != nil || b.ctx.Err() != nil {
		b.mu.Unlock()
		return
	}
	keys := make([]string, 0, len(b.state.Records))
	for key := range b.state.Records {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return b.state.Records[keys[i]].CreatedAt.Before(b.state.Records[keys[j]].CreatedAt)
	})
	for _, key := range keys {
		r := b.state.Records[key]
		if terminalExecution(r.Receipt.State) || r.Orphaned || b.active[key] != nil || r.Sent || time.Now().Before(r.RetryAt) {
			continue
		}
		if r.Request.Operation == "notify" {
			b.active[key] = &liveExecution{}
			b.wg.Add(1)
			go b.sendNotice(key, r.Request)
			continue
		}
		if r.Receipt.State != "queued" {
			continue
		}
		e, err := b.ws.resolveEngine(r.Request.Route.Target)
		if err != nil {
			continue
		}
		p, rc, err := executionTarget(e, r.Request.Route.Session)
		if err != nil {
			r.Error = err.Error()
			continue
		}
		msg := &Message{SessionKey: r.Request.Route.Session, Platform: p.Name(), UserID: "webhook", UserName: "webhook", Content: r.Request.Prompt, ReplyCtx: rc}
		agent, sessions, iKey, err := e.commandContext(p, msg)
		if err != nil {
			r.Error = err.Error()
			continue
		}
		session := sessions.GetOrCreateActive(msg.SessionKey)
		// Orphaned attempts quarantine their session even if /new changed its ID.
		quarantined := false
		for _, old := range b.state.Records {
			if old.Orphaned && !terminalExecution(old.Receipt.State) && old.Request.Route.Target == r.Request.Route.Target && old.Request.Route.Session == r.Request.Route.Session {
				quarantined = true
			}
		}
		if quarantined {
			continue
		}
		e.closingMu.Lock()
		unsafe := e.unsafeResume[iKey] || e.closingSessions[iKey] != nil
		e.closingMu.Unlock()
		if unsafe {
			r.Error = "previous session termination unresolved"
			continue
		}
		gen, ok := session.tryManagedLock()
		if !ok {
			continue
		}
		now := time.Now().UTC()
		r.Receipt.Sequence++
		r.Error = ""
		r.Dispatching = true
		r.Receipt.State = "queued"
		r.Receipt.HeartbeatAt = now
		if err = b.saveLocked(); err != nil {
			session.releaseManaged(gen)
			break
		}
		b.holds[key] = executionHold{session: session, gen: gen}
		b.active[key] = &liveExecution{cancel: func() { e.stopInteractiveSessionSilently(iKey) }}
		b.wg.Add(1)
		go b.run(key, e, p, msg, agent, sessions, iKey, session, gen)
	}
	b.heartbeatLocked()
	b.mu.Unlock()
}

func (b *executionInbox) heartbeatLocked() {
	now := time.Now().UTC()
	changed := false
	for key, r := range b.state.Records {
		if r.Request.Operation != "notify" && (r.Receipt.State == "queued" || b.active[key] != nil) && !r.Orphaned && !terminalExecution(r.Receipt.State) && now.Sub(r.Receipt.HeartbeatAt) >= 15*time.Second {
			r.Receipt.HeartbeatAt = now
			r.Receipt.Sequence++
			changed = true
		}
	}
	if changed {
		_ = b.saveLocked()
	}
}
func executionTarget(e *Engine, key string) (Platform, any, error) {
	name, _, _ := strings.Cut(key, ":")
	for _, p := range e.platforms {
		if p.Name() == name {
			if rc, ok := p.(ReplyContextReconstructor); ok {
				target, err := rc.ReconstructReplyCtx(key)
				return p, target, err
			}
		}
	}
	return nil, nil, fmt.Errorf("execution target cannot reconstruct replies")
}
func (b *executionInbox) run(key string, e *Engine, p Platform, msg *Message, agent Agent, sessions *SessionManager, iKey string, session *Session, gen uint64) {
	defer b.wg.Done()
	// Capture ownership before entering the normal engine loop. This pointer stays
	// valid if /stop removes the interactive state while the turn is running.
	state := e.getOrCreateInteractiveStateWith(iKey, p, msg.ReplyCtx, session, sessions, agent, msg.SessionKey)
	state.mu.Lock()
	runtime := state.agentSession
	observer := &managedSendObserver{done: make(chan struct{}), start: func() error {
		b.mu.Lock()
		defer b.mu.Unlock()
		r := b.state.Records[key]
		r.Receipt.State = "running"
		r.Receipt.StartedAt = time.Now().UTC()
		r.Receipt.HeartbeatAt = r.Receipt.StartedAt
		r.Receipt.Sequence++
		return b.saveLocked()
	}}
	state.managedSend = observer
	state.mu.Unlock()
	b.mu.Lock()
	stopping := b.state.Records[key].StopRequested
	b.mu.Unlock()
	if runtime != nil {
		if preparer, ok := runtime.(ExecutionPreparer); ok {
			if err := preparer.BeginExecution(); err != nil {
				b.uncertain(key, err.Error())
				return
			}
		}
	}
	if !stopping {
		e.processInteractiveMessageWithState(p, msg, session, agent, sessions, iKey, "", msg.SessionKey, gen, state)
	}
	if observer.invoked.Load() {
		select {
		case <-observer.done:
		case <-time.After(30 * time.Second):
			b.uncertain(key, "agent Send has not returned; termination is unconfirmed")
			return
		}
	}
	if runtime != nil {
		terminator, ok := runtime.(ExecutionTerminator)
		if !ok {
			b.uncertain(key, "runtime does not support confirmed termination")
			return
		}
		// Stop the idle reader before closing its event source.
		e.stopUnsolicitedReader(state)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := terminator.TerminateExecution(ctx)
		cancel()
		if err != nil {
			b.uncertain(key, "runtime termination not confirmed: "+err.Error())
			return
		}
	}
	state.markStopped()
	e.interactiveMu.Lock()
	if e.interactiveStates[iKey] == state {
		delete(e.interactiveStates, iKey)
	}
	e.interactiveMu.Unlock()
	b.finish(key, session, gen, runtime != nil)
}

func (b *executionInbox) finish(key string, session *Session, gen uint64, hasRuntime bool) {
	b.mu.Lock()
	r := b.state.Records[key]
	r.Receipt.Sequence++
	r.Receipt.FinishedAt = time.Now().UTC()
	r.Receipt.State = "succeeded"
	if !hasRuntime || r.Receipt.StartedAt.IsZero() {
		r.Receipt.State = "stopped"
	}
	r.Dispatching = false
	r.Error = ""
	if r.StopRequested {
		r.Receipt.State = "stopped"
	}
	err := b.saveLocked()
	delete(b.active, key)
	if err == nil {
		delete(b.holds, key)
	}
	b.mu.Unlock()
	if err == nil {
		session.releaseManaged(gen)
	}
}
func (b *executionInbox) uncertain(key, reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.state.Records[key]
	r.Orphaned = true
	r.Error = reason
	delete(b.active, key)
	_ = b.saveLocked()
	slog.Error("execution requires operator termination confirmation", "attempt", r.Identity.AttemptID, "reason", reason)
}
func (b *executionInbox) sendNotice(key string, req executionRequest) {
	defer b.wg.Done()
	e, err := b.ws.resolveEngine(req.Route.Target)
	if err == nil {
		err = e.SendToSession(req.Route.Session, req.Prompt)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.state.Records[key]
	if err == nil {
		r.Sent = true
		r.Error = ""
	} else {
		r.Error = err.Error()
		r.RetryAt = time.Now().Add(30 * time.Second)
	}
	delete(b.active, key)
	_ = b.saveLocked()
}

func (b *executionInbox) installGuard(name string, e *Engine) {
	e.executionGuardMu.Lock()
	defer e.executionGuardMu.Unlock()
	e.executionGuard = func(msg *Message) bool {
		command := ""
		if fields := strings.Fields(msg.Content); len(fields) > 0 {
			command = fields[0]
		}
		if command == "/status" || command == "/history" || command == "/stop" || command == "/allow" || command == "/deny" || command == "/ps" || strings.HasPrefix(command, "askq:") {
			return false
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		destructive := command == "/new" || command == "/switch" || command == "/cancel" || command == "/provider" || command == "/agent"
		for _, r := range b.state.Records {
			if r.Request.Operation == "notify" {
				continue
			}
			if r.Request.Route.Target == name && r.Request.Route.Session == msg.SessionKey && !terminalExecution(r.Receipt.State) && (r.Orphaned || r.Dispatching || r.Receipt.State == "running" || destructive) {
				return true
			}
		}
		return false
	}
}

type managedSendObserver struct {
	invoked atomic.Bool
	done    chan struct{}
	start   func() error
}
