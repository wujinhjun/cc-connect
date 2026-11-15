// A local, model-free fixture for Ink Commons' cross-repository integration test.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type platform struct{}

func (*platform) Name() string                                { return "discord" }
func (*platform) Start(core.MessageHandler) error             { return nil }
func (*platform) Stop() error                                 { return nil }
func (*platform) Reply(context.Context, any, string) error    { return nil }
func (*platform) Send(context.Context, any, string) error     { return nil }
func (*platform) ReconstructReplyCtx(key string) (any, error) { return key, nil }

type agent struct {
	role, upstream string
	busy           time.Duration
}

func (*agent) Name() string                                                  { return "fixture" }
func (*agent) Stop() error                                                   { return nil }
func (*agent) ListSessions(context.Context) ([]core.AgentSessionInfo, error) { return nil, nil }
func (*agent) SupportsManagedExecution() bool                                { return true }
func (a *agent) StartSession(ctx context.Context, _ string) (core.AgentSession, error) {
	return &runtime{agent: a, events: make(chan core.Event, 8), ctx: ctx}, nil
}

type runtime struct {
	agent  *agent
	events chan core.Event
	ctx    context.Context
	mu     sync.Mutex
	closed bool
}

var commit = regexp.MustCompile(`Commit: ([a-f0-9]{40})`)

func (r *runtime) Send(prompt, _ string, _ []core.ImageAttachment, _ []core.FileAttachment) error {
	if prompt == "hold" {
		select {
		case <-time.After(r.agent.busy):
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	} else {
		match := commit.FindStringSubmatch(prompt)
		if len(match) != 2 {
			return fmt.Errorf("missing commit in prompt")
		}
		body, _ := json.Marshal(map[string]string{"role": r.agent.role, "sha": match[1], "prompt": prompt})
		res, err := http.Post(r.agent.upstream+"/fixture/finish", "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			return fmt.Errorf("fixture callback returned %d", res.StatusCode)
		}
	}
	r.events <- core.Event{Type: core.EventResult, Content: "fixture turn complete", Done: true}
	return nil
}
func (*runtime) RespondPermission(string, core.PermissionResult) error { return nil }
func (r *runtime) Events() <-chan core.Event                           { return r.events }
func (r *runtime) CurrentSessionID() string                            { return "fixture-" + r.agent.role }
func (r *runtime) Alive() bool                                         { r.mu.Lock(); defer r.mu.Unlock(); return !r.closed }
func (r *runtime) Close() error                                        { r.mu.Lock(); r.closed = true; r.mu.Unlock(); return nil }
func (r *runtime) TerminateExecution(context.Context) error            { return r.Close() }
func main() {
	port := flag.Int("port", 0, "")
	state := flag.String("state", "", "")
	upstream := flag.String("upstream", "", "")
	busy := flag.Duration("busy", 7*time.Second, "")
	flag.Parse()
	if *port <= 0 || *state == "" || !strings.HasPrefix(*upstream, "http://127.0.0.1:") {
		fmt.Fprintln(os.Stderr, "local test fixture arguments required")
		os.Exit(2)
	}
	ws := core.NewWebhookServer(*port, "fixture-secret", "/hook")
	engines := []*core.Engine{}
	for _, role := range []string{"reviewer", "developer"} {
		p := &platform{}
		a := &agent{role: role, upstream: *upstream, busy: *busy}
		e := core.NewEngine(role, a, []core.Platform{p}, "", core.LangEnglish)
		engines = append(engines, e)
		ws.RegisterEngine(role, e)
	}
	if err := ws.EnableExecution(*state); err != nil {
		panic(err)
	}
	for _, e := range engines {
		if err := e.Start(); err != nil {
			panic(err)
		}
	}
	engines[1].ReceiveMessage(&platform{}, &core.Message{SessionKey: "discord:101", Content: "hold", ReplyCtx: "discord:101", UserID: "fixture-user"})
	ws.Start()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, os.Interrupt)
	<-ch
	for _, e := range engines {
		_ = e.Stop()
	}
	ws.Stop()
}
