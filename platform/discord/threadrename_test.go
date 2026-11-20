package discord

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
)

type renameTestTransport func(*http.Request) (*http.Response, error)

func (f renameTestTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestRenameThreadCommandAndButton(t *testing.T) {
	const proposal = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var bridgePaths []string
	bridge := &threadRenameBridge{http: &http.Client{Transport: renameTestTransport(func(req *http.Request) (*http.Response, error) {
		bridgePaths = append(bridgePaths, req.URL.Path)
		var input struct {
			ThreadID string `json:"thread_id"`
			UserID   string `json:"user_id"`
			Choice   int    `json:"choice"`
		}
		if err := json.NewDecoder(req.Body).Decode(&input); err != nil || input.ThreadID != "thread-1" || input.UserID != "user-1" {
			t.Fatalf("bridge input = %+v, %v", input, err)
		}
		res := httptest.NewRecorder()
		if strings.HasSuffix(req.URL.Path, "/choose") {
			if input.Choice != 2 {
				t.Fatalf("choose = %d", input.Choice)
			}
			fmt.Fprint(res, `{"old_title":"**Old title**","new_title":"Release ||notes||"}`)
		} else {
			fmt.Fprintf(res, `{"id":"%s","current_title":"**Old title**","titles":["First choice","Release ||notes||","Third choice"]}`, proposal)
		}
		return res.Result(), nil
	})}}
	var discordCalls []string
	s, err := discordgo.New("Bot test")
	if err != nil {
		t.Fatal(err)
	}
	s.Client = &http.Client{Transport: renameTestTransport(func(req *http.Request) (*http.Response, error) {
		var payload struct {
			Type int `json:"type"`
			Data struct {
				Flags discordgo.MessageFlags `json:"flags"`
			} `json:"data"`
			Content    string `json:"content"`
			Components []struct {
				Components []struct {
					CustomID string `json:"custom_id"`
				} `json:"components"`
			} `json:"components"`
		}
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		res := httptest.NewRecorder()
		if strings.HasSuffix(req.URL.Path, "/callback") {
			discordCalls = append(discordCalls, fmt.Sprintf("callback:%d", payload.Type))
			if payload.Type == int(discordgo.InteractionResponseChannelMessageWithSource) && payload.Data.Flags&discordgo.MessageFlagsEphemeral == 0 {
				t.Error("title card response was not ephemeral")
			}
			res.WriteHeader(http.StatusNoContent)
		} else if strings.HasSuffix(req.URL.Path, "/messages/@original") {
			discordCalls = append(discordCalls, "edit")
			if strings.Contains(payload.Content, "Choose a new title") {
				if !strings.Contains(payload.Content, `\*\*Old title\*\*`) || !strings.Contains(payload.Content, `2. Release \|\|notes\|\|`) {
					t.Errorf("unexpected title text on the card %q", payload.Content)
				}
				if len(payload.Components) != 1 || len(payload.Components[0].Components) != 4 ||
					payload.Components[0].Components[1].CustomID != renameButtonPrefix+proposal+":2" {
					t.Errorf("unexpected card components %+v", payload.Components)
				}
			} else if !strings.Contains(payload.Content, `\*\*Old title\*\* → Release \|\|notes\|\|`) || len(payload.Components) != 0 {
				t.Errorf("unexpected result card %q", payload.Content)
			}
			fmt.Fprint(res, `{"id":"msg-1","channel_id":"thread-1"}`)
		} else {
			t.Errorf("unexpected Discord request %s", req.URL.Path)
			res.WriteHeader(http.StatusNotFound)
		}
		return res.Result(), nil
	})}
	p := &Platform{threadRename: bridge, allowFrom: "user-1"}
	p.handleInteraction(s, &discordgo.InteractionCreate{Interaction: &discordgo.Interaction{
		ID: "command-1", AppID: "app", Token: "token-1", ChannelID: "thread-1", Type: discordgo.InteractionApplicationCommand,
		User: &discordgo.User{ID: "user-1"}, Data: discordgo.ApplicationCommandInteractionData{Name: renameCommand},
	}})
	p.handleInteraction(s, &discordgo.InteractionCreate{Interaction: &discordgo.Interaction{
		ID: "button-1", AppID: "app", Token: "token-2", ChannelID: "thread-1", Type: discordgo.InteractionMessageComponent,
		User: &discordgo.User{ID: "user-1"}, Data: discordgo.MessageComponentInteractionData{CustomID: renameButtonPrefix + proposal + ":2"},
	}})
	if !slices.Equal(bridgePaths, []string{"/v1/discord/thread-titles", "/v1/discord/thread-titles/" + proposal + "/choose"}) ||
		!slices.Equal(discordCalls, []string{"callback:4", "edit", "callback:6", "edit"}) {
		t.Fatalf("bridge paths = %v, Discord calls = %v", bridgePaths, discordCalls)
	}
}
