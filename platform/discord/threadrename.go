package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/chenhg5/cc-connect/core"
)

const renameCommand = "rename-thread"
const renameButtonPrefix = "thread-title:"

type threadRenameBridge struct {
	http *http.Client
}

func newThreadRenameBridge(socket string) *threadRenameBridge {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	return &threadRenameBridge{http: &http.Client{
		Timeout: 6 * time.Minute,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socket)
		}},
	}}
}

type titleProposal struct {
	ID           string   `json:"id"`
	CurrentTitle string   `json:"current_title"`
	Titles       []string `json:"titles"`
}

type titleSelection struct {
	OldTitle string `json:"old_title"`
	NewTitle string `json:"new_title"`
}

func (b *threadRenameBridge) call(path, threadID, userID string, choice int, value any) error {
	input := struct {
		ThreadID string `json:"thread_id"`
		UserID   string `json:"user_id"`
		Choice   int    `json:"choice"`
	}{ThreadID: threadID, UserID: userID, Choice: choice}
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, "http://craftworks"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := b.http.Do(req)
	if err != nil {
		return fmt.Errorf("connect to Craftworks: %w", err)
	}
	defer res.Body.Close()
	dec := json.NewDecoder(io.LimitReader(res.Body, 16<<10))
	if res.StatusCode != http.StatusOK {
		var problem struct {
			Error string `json:"error"`
		}
		if dec.Decode(&problem) == nil && problem.Error != "" {
			return fmt.Errorf("%.300s", problem.Error)
		}
		return fmt.Errorf("Craftworks returned HTTP %d", res.StatusCode)
	}
	if err := dec.Decode(value); err != nil {
		return fmt.Errorf("invalid Craftworks response: %w", err)
	}
	return nil
}

func titleI18n(locale discordgo.Locale) *core.I18n {
	language := core.NormalizeLanguageString(string(locale))
	if language == core.LangAuto {
		base, _, _ := strings.Cut(string(locale), "-")
		language = core.NormalizeLanguageString(base)
	}
	if language == core.LangAuto {
		language = core.LangEnglish
	}
	return core.NewI18n(language)
}

func (p *Platform) suggestThreadTitles(s *discordgo.Session, i *discordgo.InteractionCreate, userID string) {
	local := titleI18n(i.Locale)
	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{Flags: discordgo.MessageFlagsEphemeral, Content: local.T(core.MsgThreadTitleLoading)},
	}); err != nil {
		slog.Warn("discord: could not acknowledge rename-thread", "error", core.RedactToken(err.Error(), i.Token))
		return
	}
	var proposal titleProposal
	if err := p.threadRename.call("/v1/discord/thread-titles", i.ChannelID, userID, 0, &proposal); err != nil {
		editThreadTitleCard(s, i, local.Tf(core.MsgThreadTitleGenerateError, err), nil)
		return
	}
	content, buttons, err := titleCard(local, proposal)
	if err != nil {
		editThreadTitleCard(s, i, local.Tf(core.MsgThreadTitleGenerateError, err), nil)
		return
	}
	editThreadTitleCard(s, i, content, buttons)
}

func titleCard(local *core.I18n, proposal titleProposal) (string, []discordgo.MessageComponent, error) {
	if len(proposal.Titles) != 3 || !validProposalID(proposal.ID) {
		return "", nil, fmt.Errorf("Craftworks returned invalid title candidates")
	}
	var content strings.Builder
	content.WriteString(local.Tf(core.MsgThreadTitleCard, literalThreadTitle(proposal.CurrentTitle)))
	var buttons []discordgo.MessageComponent
	for index, title := range proposal.Titles {
		choice := strconv.Itoa(index + 1)
		fmt.Fprintf(&content, "%s. %s\n", choice, literalThreadTitle(title))
		buttons = append(buttons, discordgo.Button{
			Label: local.Tf(core.MsgThreadTitleApply, choice), Style: discordgo.PrimaryButton,
			CustomID: renameButtonPrefix + proposal.ID + ":" + choice,
		})
	}
	buttons = append(buttons, discordgo.Button{
		Label: local.T(core.MsgThreadTitleKeep), Style: discordgo.SecondaryButton,
		CustomID: renameButtonPrefix + proposal.ID + ":0",
	})
	return content.String(), []discordgo.MessageComponent{discordgo.ActionsRow{Components: buttons}}, nil
}

// literalThreadTitle escapes Discord formatting while preserving the title shown to the user.
func literalThreadTitle(title string) string {
	var escaped strings.Builder
	escaped.Grow(len(title))
	for _, char := range title {
		switch char {
		case '\\', '`', '*', '_', '~', '|', '[', ']', '(', ')', '<', '>', '#':
			escaped.WriteByte('\\')
		}
		escaped.WriteRune(char)
	}
	return escaped.String()
}

func validProposalID(id string) bool {
	if len(id) < 26 || len(id) > 80 {
		return false
	}
	for _, c := range id {
		if (c < 'A' || c > 'Z') && (c < '2' || c > '7') {
			return false
		}
	}
	return true
}

func parseTitleChoice(customID string) (string, int, bool) {
	id, value, ok := strings.Cut(strings.TrimPrefix(customID, renameButtonPrefix), ":")
	if !ok || !validProposalID(id) || len(value) != 1 || value[0] < '0' || value[0] > '3' {
		return "", 0, false
	}
	return id, int(value[0] - '0'), true
}

func (p *Platform) chooseThreadTitle(s *discordgo.Session, i *discordgo.InteractionCreate, userID, customID string) {
	local := titleI18n(i.Locale)
	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseDeferredMessageUpdate,
	}); err != nil {
		slog.Warn("discord: could not acknowledge thread title selection", "error", core.RedactToken(err.Error(), i.Token))
		return
	}
	id, choice, ok := parseTitleChoice(customID)
	if !ok {
		editThreadTitleCard(s, i, local.T(core.MsgThreadTitleExpired), nil)
		return
	}
	var result titleSelection
	if err := p.threadRename.call("/v1/discord/thread-titles/"+id+"/choose", i.ChannelID, userID, choice, &result); err != nil {
		_, followupErr := s.FollowupMessageCreate(i.Interaction, false, &discordgo.WebhookParams{
			Content:         local.Tf(core.MsgThreadTitleApplyError, err),
			Flags:           discordgo.MessageFlagsEphemeral,
			AllowedMentions: &discordgo.MessageAllowedMentions{Parse: []discordgo.AllowedMentionType{}},
		})
		if followupErr != nil {
			slog.Warn("discord: could not report title selection error", "error", core.RedactToken(followupErr.Error(), i.Token))
		}
		return
	}
	if choice == 0 {
		editThreadTitleCard(s, i, local.T(core.MsgThreadTitleKept), nil)
		return
	}
	p.channelNameCache.Delete(i.ChannelID)
	editThreadTitleCard(s, i, local.Tf(core.MsgThreadTitleApplied, literalThreadTitle(result.OldTitle), literalThreadTitle(result.NewTitle)), nil)
}

func editThreadTitleCard(s *discordgo.Session, i *discordgo.InteractionCreate, content string, components []discordgo.MessageComponent) {
	if components == nil {
		components = []discordgo.MessageComponent{}
	}
	_, err := s.InteractionResponseEdit(i.Interaction, &discordgo.WebhookEdit{
		Content:         &content,
		Components:      &components,
		AllowedMentions: &discordgo.MessageAllowedMentions{Parse: []discordgo.AllowedMentionType{}},
	})
	if err != nil {
		slog.Warn("discord: could not update thread title card", "error", core.RedactToken(err.Error(), i.Token))
	}
}
