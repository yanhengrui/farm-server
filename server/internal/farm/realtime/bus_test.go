package realtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
)

type submitterFunc func(context.Context, domain.Command) (application.CommitResult, error)

func (f submitterFunc) Submit(ctx context.Context, cmd domain.Command) (application.CommitResult, error) {
	return f(ctx, cmd)
}

type recordingPublisher struct {
	messages []Message
	err      error
}

func (p *recordingPublisher) Publish(_ context.Context, message Message) error {
	p.messages = append(p.messages, message)
	return p.err
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPublishingSubmitterPublishesOnlyCommittedNonReplay(t *testing.T) {
	result := application.CommitResult{NewVersion: 4, EventID: "event-4", Patch: domain.Patch{FarmID: 7, Version: 4}}
	next := submitterFunc(func(context.Context, domain.Command) (application.CommitResult, error) { return result, nil })
	publisher := &recordingPublisher{}
	s := NewPublishingSubmitter(next, publisher, discardLogger())
	got, err := s.Submit(context.Background(), domain.Command{FarmID: 7, ActorUser: 9, Type: domain.CmdPetAutoHarvest})
	if err != nil || got.NewVersion != 4 {
		t.Fatalf("result=%+v err=%v", got, err)
	}
	if len(publisher.messages) != 1 || publisher.messages[0].FarmVersion != 4 || publisher.messages[0].ActorUserID != 9 || publisher.messages[0].CommandType != domain.CmdPetAutoHarvest {
		t.Fatalf("messages=%+v", publisher.messages)
	}
}

func TestPublishingFailureDoesNotRollbackSuccess(t *testing.T) {
	result := application.CommitResult{NewVersion: 2, EventID: "event-2"}
	next := submitterFunc(func(context.Context, domain.Command) (application.CommitResult, error) { return result, nil })
	s := NewPublishingSubmitter(next, &recordingPublisher{err: errors.New("redis down")}, discardLogger())
	got, err := s.Submit(context.Background(), domain.Command{FarmID: 1})
	if err != nil || got.NewVersion != 2 {
		t.Fatalf("successful commit changed by publish failure: result=%+v err=%v", got, err)
	}
}

func TestPublishingSubmitterSkipsReplay(t *testing.T) {
	next := submitterFunc(func(context.Context, domain.Command) (application.CommitResult, error) {
		return application.CommitResult{Replayed: true}, nil
	})
	publisher := &recordingPublisher{}
	s := NewPublishingSubmitter(next, publisher, discardLogger())
	_, _ = s.Submit(context.Background(), domain.Command{FarmID: 1})
	if len(publisher.messages) != 0 {
		t.Fatalf("replay published: %+v", publisher.messages)
	}
}
