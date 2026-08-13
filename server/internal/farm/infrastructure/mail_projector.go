// Package infrastructure 提供邮件投影器 MailProjector。
// MailProjector 订阅 farm.stolen.v1 和 social.friend_accepted.v1 事件，
// 通过 mailrpc.Client 向目标用户发送站内信通知。
package infrastructure

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
	socialevents "github.com/photon/farm-server/server/contracts/events/socialv1"
	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	"github.com/photon/farm-server/server/internal/mail/transport/mailrpc"
)

// MailProjector 消费农场和社交事件，向相关用户发送站内信。
// 实现 farm/infrastructure.EventProjector 接口。
type MailProjector struct {
	mail *mailrpc.Client
	log  *slog.Logger
}

// NewMailProjector 构造 MailProjector。mail 是 gamesvr mailrpc 客户端。
func NewMailProjector(mail *mailrpc.Client, log *slog.Logger) *MailProjector {
	return &MailProjector{mail: mail, log: log}
}

// Name 返回用于去重的稳定消费者名称。
func (p *MailProjector) Name() string { return "mail-projector" }

// Handle 根据事件类型发送相应的站内信通知。
// 未知事件类型静默跳过，不返回错误（避免阻塞 Relay）。
func (p *MailProjector) Handle(ctx context.Context, env farmevents.EventEnvelope) error {
	switch env.EventType {
	case farmevents.EventTypeFarmStolen:
		return p.handleStolen(ctx, env)
	case farmevents.EventType(socialevents.EventTypeFriendAccepted):
		return p.handleFriendAccepted(ctx, env)
	default:
		// 不关心的事件类型静默跳过。
		return nil
	}
}

// handleStolen 消费 farm.stolen.v1：向农场主发偷菜通知邮件。
func (p *MailProjector) handleStolen(ctx context.Context, env farmevents.EventEnvelope) error {
	raw, err := json.Marshal(env.Payload)
	if err != nil {
		return fmt.Errorf("marshal stolen payload: %w", err)
	}
	var payload farmevents.FarmStolenPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("unmarshal stolen payload: %w", err)
	}

	ownerID, err := strconv.ParseInt(payload.OwnerUserID, 10, 64)
	if err != nil {
		return fmt.Errorf("parse owner_user_id: %w", err)
	}
	actorID, err := strconv.ParseInt(payload.ActorUserID, 10, 64)
	if err != nil {
		return fmt.Errorf("parse actor_user_id: %w", err)
	}

	content := fmt.Sprintf("玩家 %s 偷走了你农场第 %d 块地的 %s（共 %d 个）。",
		payload.ActorDisplayName, payload.PlotID, payload.CropID, payload.StolenAmount)

	_, err = p.mail.SendMail(ctx, maildomain.SendMailReq{
		UserID:   ownerID,
		SenderID: &actorID,
		MailType: maildomain.MailTypeStealNotify,
		Title:    "你的作物被偷了",
		Content:  content,
	})
	if err != nil {
		p.log.Error("send steal notify mail failed",
			slog.String("event_id", env.EventID),
			slog.Int64("owner_id", ownerID),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("send steal notify: %w", err)
	}
	return nil
}

// handleFriendAccepted 消费 social.friend_accepted.v1：向双方各发好友建立通知邮件。
func (p *MailProjector) handleFriendAccepted(ctx context.Context, env farmevents.EventEnvelope) error {
	raw, err := json.Marshal(env.Payload)
	if err != nil {
		return fmt.Errorf("marshal friend_accepted payload: %w", err)
	}
	var payload socialevents.FriendAcceptedPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("unmarshal friend_accepted payload: %w", err)
	}

	inviterID, err := strconv.ParseInt(payload.InviterID, 10, 64)
	if err != nil {
		return fmt.Errorf("parse inviter_id: %w", err)
	}
	accepterID, err := strconv.ParseInt(payload.AccepterID, 10, 64)
	if err != nil {
		return fmt.Errorf("parse accepter_id: %w", err)
	}

	// 向邀请者发通知：对方接受了你的邀请。
	if _, err := p.mail.SendMail(ctx, maildomain.SendMailReq{
		UserID:   inviterID,
		SenderID: &accepterID,
		MailType: maildomain.MailTypeFriendAccepted,
		Title:    "好友邀请已被接受",
		Content:  fmt.Sprintf("玩家 %s 接受了你的好友邀请，现在你们是好友了！", payload.AccepterDisplayName),
	}); err != nil {
		p.log.Error("send friend_accepted mail to inviter failed",
			slog.String("event_id", env.EventID),
			slog.Int64("inviter_id", inviterID),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("send friend accepted to inviter: %w", err)
	}

	// 向接受者发通知：你们成为好友了。
	if _, err := p.mail.SendMail(ctx, maildomain.SendMailReq{
		UserID:   accepterID,
		SenderID: &inviterID,
		MailType: maildomain.MailTypeFriendAccepted,
		Title:    "好友添加成功",
		Content:  fmt.Sprintf("你已成功添加玩家 %s 为好友！", payload.InviterDisplayName),
	}); err != nil {
		p.log.Error("send friend_accepted mail to accepter failed",
			slog.String("event_id", env.EventID),
			slog.Int64("accepter_id", accepterID),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("send friend accepted to accepter: %w", err)
	}

	return nil
}
