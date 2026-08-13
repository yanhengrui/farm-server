// Package infrastructure 提供 mail 领域的 MySQL 实现。
package infrastructure

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/mysqlretry"
)

// MySQLMailService 实现 mail/domain.MailService。
type MySQLMailService struct {
	db       *sql.DB
	notifier maildomain.MailboxNotifier
}

// NewMySQLMailService 构造 MySQLMailService。
func NewMySQLMailService(db *sql.DB) *MySQLMailService {
	return &MySQLMailService{db: db}
}

// WithNotifier enables best-effort online badge updates after authoritative
// MySQL transactions commit. A notification failure never rolls back mail.
func (s *MySQLMailService) WithNotifier(notifier maildomain.MailboxNotifier) *MySQLMailService {
	s.notifier = notifier
	return s
}

// SendMail 在单事务内写入 mails + mail_attachments。
func (s *MySQLMailService) SendMail(ctx context.Context, req maildomain.SendMailReq) (int64, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin_tx send_mail: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now().UTC()
	const qMail = `
		INSERT INTO mails (user_id, sender_id, mail_type, title, content, status, expires_at, created_at)
		VALUES (?, ?, ?, ?, ?, 'UNREAD', ?, ?)`
	res, err := tx.ExecContext(ctx, qMail,
		uint64(req.UserID), nullInt64(req.SenderID),
		string(req.MailType), req.Title, req.Content,
		nullTime(req.ExpiresAt), now,
	)
	if err != nil {
		return 0, fmt.Errorf("insert_mail: %w", err)
	}
	mailID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last_insert_id: %w", err)
	}

	for _, att := range req.Attachments {
		const qAtt = `INSERT INTO mail_attachments (mail_id, item_type, item_id, quantity) VALUES (?, ?, ?, ?)`
		if _, err := tx.ExecContext(ctx, qAtt,
			uint64(mailID), att.ItemType, uint64(att.ItemID), uint64(att.Quantity),
		); err != nil {
			return 0, fmt.Errorf("insert_attachment: %w", err)
		}
	}

	// The mails INSERT trigger owns unread-count maintenance so old gamesvr
	// binaries remain compatible throughout a rolling release.
	summary, err := loadSummaryTx(ctx, tx, req.UserID)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	s.notify(ctx, summary)
	return mailID, nil
}

// GetSummary returns the authoritative mailbox badge state. Users without any
// mail do not need a mailbox_states row and read as version/count zero.
func (s *MySQLMailService) GetSummary(ctx context.Context, userID int64) (maildomain.MailboxSummary, error) {
	summary := maildomain.MailboxSummary{UserID: userID}
	err := s.db.QueryRowContext(ctx,
		`SELECT unread_count, version FROM mailbox_states WHERE user_id=?`,
		uint64(userID),
	).Scan(&summary.UnreadCount, &summary.Version)
	if err == sql.ErrNoRows {
		return summary, nil
	}
	if err != nil {
		return maildomain.MailboxSummary{}, fmt.Errorf("get mailbox summary: %w", err)
	}
	return summary, nil
}

// ListMails 返回用户未删除邮件（含附件），按 created_at 降序，最多 limit 封。
func (s *MySQLMailService) ListMails(ctx context.Context, userID int64, limit int) ([]maildomain.Mail, error) {
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	const qMails = `
		SELECT mail_id, sender_id, mail_type, title, content, status, read_at, expires_at, created_at
		FROM mails
		WHERE user_id = ? AND status != 'DELETED'
		ORDER BY created_at DESC
		LIMIT ?`
	rows, err := s.db.QueryContext(ctx, qMails, uint64(userID), limit)
	if err != nil {
		return nil, fmt.Errorf("list_mails: %w", err)
	}
	defer rows.Close()

	var mails []maildomain.Mail
	var mailIDs []uint64
	for rows.Next() {
		var m maildomain.Mail
		var senderID sql.NullInt64
		var readAt, expiresAt sql.NullTime
		if err := rows.Scan(&m.MailID, &senderID, &m.MailType, &m.Title, &m.Content,
			&m.Status, &readAt, &expiresAt, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan_mail: %w", err)
		}
		m.UserID = userID
		if senderID.Valid {
			v := senderID.Int64
			m.SenderID = &v
		}
		if readAt.Valid {
			v := readAt.Time
			m.ReadAt = &v
		}
		if expiresAt.Valid {
			v := expiresAt.Time
			m.ExpiresAt = &v
		}
		mails = append(mails, m)
		mailIDs = append(mailIDs, uint64(m.MailID))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(mailIDs) == 0 {
		return mails, nil
	}

	// 批量加载附件（IN 查询），避免 N+1。
	attMap, err := s.loadAttachments(ctx, mailIDs)
	if err != nil {
		return nil, err
	}
	for i := range mails {
		mails[i].Attachments = attMap[uint64(mails[i].MailID)]
	}
	return mails, nil
}

// loadAttachments 批量按 mail_id 加载附件（IN 参数手动展开）。
func (s *MySQLMailService) loadAttachments(ctx context.Context, mailIDs []uint64) (map[uint64][]maildomain.Attachment, error) {
	if len(mailIDs) == 0 {
		return nil, nil
	}
	// 手动构造 IN (?,?,?...) 占位符。
	args := make([]any, len(mailIDs))
	placeholders := make([]byte, 0, len(mailIDs)*3)
	for i, id := range mailIDs {
		args[i] = id
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
	}
	q := "SELECT attachment_id, mail_id, item_type, item_id, quantity, claimed_at FROM mail_attachments WHERE mail_id IN (" + string(placeholders) + ")"
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("load_attachments: %w", err)
	}
	defer rows.Close()

	result := make(map[uint64][]maildomain.Attachment)
	for rows.Next() {
		var a maildomain.Attachment
		var claimedAt sql.NullTime
		if err := rows.Scan(&a.AttachmentID, &a.MailID, &a.ItemType, &a.ItemID, &a.Quantity, &claimedAt); err != nil {
			return nil, fmt.Errorf("scan_attachment: %w", err)
		}
		if claimedAt.Valid {
			v := claimedAt.Time
			a.ClaimedAt = &v
		}
		result[uint64(a.MailID)] = append(result[uint64(a.MailID)], a)
	}
	return result, rows.Err()
}

// ClaimAttachment 领取附件：幂等检查 → 写入库存/钱包 → 标记 claimed_at（单事务）。
func (s *MySQLMailService) ClaimAttachment(ctx context.Context, attachmentID int64, userID int64) error {
	return mysqlretry.Do(ctx, mysqlretry.IsTransient, func() error {
		return s.claimAttachmentOnce(ctx, attachmentID, userID)
	})
}

func (s *MySQLMailService) claimAttachmentOnce(ctx context.Context, attachmentID int64, userID int64) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin_tx claim: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// 1. 加锁读取附件，同时校验所属邮件归属当前用户。
	const qLoad = `
		SELECT a.item_type, a.item_id, a.quantity, a.claimed_at
		FROM mail_attachments a
		JOIN mails m ON m.mail_id = a.mail_id
		WHERE a.attachment_id = ? AND m.user_id = ?
		FOR UPDATE`
	var itemType string
	var itemID, quantity int64
	var claimedAt sql.NullTime
	err = tx.QueryRowContext(ctx, qLoad, uint64(attachmentID), uint64(userID)).
		Scan(&itemType, &itemID, &quantity, &claimedAt)
	if err == sql.ErrNoRows {
		return errcode.New(errcode.MailNotFound, "attachment not found or not yours")
	}
	if err != nil {
		return fmt.Errorf("load_attachment: %w", err)
	}
	if claimedAt.Valid {
		return errcode.New(errcode.MailAlreadyClaimed, "attachment already claimed")
	}

	now := time.Now().UTC()

	// 2. 写入库存或钱包。
	if err := s.creditItem(ctx, tx, userID, itemType, itemID, quantity, now); err != nil {
		return err
	}

	// 3. 标记已领取。
	const qClaim = `UPDATE mail_attachments SET claimed_at = ? WHERE attachment_id = ?`
	if _, err := tx.ExecContext(ctx, qClaim, now, uint64(attachmentID)); err != nil {
		return fmt.Errorf("mark_claimed: %w", err)
	}

	return tx.Commit()
}

// creditItem 向用户库存或钱包写入物品。
func (s *MySQLMailService) creditItem(ctx context.Context, tx *sql.Tx, userID int64, itemType string, itemID, quantity int64, now time.Time) error {
	switch itemType {
	case "COIN":
		// 直接增加金币余额（无需检查上限）。
		_, err := tx.ExecContext(ctx,
			`UPDATE wallets SET coin_balance = coin_balance + ?, updated_at = ? WHERE user_id = ?`,
			quantity, now, uint64(userID))
		return err
	default:
		// CROP / SEED：幂等 upsert 库存。
		_, err := tx.ExecContext(ctx, `
			INSERT INTO inventory_items (user_id, item_type, item_id, quantity, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE quantity = quantity + VALUES(quantity), updated_at = VALUES(updated_at)`,
			uint64(userID), itemType, uint64(itemID), quantity, now, now)
		return err
	}
}

// MarkRead 将邮件标记为已读。
func (s *MySQLMailService) MarkRead(ctx context.Context, mailID int64, userID int64) error {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin mark read: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	result, err := tx.ExecContext(ctx,
		`UPDATE mails SET status='READ', read_at=? WHERE mail_id=? AND user_id=? AND status='UNREAD'`,
		now, uint64(mailID), uint64(userID))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark read affected rows: %w", err)
	}
	if affected == 0 {
		return tx.Commit()
	}
	// The mails UPDATE trigger performed the authoritative decrement.
	summary, err := loadSummaryTx(ctx, tx, userID)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notify(ctx, summary)
	return nil
}

func loadSummaryTx(ctx context.Context, tx *sql.Tx, userID int64) (maildomain.MailboxSummary, error) {
	summary := maildomain.MailboxSummary{UserID: userID}
	if err := tx.QueryRowContext(ctx,
		`SELECT unread_count, version FROM mailbox_states WHERE user_id=?`,
		uint64(userID),
	).Scan(&summary.UnreadCount, &summary.Version); err != nil {
		return maildomain.MailboxSummary{}, fmt.Errorf("load mailbox summary: %w", err)
	}
	return summary, nil
}

func (s *MySQLMailService) notify(ctx context.Context, summary maildomain.MailboxSummary) {
	if s.notifier == nil {
		return
	}
	notifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 100*time.Millisecond)
	defer cancel()
	_ = s.notifier.NotifyMailboxChanged(notifyCtx, summary)
}

// ── SQL helpers ───────────────────────────────────────────────────────────────

func nullInt64(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullTime(v *time.Time) any {
	if v == nil {
		return nil
	}
	return *v
}
