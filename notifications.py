#!/usr/bin/env python3
"""
Email notifications for new Executive Council meetings.
Sends via AWS SES to subscribed users.
"""

import sqlite3
import time
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

try:
    import boto3
except ImportError:
    boto3 = None

DB_PATH = Path(__file__).parent / "executive_council.db"
SENDER = "NH Executive Council <alerts@executivecouncilnh.com>"
SITE_URL = "https://executivecouncilnh.com"
EMAILS_PER_SECOND = 14

log = logging.getLogger(__name__)


def get_subscribed_users(conn):
    """Return list of (user_id, email) for users with notifications enabled."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, email FROM users
        WHERE is_active = 1 AND notify_new_meetings = 1
    """)
    return cursor.fetchall()


def format_currency(value):
    if not value:
        return "$0"
    if value >= 1_000_000_000:
        return f"${value/1_000_000_000:,.2f}B"
    if value >= 1_000_000:
        return f"${value/1_000_000:,.1f}M"
    return f"${value:,.0f}"


def build_email(summary):
    """Build HTML + plain text email for a new meeting notification.

    summary dict keys: meeting_id, meeting_date, item_count, total_value,
                       contracts, grants, nominations, top_items
    """
    date_str = summary['meeting_date']
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        date_display = dt.strftime('%B %-d, %Y')
    except ValueError:
        date_display = date_str

    value_str = format_currency(summary.get('total_value', 0))
    meeting_url = f"{SITE_URL}/meeting/{summary['meeting_id']}"

    subject = f"New G&C Agenda: {date_display} -- {summary['item_count']} Items, {value_str}"

    # Top items list
    top_html = ""
    top_text = ""
    for item in summary.get('top_items', [])[:5]:
        amt = format_currency(item.get('amount'))
        desc = (item.get('description', '') or '')[:120]
        top_html += f'<li style="margin-bottom:6px;"><strong>{amt}</strong> — {desc}</li>\n'
        top_text += f"  - {amt} — {desc}\n"

    html_body = f"""
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:560px;margin:0 auto;color:#1e293b;">
  <h2 style="margin-bottom:4px;">New G&C Meeting Posted</h2>
  <p style="font-size:22px;font-weight:800;margin:0 0 16px 0;">{date_display}</p>

  <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
    <tr>
      <td style="padding:10px 16px;background:#f8fafc;border:1px solid #e2e8f0;text-align:center;">
        <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;">Items</div>
        <div style="font-size:22px;font-weight:700;">{summary['item_count']}</div>
      </td>
      <td style="padding:10px 16px;background:#f8fafc;border:1px solid #e2e8f0;text-align:center;">
        <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;">Total Value</div>
        <div style="font-size:22px;font-weight:700;">{value_str}</div>
      </td>
      <td style="padding:10px 16px;background:#f8fafc;border:1px solid #e2e8f0;text-align:center;">
        <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;">Contracts</div>
        <div style="font-size:22px;font-weight:700;">{summary.get('contracts', 0)}</div>
      </td>
    </tr>
  </table>

  {"<h3 style='margin-bottom:8px;'>Largest Items</h3><ul style='padding-left:20px;font-size:14px;'>" + top_html + "</ul>" if top_html else ""}

  <p style="margin-top:24px;">
    <a href="{meeting_url}" style="display:inline-block;padding:10px 24px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;font-weight:600;font-size:14px;">View Full Agenda</a>
  </p>

  <hr style="border:none;border-top:1px solid #e2e8f0;margin:24px 0;">
  <p style="font-size:11px;color:#94a3b8;">
    You're receiving this because you signed up for meeting alerts on <a href="{SITE_URL}" style="color:#94a3b8;">{SITE_URL}</a>.
    <a href="{SITE_URL}/account" style="color:#94a3b8;">Manage notifications</a>
  </p>
</div>
"""

    text_body = f"""New G&C Meeting Posted: {date_display}

Items: {summary['item_count']}
Total Value: {value_str}
Contracts: {summary.get('contracts', 0)}

{"Largest Items:\n" + top_text if top_text else ""}
View full agenda: {meeting_url}

---
Manage notifications: {SITE_URL}/account
"""

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = SENDER
    msg.attach(MIMEText(text_body, 'plain'))
    msg.attach(MIMEText(html_body, 'html'))
    return msg


def send_notifications(summary, db_path=None):
    """Send notification emails to all subscribed users for a new meeting."""
    if boto3 is None:
        log.warning("boto3 not installed, skipping notifications")
        return 0

    db_path = db_path or DB_PATH
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    users = get_subscribed_users(conn)

    if not users:
        log.info("No subscribed users, skipping notifications")
        conn.close()
        return 0

    ses = boto3.client('ses', region_name='us-east-1')
    sent = 0

    for i, user in enumerate(users):
        email = user['email']
        msg = build_email(summary)
        msg['To'] = email

        try:
            ses.send_raw_email(
                Source=SENDER,
                Destinations=[email],
                RawMessage={'Data': msg.as_string()}
            )
            conn.execute(
                "INSERT INTO notification_log (user_id, meeting_id, sent_at, status) VALUES (?, ?, ?, 'sent')",
                (user['id'], summary['meeting_id'], datetime.now().isoformat())
            )
            sent += 1
            log.info(f"Sent notification to {email}")
        except Exception as e:
            conn.execute(
                "INSERT INTO notification_log (user_id, meeting_id, sent_at, status, error) VALUES (?, ?, ?, 'failed', ?)",
                (user['id'], summary['meeting_id'], datetime.now().isoformat(), str(e))
            )
            log.error(f"Failed to send to {email}: {e}")

        if (i + 1) % EMAILS_PER_SECOND == 0:
            time.sleep(1)

    conn.commit()
    conn.close()
    log.info(f"Sent {sent}/{len(users)} notifications for meeting {summary['meeting_date']}")
    return sent
