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
SENDER = "Granite State G&C Tracker <alerts@executivecouncilnh.com>"
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
                       contracts, grants, nominations, top_items,
                       departments (list of dicts), type_breakdown (list of dicts),
                       consent_count, regular_count, tabled_count, late_count
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

    # ── Stats row ──
    stats_cells = [
        ('Items', str(summary['item_count'])),
        ('Total Value', value_str),
        ('Contracts', str(summary.get('contracts', 0))),
        ('Grants', str(summary.get('grants', 0))),
        ('Amendments', str(summary.get('amendments', 0))),
        ('Nominations', str(summary.get('nominations', 0))),
    ]
    stats_html = ""
    for label, val in stats_cells:
        if val == "0" and label not in ('Items', 'Total Value', 'Contracts'):
            continue
        stats_html += f'''      <td style="padding:8px 12px;background:#f8fafc;border:1px solid #e2e8f0;text-align:center;">
        <div style="font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.5px;">{label}</div>
        <div style="font-size:20px;font-weight:700;color:#1e293b;">{val}</div>
      </td>\n'''

    # ── Top items table ──
    top_html = ""
    top_text = ""
    for item in summary.get('top_items', [])[:10]:
        amt = format_currency(item.get('amount'))
        dept = (item.get('department', '') or '').replace('DEPARTMENT OF ', '').replace('NEW HAMPSHIRE ', '').title()
        vendor = item.get('vendor') or ''
        desc = (item.get('description', '') or '')[:200]
        item_num = item.get('item_number', '')
        sub = item.get('sub_item', '') or ''
        item_url = f"{meeting_url}#item-{item_num}{sub}"

        # Build a concise but informative row
        meta_parts = []
        if dept:
            meta_parts.append(dept)
        if vendor:
            meta_parts.append(vendor)
        meta_line = ' · '.join(meta_parts)

        top_html += f'''<tr style="border-bottom:1px solid #f1f5f9;">
  <td style="padding:8px 0;vertical-align:top;width:80px;"><span style="font-weight:700;color:#1e293b;font-size:14px;">{amt}</span></td>
  <td style="padding:8px 0 8px 12px;vertical-align:top;">
    <div style="font-size:13px;color:#334155;line-height:1.4;">{desc}</div>
    <div style="font-size:11px;color:#94a3b8;margin-top:2px;">{meta_line}</div>
  </td>
</tr>\n'''
        top_text += f"  {amt} — {desc[:100]}\n"
        if meta_line:
            top_text += f"    {meta_line}\n"

    # ── Department breakdown ──
    dept_html = ""
    dept_text = ""
    departments = summary.get('departments', [])
    if departments:
        for d in departments[:8]:
            name = (d['department'] or '').replace('DEPARTMENT OF ', '').replace('NEW HAMPSHIRE ', '').title()
            d_amt = format_currency(d.get('total', 0))
            d_cnt = d.get('count', 0)
            bar_pct = min(100, max(5, (d.get('total', 0) / max(summary.get('total_value', 1), 1)) * 100))
            dept_html += f'''<tr>
  <td style="padding:4px 0;font-size:12px;color:#334155;white-space:nowrap;">{name}</td>
  <td style="padding:4px 8px;width:100%;">
    <div style="background:#e2e8f0;border-radius:3px;height:14px;"><div style="background:#3b82f6;border-radius:3px;height:14px;width:{bar_pct:.0f}%;"></div></div>
  </td>
  <td style="padding:4px 0;font-size:12px;color:#64748b;white-space:nowrap;text-align:right;">{d_amt}</td>
  <td style="padding:4px 0 4px 8px;font-size:11px;color:#94a3b8;white-space:nowrap;">{d_cnt} items</td>
</tr>\n'''
            dept_text += f"  {name}: {d_amt} ({d_cnt} items)\n"

    # ── Type breakdown ──
    type_html = ""
    type_text = ""
    type_breakdown = summary.get('type_breakdown', [])
    if type_breakdown:
        for t in type_breakdown:
            if t['count'] == 0:
                continue
            t_name = (t.get('item_type', '') or 'other').replace('_', ' ').title()
            t_amt = format_currency(t.get('total', 0))
            type_html += f'<span style="display:inline-block;margin:0 8px 4px 0;padding:3px 8px;background:#f1f5f9;border-radius:4px;font-size:11px;color:#475569;">{t_name}: {t["count"]} ({t_amt})</span>\n'
            type_text += f"  {t_name}: {t['count']} items ({t_amt})\n"

    # ── Calendar breakdown line ──
    cal_parts = []
    if summary.get('consent_count'):
        cal_parts.append(f"{summary['consent_count']} consent")
    if summary.get('regular_count'):
        cal_parts.append(f"{summary['regular_count']} regular")
    if summary.get('tabled_count'):
        cal_parts.append(f"{summary['tabled_count']} tabled")
    if summary.get('late_count'):
        cal_parts.append(f"{summary['late_count']} late")
    cal_line = ' · '.join(cal_parts)

    # ── Assemble HTML ──
    html_body = f"""
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;margin:0 auto;color:#1e293b;">
  <p style="margin:0 0 8px 0;font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Granite State G&amp;C Tracker</p>
  <h2 style="margin:0 0 4px 0;font-size:18px;color:#64748b;font-weight:600;">New G&amp;C Agenda Posted</h2>
  <p style="font-size:24px;font-weight:800;margin:0 0 4px 0;">{date_display}</p>
  <p style="font-size:12px;color:#94a3b8;margin:0 0 20px 0;">{cal_line}</p>

  <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
    <tr>
{stats_html}    </tr>
  </table>

  <h3 style="font-size:15px;font-weight:700;margin:0 0 8px 0;color:#1e293b;">Top Items by Value</h3>
  <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
{top_html}  </table>

  {"<h3 style='font-size:15px;font-weight:700;margin:0 0 8px 0;color:#1e293b;'>Spending by Department</h3>" + "<table style='width:100%;border-collapse:collapse;margin-bottom:24px;'>" + dept_html + "</table>" if dept_html else ""}

  {"<h3 style='font-size:15px;font-weight:700;margin:0 0 8px 0;color:#1e293b;'>Item Types</h3><p style='margin:0 0 24px 0;'>" + type_html + "</p>" if type_html else ""}

  <p style="margin-top:8px;">
    <a href="{meeting_url}" style="display:inline-block;padding:10px 28px;background:#2563eb;color:#ffffff;text-decoration:none;border-radius:6px;font-weight:600;font-size:14px;">View Full Agenda &rarr;</a>
  </p>

  <hr style="border:none;border-top:1px solid #e2e8f0;margin:28px 0 16px 0;">
  <p style="font-size:11px;color:#94a3b8;">
    You're receiving this because you signed up for alerts on <a href="{SITE_URL}" style="color:#94a3b8;">Granite State G&amp;C Tracker</a>. This is an unofficial resource.
    <a href="{SITE_URL}/account" style="color:#94a3b8;">Manage notifications</a>
  </p>
</div>
"""

    # ── Plain text version ──
    text_body = f"""New G&C Agenda Posted: {date_display}
{cal_line}

Items: {summary['item_count']} | Total Value: {value_str} | Contracts: {summary.get('contracts', 0)} | Grants: {summary.get('grants', 0)} | Amendments: {summary.get('amendments', 0)}

TOP ITEMS BY VALUE
{top_text}
{"SPENDING BY DEPARTMENT" + chr(10) + dept_text + chr(10) if dept_text else ""}{"ITEM TYPES" + chr(10) + type_text + chr(10) if type_text else ""}View full agenda: {meeting_url}

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
