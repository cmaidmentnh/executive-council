#!/usr/bin/env python3
"""
Daily cron job to check for new Executive Council meetings.

Checks the NH SoS API for meetings not yet in the database,
scrapes them, and sends email notifications to subscribed users.

Usage:
    python3 cron_scraper.py          # Check for new meetings
    python3 cron_scraper.py --force   # Re-check even if recently scraped
"""

import json
import os
import re
import sqlite3
import sys
import time
import logging
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

import boto3

from scraper import (
    clean_text, extract_amount, extract_vendor, extract_dates,
    extract_funding_source, classify_item
)
from rescrape_2022 import parse_meeting_dot_format
from notifications import send_notifications, format_currency
from downloader import (
    ensure_schema as ensure_download_schema,
    create_session as create_download_session,
    scrape_and_download_meeting
)

DB_PATH = Path(__file__).parent / "executive_council.db"
DOWNLOAD_DIR = Path(__file__).parent / "downloads"
BASE_URL = "https://www.sos.nh.gov"
LOG_PATH = Path(__file__).parent / "cron_scraper.log"

# R2 config
R2_BUCKET = "executive-council-docs"
R2_ENDPOINT = "https://ecdb5e1e7f77f60c63411ccbf171edc8.r2.cloudflarestorage.com"
R2_ACCESS_KEY = "985056ec54f1b907ba443ba8631aa24a"
R2_SECRET_KEY = "8f1e8baedeb2b0866048aeff6e23458e018f45f27d11672f139eef12870a1d62"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def launch_browser(playwright):
    """Launch headless Chrome with anti-bot measures."""
    browser = playwright.chromium.launch(
        headless=True,
        channel="chrome",
        args=['--disable-blink-features=AutomationControlled']
    )
    context = browser.new_context(
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    )
    page = context.new_page()
    page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        window.chrome = { runtime: {} };
    """)
    return browser, page


def fetch_recent_meetings(page):
    """Fetch the most recent meetings from the SoS API via Playwright.

    Returns list of dicts with keys: title, nid, url, date
    """
    log.info("Navigating to SoS meetings page...")
    page.goto(f"{BASE_URL}/administration/governor-executive-council/meetings", timeout=30000)
    page.wait_for_timeout(3000)

    # Fetch first 2 pages (10 meetings) — enough to catch any new ones
    all_meetings = []
    for pg in [1, 2]:
        result_text = page.evaluate(f"""
            async () => {{
                const params = new URLSearchParams({{
                    q: '@field_categories|=|1036',
                    sort: 'field_date|desc',
                    size: '5',
                    show_body: '0',
                    show_date: '1',
                    show_audio: '1',
                    link_to_content: '1',
                    page: '{pg}'
                }});
                const resp = await fetch('/content/api/meetings?' + params.toString());
                return await resp.text();
            }}
        """)
        data = json.loads(result_text)
        for item in data.get('data', []):
            title = item.get('title', '').strip()
            nid = item.get('id', '')
            lc = item.get('list_content', '')
            url_match = re.search(r'href="([^"]*)"', lc)
            url = url_match.group(1) if url_match else ''
            fields = item.get('fields', {})
            field_date = fields.get('field_date', [''])[0] if fields.get('field_date') else ''
            all_meetings.append({
                'title': title,
                'nid': nid,
                'url': url,
                'date': field_date
            })

    log.info(f"Fetched {len(all_meetings)} recent meetings from API")
    return all_meetings


def find_new_meetings(meetings, conn):
    """Return meetings not yet in the database."""
    c = conn.cursor()
    existing_nids = set()
    c.execute("SELECT nid FROM meetings")
    for row in c.fetchall():
        existing_nids.add(str(row['nid']))

    new = [m for m in meetings if str(m['nid']) not in existing_nids]
    return new


def find_empty_meetings(meetings, conn):
    """Return meetings that exist in DB but have 0 items (failed scrape)."""
    c = conn.cursor()
    empty = []
    for m in meetings:
        c.execute("SELECT id, item_count FROM meetings WHERE nid = ?", (m['nid'],))
        row = c.fetchone()
        if row and (row['item_count'] is None or row['item_count'] == 0):
            empty.append(m)
    return empty


def insert_meeting(meeting, conn):
    """Insert a new meeting into the database. Returns the meeting id."""
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO meetings (nid, title, meeting_date, url)
        VALUES (?, ?, ?, ?)
    """, (meeting['nid'], meeting['title'], meeting['date'], meeting['url']))
    conn.commit()
    c.execute("SELECT id FROM meetings WHERE nid = ?", (meeting['nid'],))
    row = c.fetchone()
    return row['id'] if row else None


def scrape_and_store(page, meeting, meeting_id, conn):
    """Scrape a meeting page and store items. Returns count of items inserted."""
    url = meeting['url']
    if not url:
        log.warning(f"No URL for meeting {meeting['title']}")
        return 0

    full_url = f"{BASE_URL}{url}" if url.startswith('/') else url
    log.info(f"Scraping {full_url}")

    try:
        page.goto(full_url, timeout=45000)
        page.wait_for_timeout(3000)
    except Exception as e:
        log.error(f"Failed to load {full_url}: {e}")
        return 0

    page_text = page.inner_text('body')
    page_html = page.content()

    # Use the unified parser that handles all formats
    items = parse_meeting_dot_format(page_text, page_html)
    log.info(f"Parser found {len(items)} items")

    if not items:
        return 0

    c = conn.cursor()
    # Get existing items to avoid duplicates
    c.execute("SELECT item_number, sub_item FROM agenda_items WHERE meeting_id = ?", (meeting_id,))
    existing_keys = {(row['item_number'], row['sub_item']) for row in c.fetchall()}

    inserted = 0
    for item in items:
        key = (item['item_number'], item['sub_item'])
        if key in existing_keys:
            continue
        try:
            c.execute("""
                INSERT INTO agenda_items
                (meeting_id, item_number, sub_item, section, department, sub_department,
                 description, amount, amount_text, vendor, vendor_city, vendor_state,
                 funding_source, effective_date_start, effective_date_end,
                 item_type, is_consent_calendar, is_tabled, is_late_item,
                 download_url, business_record_url, raw_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                meeting_id, item['item_number'], item['sub_item'],
                item['section'], item['department'], item['sub_department'],
                item['description'], item['amount'], item['amount_text'],
                item['vendor'], item['vendor_city'], item['vendor_state'],
                item['funding_source'], item['effective_date_start'], item['effective_date_end'],
                item['item_type'], item['is_consent_calendar'], item['is_tabled'],
                item['is_late_item'], item['download_url'], item['business_record_url'],
                item['raw_text']
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    # Update meeting item count and scrape timestamp
    c.execute("SELECT COUNT(*) as cnt FROM agenda_items WHERE meeting_id = ?", (meeting_id,))
    total = c.fetchone()['cnt']
    c.execute("UPDATE meetings SET item_count = ?, scraped_at = ? WHERE id = ?",
              (total, datetime.now().isoformat(), meeting_id))
    conn.commit()

    return inserted


def build_notification_summary(meeting_id, conn):
    """Build the summary dict needed by notifications.send_notifications()."""
    c = conn.cursor()
    c.execute("SELECT id, meeting_date FROM meetings WHERE id = ?", (meeting_id,))
    meeting = c.fetchone()
    if not meeting:
        return None

    c.execute("SELECT COUNT(*) as cnt FROM agenda_items WHERE meeting_id = ?", (meeting_id,))
    item_count = c.fetchone()['cnt']

    c.execute("SELECT COALESCE(SUM(amount), 0) as total FROM agenda_items WHERE meeting_id = ? AND amount IS NOT NULL", (meeting_id,))
    total_value = c.fetchone()['total']

    # Type counts
    c.execute("SELECT item_type, COUNT(*) as cnt, COALESCE(SUM(amount),0) as total FROM agenda_items WHERE meeting_id = ? GROUP BY item_type ORDER BY total DESC", (meeting_id,))
    type_rows = c.fetchall()
    type_breakdown = [{'item_type': r['item_type'], 'count': r['cnt'], 'total': r['total']} for r in type_rows]

    type_counts = {r['item_type']: r['cnt'] for r in type_rows}
    contracts = type_counts.get('contract', 0)
    grants = type_counts.get('grant', 0)
    amendments = type_counts.get('amendment', 0)
    nominations = type_counts.get('nomination', 0)

    # Calendar breakdown
    c.execute("SELECT COALESCE(SUM(is_consent_calendar), 0) as consent, COUNT(*) - COALESCE(SUM(is_consent_calendar), 0) as regular, COALESCE(SUM(is_tabled), 0) as tabled, COALESCE(SUM(is_late_item), 0) as late FROM agenda_items WHERE meeting_id = ?", (meeting_id,))
    cal = c.fetchone()

    # Top 10 items by dollar amount
    c.execute("""
        SELECT item_number, sub_item, department, vendor, description, amount
        FROM agenda_items WHERE meeting_id = ? AND amount IS NOT NULL
        ORDER BY amount DESC LIMIT 10
    """, (meeting_id,))
    top_items = [{'item_number': r['item_number'], 'sub_item': r['sub_item'],
                  'department': r['department'], 'vendor': r['vendor'],
                  'description': r['description'], 'amount': r['amount']} for r in c.fetchall()]

    # Top departments by spending
    c.execute("""
        SELECT department, COUNT(*) as count, COALESCE(SUM(amount), 0) as total
        FROM agenda_items WHERE meeting_id = ? AND department != ''
        GROUP BY department ORDER BY total DESC LIMIT 8
    """, (meeting_id,))
    departments = [{'department': r['department'], 'count': r['count'], 'total': r['total']} for r in c.fetchall()]

    return {
        'meeting_id': meeting_id,
        'meeting_date': meeting['meeting_date'],
        'item_count': item_count,
        'total_value': total_value,
        'contracts': contracts,
        'grants': grants,
        'amendments': amendments,
        'nominations': nominations,
        'top_items': top_items,
        'departments': departments,
        'type_breakdown': type_breakdown,
        'consent_count': cal['consent'],
        'regular_count': cal['regular'],
        'tabled_count': cal['tabled'],
        'late_count': cal['late'],
    }


def check_for_late_items(page, conn):
    """Re-scrape the most recent meeting to detect newly added late items.

    Late items are often added after the initial agenda is posted.
    Returns list of (meeting_id, new_item_count) for meetings with new items.
    """
    c = conn.cursor()
    # Get the most recent meeting that has items (likely to get late items added)
    c.execute("""
        SELECT m.id, m.nid, m.url, m.meeting_date, m.item_count
        FROM meetings m WHERE m.item_count > 0
        ORDER BY m.meeting_date DESC LIMIT 2
    """)
    recent = c.fetchall()

    updates = []
    for meeting in recent:
        meeting_id = meeting['id']
        url = meeting['url']
        if not url:
            continue

        full_url = f"{BASE_URL}{url}" if url.startswith('/') else url
        log.info(f"Checking for late items: {meeting['meeting_date']} ({meeting['item_count']} existing)")

        try:
            page.goto(full_url, timeout=45000)
            page.wait_for_timeout(3000)
        except Exception as e:
            log.error(f"Failed to load {full_url}: {e}")
            continue

        page_text = page.inner_text('body')
        page_html = page.content()
        items = parse_meeting_dot_format(page_text, page_html)

        if not items:
            continue

        # Get existing items
        c.execute("SELECT item_number, sub_item FROM agenda_items WHERE meeting_id = ?", (meeting_id,))
        existing_keys = {(row['item_number'], row['sub_item']) for row in c.fetchall()}

        new_count = 0
        for item in items:
            key = (item['item_number'], item['sub_item'])
            if key in existing_keys:
                continue
            try:
                c.execute("""
                    INSERT INTO agenda_items
                    (meeting_id, item_number, sub_item, section, department, sub_department,
                     description, amount, amount_text, vendor, vendor_city, vendor_state,
                     funding_source, effective_date_start, effective_date_end,
                     item_type, is_consent_calendar, is_tabled, is_late_item,
                     download_url, business_record_url, raw_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    meeting_id, item['item_number'], item['sub_item'],
                    item['section'], item['department'], item['sub_department'],
                    item['description'], item['amount'], item['amount_text'],
                    item['vendor'], item['vendor_city'], item['vendor_state'],
                    item['funding_source'], item['effective_date_start'], item['effective_date_end'],
                    item['item_type'], item['is_consent_calendar'], item['is_tabled'],
                    item['is_late_item'], item['download_url'], item['business_record_url'],
                    item['raw_text']
                ))
                new_count += 1
            except sqlite3.IntegrityError:
                pass

        if new_count > 0:
            # Update item count
            c.execute("SELECT COUNT(*) as cnt FROM agenda_items WHERE meeting_id = ?", (meeting_id,))
            total = c.fetchone()['cnt']
            c.execute("UPDATE meetings SET item_count = ?, scraped_at = ? WHERE id = ?",
                      (total, datetime.now().isoformat(), meeting_id))
            conn.commit()
            log.info(f"Found {new_count} new late items for {meeting['meeting_date']}")
            updates.append((meeting_id, new_count))
        else:
            log.info(f"No new items found for {meeting['meeting_date']}")

        time.sleep(2)

    return updates


def download_and_upload_pdfs(meeting_id, meeting_date, meeting_url, conn):
    """Download PDFs for a meeting and upload them to R2."""
    ensure_download_schema()
    DOWNLOAD_DIR.mkdir(exist_ok=True)

    session = create_download_session()
    c = conn.cursor()
    c.execute("SELECT id, nid, title, meeting_date, url FROM meetings WHERE id = ?", (meeting_id,))
    row = c.fetchone()
    if not row:
        log.warning(f"Meeting {meeting_id} not found for PDF download")
        return

    downloaded, failed = scrape_and_download_meeting(session, row, conn)
    log.info(f"PDFs: {downloaded} downloaded, {failed} failed for {meeting_date}")

    # Upload to R2
    meeting_dir = DOWNLOAD_DIR / meeting_date
    if not meeting_dir.exists():
        log.warning(f"No download dir for {meeting_date}")
        return

    try:
        s3 = boto3.client('s3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name='auto'
        )
        uploaded = 0
        for pdf in meeting_dir.glob('*.pdf'):
            key = f"{meeting_date}/{pdf.name}"
            s3.upload_file(str(pdf), R2_BUCKET, key,
                          ExtraArgs={'ContentType': 'application/pdf'})
            uploaded += 1
        log.info(f"R2: uploaded {uploaded} PDFs for {meeting_date}")
    except Exception as e:
        log.error(f"R2 upload failed for {meeting_date}: {e}")


def main():
    force = '--force' in sys.argv

    log.info("=" * 60)
    log.info("Executive Council cron scraper starting")
    log.info(f"Database: {DB_PATH}")

    conn = get_db()

    new_meetings_found = []
    late_item_updates = []

    with sync_playwright() as p:
        browser, page = launch_browser(p)

        try:
            # Fetch recent meetings from API
            recent = fetch_recent_meetings(page)

            # Find meetings not yet in DB
            new_meetings = find_new_meetings(recent, conn)

            # Also check for meetings with 0 items (failed previous scrape)
            empty_meetings = find_empty_meetings(recent, conn) if force else []

            to_process = new_meetings + empty_meetings

            if to_process:
                log.info(f"Found {len(new_meetings)} new + {len(empty_meetings)} empty meetings to process")

                for meeting in to_process:
                    log.info(f"Processing: {meeting['title']} ({meeting['date']})")

                    # Insert into DB if new
                    meeting_id = insert_meeting(meeting, conn)
                    if not meeting_id:
                        log.error(f"Failed to get meeting_id for {meeting['title']}")
                        continue

                    # Scrape the meeting page
                    count = scrape_and_store(page, meeting, meeting_id, conn)
                    log.info(f"Inserted {count} items for {meeting['title']}")

                    if count > 0:
                        new_meetings_found.append((meeting_id, meeting))
                        # Download PDFs and upload to R2
                        download_and_upload_pdfs(meeting_id, meeting['date'], meeting['url'], conn)

                    time.sleep(2)
            else:
                log.info("No new meetings found")

            # Always check the most recent meetings for late items
            late_item_updates = check_for_late_items(page, conn)

        finally:
            browser.close()

    # Send notifications for genuinely new meetings
    for meeting_id, meeting in new_meetings_found:
        if meeting in new_meetings:
            summary = build_notification_summary(meeting_id, conn)
            if summary and summary['item_count'] > 0:
                log.info(f"Sending notifications for {meeting['date']}...")
                sent = send_notifications(summary, db_path=DB_PATH)
                log.info(f"Sent {sent} notification emails")

    # Handle late items: download PDFs, upload to R2, send notifications
    for meeting_id, new_count in late_item_updates:
        # Download any new PDFs and upload to R2
        c = conn.cursor()
        c.execute("SELECT meeting_date, url FROM meetings WHERE id = ?", (meeting_id,))
        m_row = c.fetchone()
        if m_row:
            download_and_upload_pdfs(meeting_id, m_row['meeting_date'], m_row['url'], conn)

        summary = build_notification_summary(meeting_id, conn)
        if summary:
            summary['_is_late_update'] = True
            summary['_new_late_count'] = new_count
            log.info(f"Sending late item notifications for meeting {meeting_id} ({new_count} new items)...")
            sent = send_notifications(summary, db_path=DB_PATH)
            log.info(f"Sent {sent} late item notification emails")

    conn.close()
    log.info("Cron scraper finished")
    log.info("=" * 60)


if __name__ == '__main__':
    main()
