#!/usr/bin/env python3
"""
NH Executive Council PDF Downloader & Vote Extractor

Downloads all PDFs from G&C meetings and extracts vote outcomes.
Uses curl_cffi with Chrome TLS impersonation — no browser window.

For each meeting:
1. Loads the meeting page HTML
2. Finds all PDF links matching the meeting's date code
3. Downloads the raw PDFs directly
4. Also grabs Quick Results, Minutes, Printable Agenda
5. Parses Quick Results for vote outcomes

Usage:
    python3 downloader.py                  # Download everything
    python3 downloader.py --meeting URL    # One meeting only
    python3 downloader.py --votes-only     # Extract votes from downloaded Quick Results
    python3 downloader.py --status         # Progress report
"""

import json
import re
import sqlite3
import sys
import time
from pathlib import Path
from datetime import datetime

from curl_cffi import requests as cffi_requests

DB_PATH = Path(__file__).parent / "executive_council.db"
DOWNLOAD_DIR = Path(__file__).parent / "downloads"
BASE_URL = "https://www.sos.nh.gov"


def get_conn():
    if not DB_PATH.exists():
        print("Database not found. Run scraper.py first.")
        sys.exit(1)
    return sqlite3.connect(DB_PATH)


def ensure_schema():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS meeting_downloads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        meeting_id INTEGER REFERENCES meetings(id),
        doc_type TEXT, filename TEXT, file_path TEXT,
        file_size INTEGER, download_url TEXT, downloaded_at TIMESTAMP,
        UNIQUE(meeting_id, doc_type))""")
    c.execute("""CREATE TABLE IF NOT EXISTS item_downloads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER, meeting_id INTEGER REFERENCES meetings(id),
        item_number TEXT, sub_item TEXT,
        filename TEXT, file_path TEXT, file_size INTEGER,
        download_url TEXT, downloaded_at TIMESTAMP,
        UNIQUE(meeting_id, item_number, sub_item))""")
    c.execute("""CREATE TABLE IF NOT EXISTS vote_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        meeting_id INTEGER REFERENCES meetings(id),
        item_number TEXT, sub_item TEXT, outcome TEXT, vote_type TEXT,
        yeas INTEGER, nays INTEGER, abstain INTEGER, raw_text TEXT,
        UNIQUE(meeting_id, item_number, sub_item))""")
    c.execute("""CREATE TABLE IF NOT EXISTS councilor_votes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vote_outcome_id INTEGER REFERENCES vote_outcomes(id),
        councilor_name TEXT, vote TEXT,
        UNIQUE(vote_outcome_id, councilor_name))""")
    conn.commit()
    conn.close()


def show_status():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM meetings")
    total_meetings = c.fetchone()[0]

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='item_downloads'")
    if not c.fetchone():
        print("No downloads yet.")
        conn.close()
        return

    c.execute("SELECT COUNT(DISTINCT meeting_id) FROM item_downloads")
    meetings_done = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM item_downloads")
    items_done = c.fetchone()[0]

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='meeting_downloads'")
    doc_counts = {}
    if c.fetchone():
        c.execute("SELECT doc_type, COUNT(*) FROM meeting_downloads GROUP BY doc_type")
        doc_counts = dict(c.fetchall())

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vote_outcomes'")
    votes = 0
    if c.fetchone():
        c.execute("SELECT COUNT(*) FROM vote_outcomes")
        votes = c.fetchone()[0]

    total_size = 0
    if DOWNLOAD_DIR.exists():
        for f in DOWNLOAD_DIR.rglob('*'):
            if f.is_file():
                total_size += f.stat().st_size

    print(f"{'='*60}")
    print(f"DOWNLOAD STATUS")
    print(f"{'='*60}")
    print(f"Total meetings:          {total_meetings}")
    print(f"Meetings with items:     {meetings_done}")
    print(f"Item PDFs downloaded:    {items_done}")
    print(f"  Quick Results:         {doc_counts.get('quick_results', 0)}")
    print(f"  Minutes:               {doc_counts.get('minutes', 0)}")
    print(f"  Printable Agendas:     {doc_counts.get('printable_agenda', 0)}")
    print(f"Vote outcomes:           {votes}")
    print(f"Disk usage:              {total_size / (1024*1024*1024):.2f} GB")
    conn.close()


def sanitize_filename(name):
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:200]


def meeting_dir(meeting_date):
    path = DOWNLOAD_DIR / (meeting_date or "unknown")
    path.mkdir(parents=True, exist_ok=True)
    return path


def make_date_code(meeting_date):
    """Convert '2026-01-28' to '012826' (MMDDYY).
    Also returns MMDDYYYY variant for older meetings."""
    if not meeting_date:
        return None, None
    try:
        dt = datetime.strptime(meeting_date, '%Y-%m-%d')
        mmddyy = dt.strftime('%m%d%y')
        mmddyyyy = dt.strftime('%m%d%Y')
        return mmddyy, mmddyyyy
    except ValueError:
        return None, None


def create_session():
    return cffi_requests.Session(impersonate='chrome120')


def download_file(session, url, save_path, retries=2):
    """Download a file. Returns (success, file_size)."""
    for attempt in range(retries + 1):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 500:
                save_path.parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, 'wb') as f:
                    f.write(r.content)
                return True, len(r.content)
            if attempt < retries:
                time.sleep(2)
                continue
            return False, 0
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            return False, 0
    return False, 0


def scrape_and_download_meeting(session, meeting_row, conn):
    """Scrape a meeting page for PDF links and download them all.

    Returns (num_downloaded, num_failed).
    """
    meeting_id, nid, title, meeting_date, meeting_url = meeting_row
    c = conn.cursor()

    # Check if already done — only skip if we have a reasonable number of downloads
    c.execute("SELECT COUNT(*) FROM item_downloads WHERE meeting_id = ?", (meeting_id,))
    already_done = c.fetchone()[0]
    c.execute("SELECT item_count FROM meetings WHERE id = ?", (meeting_id,))
    expected = c.fetchone()[0] or 0
    # Skip if we have at least 60% of expected items (allow for sub-items sharing PDFs)
    if already_done > 0 and (expected == 0 or already_done >= expected * 0.6):
        return already_done, 0  # Skip — already well-covered

    # Build the meeting page URL — try multiple patterns
    urls_to_try = []
    if meeting_url.startswith('/meeting/'):
        urls_to_try.append(BASE_URL + meeting_url)
        urls_to_try.append(BASE_URL + meeting_url.replace('/meeting/', '/'))
    elif meeting_url.startswith('/'):
        urls_to_try.append(BASE_URL + meeting_url)
        urls_to_try.append(BASE_URL + '/meeting' + meeting_url)
    else:
        urls_to_try.append(BASE_URL + '/meeting/' + meeting_url)
        urls_to_try.append(BASE_URL + '/' + meeting_url)

    # Get date codes for this meeting
    date_short, date_long = make_date_code(meeting_date)
    if not date_short:
        return 0, 0

    # Fetch the meeting page — try each URL
    r = None
    for try_url in urls_to_try:
        try:
            r = session.get(try_url, timeout=20)
            if r.status_code == 200:
                break
        except Exception:
            continue
    if r is None or r.status_code != 200:
        print(f"    Page failed: all URLs returned non-200")
        return 0, 0

    html = r.text

    # Find all PDF links — both new format (media.sos.nh.gov) and old CMS (sos.nh.gov/media/)
    all_pdfs_new_raw = re.findall(
        r'href="(https://media\.sos\.nh\.gov/govcouncil/[^"]+\.pdf)\s*"',
        html, re.IGNORECASE
    )
    all_pdfs_old = re.findall(
        r'href="(https://sos\.nh\.gov/media/[^"]+\.pdf)"',
        html, re.IGNORECASE
    )

    # Drupal CMS pattern: /sites/g/files/.../sonh/filename.pdf (e.g. 2022)
    all_pdfs_drupal = re.findall(
        r'href="(/sites/g/files/[^"]+\.pdf)"',
        html, re.IGNORECASE
    )
    # Convert relative Drupal URLs to absolute
    all_pdfs_old += [f'https://www.sos.nh.gov{p}' for p in all_pdfs_drupal]

    # Fix broken URLs missing MMDD subdirectory
    # e.g. /govcouncil/2014/001A GC... -> /govcouncil/2014/0805/001A GC...
    dt = datetime.strptime(meeting_date, '%Y-%m-%d')
    mmdd = dt.strftime('%m%d')
    year = dt.strftime('%Y')
    all_pdfs_new = []
    for url in all_pdfs_new_raw:
        # Check if URL is missing the MMDD subdirectory
        # Pattern: /govcouncil/YYYY/MMDD/filename or /govcouncil/YYYY/filename
        m = re.match(r'(https://media\.sos\.nh\.gov/govcouncil/\d{4}/)(\d{4}/)?(.+)', url)
        if m and not m.group(2):
            # Missing MMDD — insert it
            url = f'{m.group(1)}{mmdd}/{m.group(3)}'
        all_pdfs_new.append(url)

    mdir = meeting_dir(meeting_date)
    downloaded = 0
    failed = 0
    seen = set()

    def _process_pdf(pdf_url, format_type='new'):
        """Process a single PDF URL. Returns True if it was an item PDF."""
        nonlocal downloaded, failed
        fname = pdf_url.split('/')[-1]

        if format_type == 'new':
            # New CDN: '001A GC Agenda MMDDYYYY.pdf'
            item_match = re.match(r'^0*(\d+)([A-Z])?\s+GC\s+', fname, re.IGNORECASE)
            if not item_match:
                if 'quick result' in fname.lower():
                    _save_meeting_doc(session, conn, meeting_id, meeting_date,
                                      'quick_results', pdf_url, mdir)
                elif 'minutes' in fname.lower():
                    _save_meeting_doc(session, conn, meeting_id, meeting_date,
                                      'minutes', pdf_url, mdir)
                elif 'printable' in fname.lower():
                    _save_meeting_doc(session, conn, meeting_id, meeting_date,
                                      'printable_agenda', pdf_url, mdir)
                return False
        else:
            # Old CMS / Drupal formats:
            #   '01a-gc-agenda-MMDDYY.pdf'   (with gc prefix)
            #   '001a-gcagendaMMDDYY.pdf'     (no separators)
            #   '01a-agenda-01-16-13.pdf'     (no gc prefix, hyphenated date)
            item_match = re.match(r'^0*(\d+)([a-zA-Z])?[-\s]*(?:gc[-\s]*)?agenda', fname, re.IGNORECASE)
            if not item_match:
                if 'quick' in fname.lower() and 'result' in fname.lower():
                    _save_meeting_doc(session, conn, meeting_id, meeting_date,
                                      'quick_results', pdf_url, mdir)
                elif 'minute' in fname.lower():
                    _save_meeting_doc(session, conn, meeting_id, meeting_date,
                                      'minutes', pdf_url, mdir)
                return False

        item_num = item_match.group(1)
        sub_item = (item_match.group(2) or '').upper()
        key = (item_num, sub_item)
        if key in seen:
            return True  # Already processed
        seen.add(key)

        save_name = f"Item_{item_num}{sub_item}.pdf"
        save_path = mdir / save_name

        if save_path.exists():
            fsize = save_path.stat().st_size
            success = True
        else:
            success, fsize = download_file(session, pdf_url, save_path)

        if success:
            c.execute("""
                INSERT OR REPLACE INTO item_downloads
                (item_id, meeting_id, item_number, sub_item, filename, file_path,
                 file_size, download_url, downloaded_at)
                VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (meeting_id, item_num, sub_item or None, save_name, str(save_path),
                  fsize, pdf_url, datetime.now().isoformat()))
            downloaded += 1
        else:
            failed += 1

        time.sleep(0.05)
        return True

    # First pass: process with date code filter (strict)
    for pdf_url in all_pdfs_new:
        fname = pdf_url.split('/')[-1]
        if date_short not in fname and date_long not in fname:
            continue
        _process_pdf(pdf_url, 'new')

    date_dash = date_short
    for pdf_url in all_pdfs_old:
        fname = pdf_url.split('/')[-1]
        if date_dash not in fname and date_long not in fname:
            continue
        _process_pdf(pdf_url, 'old')

    # Second pass: grab ALL remaining item PDFs from page (cross-date items from
    # cancelled/tabled meetings that were rolled into this meeting's agenda)
    for pdf_url in all_pdfs_new:
        _process_pdf(pdf_url, 'new')
    for pdf_url in all_pdfs_old:
        _process_pdf(pdf_url, 'old')

    # Also look for Quick Results / Minutes that may not be on the meeting page
    # They're often linked separately or follow a URL pattern
    _try_standard_doc_urls(session, conn, meeting_id, meeting_date, mdir, date_short, date_long)

    conn.commit()
    return downloaded, failed


def _save_meeting_doc(session, conn, meeting_id, meeting_date, doc_type, url, mdir):
    """Download and save a meeting-level document."""
    c = conn.cursor()
    c.execute("SELECT id FROM meeting_downloads WHERE meeting_id=? AND doc_type=?",
              (meeting_id, doc_type))
    if c.fetchone():
        return

    filename = f"{doc_type}_{meeting_date}.pdf"
    save_path = mdir / filename

    if save_path.exists():
        fsize = save_path.stat().st_size
        success = True
    else:
        success, fsize = download_file(session, url, save_path)

    if success:
        c.execute("""
            INSERT OR REPLACE INTO meeting_downloads
            (meeting_id, doc_type, filename, file_path, file_size, download_url, downloaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (meeting_id, doc_type, filename, str(save_path), fsize, url,
              datetime.now().isoformat()))
        conn.commit()
        print(f"    {doc_type}: {fsize/1024:.0f} KB")


def _try_standard_doc_urls(session, conn, meeting_id, meeting_date, mdir, date_short, date_long):
    """Try standard URL patterns for Quick Results and other meeting docs."""
    c = conn.cursor()
    dt = datetime.strptime(meeting_date, '%Y-%m-%d')
    year = dt.strftime('%Y')
    mmdd = dt.strftime('%m%d')

    # Quick Results pattern: /govcouncil/YYYY/MMDD/Quick Results MMDDYYYY.pdf
    for date_fmt in [date_long, date_short]:
        for name_pattern in [
            f"Quick Results {date_fmt}.pdf",
            f"Quick Results {date_fmt} .pdf",
            f"Quick%20Results%20{date_fmt}.pdf",
        ]:
            url = f"https://media.sos.nh.gov/govcouncil/{year}/{mmdd}/{name_pattern}"
            c.execute("SELECT id FROM meeting_downloads WHERE meeting_id=? AND doc_type='quick_results'",
                      (meeting_id,))
            if c.fetchone():
                break
            try:
                r = session.head(url, timeout=10)
                if r.status_code == 200:
                    _save_meeting_doc(session, conn, meeting_id, meeting_date,
                                      'quick_results', url, mdir)
                    break
            except Exception:
                continue


def download_all(single_meeting_url=None):
    ensure_schema()
    conn = get_conn()
    c = conn.cursor()

    if single_meeting_url:
        c.execute("SELECT id, nid, title, meeting_date, url FROM meetings WHERE url LIKE ?",
                  (f'%{single_meeting_url}%',))
        meetings = c.fetchall()
    else:
        c.execute("""SELECT id, nid, title, meeting_date, url FROM meetings
                     WHERE scraped_at IS NOT NULL ORDER BY meeting_date DESC""")
        meetings = c.fetchall()

    if not meetings:
        print("No meetings found.")
        conn.close()
        return

    print(f"Downloading PDFs from {len(meetings)} meetings...")
    DOWNLOAD_DIR.mkdir(exist_ok=True)

    session = create_session()
    total_new = 0
    total_failed = 0
    total_skipped = 0
    start = time.time()

    for idx, meeting_row in enumerate(meetings, 1):
        meeting_id, nid, title, meeting_date, url = meeting_row

        downloaded, failed = scrape_and_download_meeting(session, meeting_row, conn)

        if downloaded > 0 and failed == 0:
            print(f"[{idx}/{len(meetings)}] {meeting_date}: {downloaded} PDFs")
            total_new += downloaded
        elif downloaded > 0:
            print(f"[{idx}/{len(meetings)}] {meeting_date}: {downloaded} OK, {failed} failed")
            total_new += downloaded
            total_failed += failed
        elif failed > 0:
            print(f"[{idx}/{len(meetings)}] {meeting_date}: {failed} FAILED")
            total_failed += failed
        else:
            total_skipped += 1

        if idx % 50 == 0:
            elapsed = time.time() - start
            print(f"\n--- {idx}/{len(meetings)} ({elapsed/60:.1f} min), "
                  f"+{total_new} items, {total_failed} failed ---\n")

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"DONE ({elapsed/60:.1f} min)")
    print(f"{'='*60}")
    print(f"Meetings: {len(meetings)} ({total_skipped} skipped/no PDFs)")
    print(f"Items downloaded: {total_new}")
    print(f"Failed: {total_failed}")
    conn.close()


def extract_votes_from_quick_results():
    try:
        import pdfplumber
    except ImportError:
        print("pip3 install pdfplumber")
        sys.exit(1)

    ensure_schema()
    conn = get_conn()
    c = conn.cursor()

    c.execute("""SELECT md.meeting_id, md.file_path, m.meeting_date
                 FROM meeting_downloads md JOIN meetings m ON md.meeting_id = m.id
                 WHERE md.doc_type = 'quick_results' ORDER BY m.meeting_date DESC""")
    qr_files = c.fetchall()

    if not qr_files:
        print("No Quick Results PDFs found.")
        conn.close()
        return

    print(f"Processing {len(qr_files)} Quick Results...")
    total = 0

    for meeting_id, file_path, meeting_date in qr_files:
        if not Path(file_path).exists():
            continue
        try:
            with pdfplumber.open(file_path) as pdf:
                text = "\n".join(pg.extract_text() or '' for pg in pdf.pages)
        except Exception as e:
            print(f"  Error {file_path}: {e}")
            continue

        votes = parse_quick_results_text(text)
        for vote in votes:
            c.execute("""INSERT OR REPLACE INTO vote_outcomes
                (meeting_id, item_number, sub_item, outcome, vote_type, yeas, nays, abstain, raw_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (meeting_id, vote['item_number'], vote.get('sub_item'),
                 vote['outcome'], vote.get('vote_type', 'unknown'),
                 vote.get('yeas'), vote.get('nays'), vote.get('abstain'),
                 vote.get('raw_text', '')))
            vid = c.lastrowid
            for cv in vote.get('councilor_votes', []):
                c.execute("""INSERT OR REPLACE INTO councilor_votes
                    (vote_outcome_id, councilor_name, vote) VALUES (?, ?, ?)""",
                    (vid, cv['name'], cv['vote']))
        conn.commit()
        if votes:
            total += len(votes)
            print(f"  {meeting_date}: {len(votes)} votes")

    print(f"\nTotal: {total} vote outcomes")
    conn.close()


def parse_quick_results_text(text):
    votes = []
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue
        m = re.search(r'(?:[Ii]tem\s+)?#(\d+)([A-Z])?\s*[-–:]\s*(.*)', line)
        if m:
            vote = parse_vote_line(m.group(3).strip(), m.group(1), m.group(2))
            if vote:
                votes.append(vote)
                continue
        if votes and ('Yeas:' in line or 'Nays:' in line):
            parse_councilor_names(line, votes[-1])
    return votes


def parse_vote_line(text, item_num, sub_item=None):
    vote = {'item_number': item_num, 'sub_item': sub_item,
            'raw_text': text, 'councilor_votes': []}
    tl = text.lower()
    for keyword, outcome in [('approved', 'approved'), ('passed', 'approved'),
                              ('denied', 'denied'), ('failed', 'denied'),
                              ('tabled', 'tabled'), ('withdrawn', 'withdrawn'),
                              ('confirmed', 'confirmed'), ('postponed', 'postponed')]:
        if keyword in tl:
            vote['outcome'] = outcome
            break
    else:
        vote['outcome'] = text[:50]

    cm = re.search(r'(\d+)\s*[-–]\s*(\d+)', text)
    if cm:
        vote['yeas'] = int(cm.group(1))
        vote['nays'] = int(cm.group(2))
        vote['vote_type'] = 'unanimous' if vote['nays'] == 0 else 'roll_call'
    elif 'voice vote' in tl:
        vote['vote_type'] = 'voice'
    elif 'unanimous' in tl:
        vote['vote_type'] = 'unanimous'
    else:
        vote['vote_type'] = 'unknown'

    pm = re.search(r'\(([^)]+)\)', text)
    if pm:
        nm = re.search(r'[Cc]ouncilors?\s+(.+?)\s+voting\s+(nay|no)', pm.group(1))
        if nm:
            for name in re.split(r',\s*(?:and\s+)?', nm.group(1)):
                name = re.sub(r'^Councilor\s+', '', name.strip(), flags=re.IGNORECASE)
                if name:
                    vote['councilor_votes'].append({'name': name, 'vote': 'nay'})

    parse_councilor_names(text, vote)
    return vote


def parse_councilor_names(text, vote):
    existing = {cv['name'] for cv in vote['councilor_votes']}
    for pattern, v in [(r'[Yy]eas?:\s*(.+?)(?:\s*[Nn]ays?:|$)', 'yea'),
                       (r'[Nn]ays?:\s*(.+?)(?:\s*[Aa]bstain|$)', 'nay')]:
        m = re.search(pattern, text)
        if m:
            for name in re.split(r',\s*', m.group(1)):
                name = re.sub(r'^Councilor\s+', '', name.strip().rstrip(','), flags=re.IGNORECASE)
                if name and len(name) > 1 and name not in existing:
                    vote['councilor_votes'].append({'name': name, 'vote': v})
                    existing.add(name)


def main():
    if len(sys.argv) < 2:
        download_all()
        return
    cmd = sys.argv[1]
    if cmd == '--status':
        show_status()
    elif cmd == '--meeting':
        download_all(single_meeting_url=sys.argv[2] if len(sys.argv) > 2 else None)
    elif cmd == '--votes-only':
        extract_votes_from_quick_results()
    elif cmd == '--setup':
        ensure_schema()
        print("Ready.")
    else:
        print(__doc__)


if __name__ == '__main__':
    main()
