#!/usr/bin/env python3
"""
Download all G&C Minutes PDFs.

Pattern: media.sos.nh.gov/govcouncil/YYYY/MMDD/GC Minutes MMDDYY.pdf

Run after or alongside downloader.py — this only grabs Minutes.
"""

import sqlite3
import time
from datetime import datetime
from pathlib import Path

from curl_cffi import requests as cffi_requests

DB_PATH = Path(__file__).parent / "executive_council.db"
DOWNLOAD_DIR = Path(__file__).parent / "downloads"


def main():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    c = conn.cursor()

    # Ensure meeting_downloads table exists
    c.execute("""CREATE TABLE IF NOT EXISTS meeting_downloads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        meeting_id INTEGER REFERENCES meetings(id),
        doc_type TEXT, filename TEXT, file_path TEXT,
        file_size INTEGER, download_url TEXT, downloaded_at TIMESTAMP,
        UNIQUE(meeting_id, doc_type))""")
    conn.commit()

    # Get all meetings
    c.execute("""SELECT id, meeting_date FROM meetings
                 WHERE scraped_at IS NOT NULL ORDER BY meeting_date DESC""")
    meetings = c.fetchall()

    # Skip ones we already have
    c.execute("SELECT meeting_id FROM meeting_downloads WHERE doc_type='minutes'")
    done = {r[0] for r in c.fetchall()}

    todo = [(mid, mdate) for mid, mdate in meetings if mid not in done]
    print(f"{len(todo)} meetings need Minutes ({len(done)} already done)")

    session = cffi_requests.Session(impersonate='chrome120')
    downloaded = 0
    failed = 0

    for idx, (meeting_id, meeting_date) in enumerate(todo, 1):
        dt = datetime.strptime(meeting_date, '%Y-%m-%d')
        year = dt.strftime('%Y')
        mmdd = dt.strftime('%m%d')
        code = dt.strftime('%m%d%y')

        url = f"https://media.sos.nh.gov/govcouncil/{year}/{mmdd}/GC Minutes {code}.pdf"

        mdir = DOWNLOAD_DIR / meeting_date
        mdir.mkdir(parents=True, exist_ok=True)
        save_path = mdir / f"minutes_{meeting_date}.pdf"

        if save_path.exists():
            fsize = save_path.stat().st_size
        else:
            try:
                r = session.get(url, timeout=30)
                if r.status_code == 200 and len(r.content) > 500:
                    with open(save_path, 'wb') as f:
                        f.write(r.content)
                    fsize = len(r.content)
                else:
                    failed += 1
                    if idx % 25 == 0 or idx <= 5:
                        print(f"  [{idx}/{len(todo)}] {meeting_date}: not found ({r.status_code})")
                    continue
            except Exception as e:
                failed += 1
                continue

        c.execute("""INSERT OR REPLACE INTO meeting_downloads
            (meeting_id, doc_type, filename, file_path, file_size, download_url, downloaded_at)
            VALUES (?, 'minutes', ?, ?, ?, ?, ?)""",
            (meeting_id, save_path.name, str(save_path), fsize, url,
             datetime.now().isoformat()))
        conn.commit()
        downloaded += 1

        if idx % 25 == 0:
            print(f"  [{idx}/{len(todo)}] {downloaded} downloaded, {failed} not found")

        time.sleep(0.05)

    print(f"\nDone: {downloaded} Minutes downloaded, {failed} not found")
    conn.close()


if __name__ == '__main__':
    main()
