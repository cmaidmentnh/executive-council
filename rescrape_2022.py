#!/usr/bin/env python3
"""
Targeted re-scrape of 18 partially-scraped 2022 meetings + March 4, 2026.

Problem: These meetings use "N. Description" format instead of "#N Description".
The original scraper's regex (^#(\d+)...) missed all numbered items.
Only late items (A-I) were captured via the sub-item regex.

This script has a custom parser for the "N." format.
"""

import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

from scraper import clean_text, extract_amount, extract_vendor, extract_dates, \
    extract_funding_source, classify_item

DB_PATH = Path(__file__).parent / "executive_council.db"
BASE_URL = "https://www.sos.nh.gov"
DEBUG_DIR = Path(__file__).parent / "debug_pages"

# All 18 affected 2022 meetings + March 4, 2026
MEETINGS_TO_RESCRAPE = [
    (95, "/meeting/january-12-2022"),
    (94, "/meeting/january-26-2022"),
    (93, "/meeting/february-16-2022"),
    (92, "/meeting/march-9-2022"),
    (91, "/march-23-2022"),
    (90, "/meeting/april-6-2022"),
    (89, "/meeting/april-20-2022"),
    (88, "/meeting/may-4-2022"),
    (87, "/meeting/may-18-2022"),
    (86, "/meeting/june-1-2022"),
    (85, "/meeting/june-15-2022"),
    (84, "/meeting/june-29-2022"),
    (83, "/meeting/july-12-2022"),
    (82, "/meeting/july-27-2022"),
    (81, "/meeting/august-17-2022"),
    (80, "/meeting/september-7-2022"),
    (79, "/meeting/september-21-2022"),
    (78, "/meeting/october-4-2022"),
    (1, "/meeting/march-4-2026-gc-agenda"),
]


def parse_meeting_dot_format(page_text, page_html):
    """Parse meetings that use 'N. Description' format instead of '#N Description'.

    In 2022, the SoS website used this format:
      6. Authorize to hold a Public Hearing...
      7. Authorize to amend an existing contract...
      8A. Authorize to enter into a sole source...

    Consent calendar sections:
      2. MOP 150, I, B (2): Approval of Acceptances
      A. Description...
      B. Description...

    This is different from other years that use:
      #6    Authorize to hold a Public Hearing...
    """
    items = []
    lines = page_text.split('\n')

    current_section = ''
    current_department = ''
    current_sub_dept = ''
    current_consent_num = ''  # Track parent MOP section number for consent sub-items
    is_consent = False
    is_tabled_section = False
    is_late_items = False

    # Department keywords for detection
    dept_keywords = [
        'DEPARTMENT', 'OFFICE', 'COMMISSION', 'AUTHORITY', 'DIVISION',
        'SERVICES', 'TREASURY', 'GUARD', 'JUSTICE', 'COURT', 'POLICE',
        'GAME', 'SAFETY', 'LABOR', 'EDUCATION', 'RESOURCES',
        'TRANSPORTATION', 'CORRECTIONS', 'INSURANCE', 'VETERANS',
        'MILITARY', 'AGRICULTURE', 'LOTTERY', 'ENVIRONMENTAL',
        'GOVERNOR', 'ADMINISTRATIVE', 'LIQUOR', 'FISH', 'COUNCIL',
        'EMPLOYMENT', 'REVENUE', 'PERSONNEL', 'PROFESSIONAL',
        'COMMUNITY', 'POSTSECONDARY', 'INFORMATION', 'TECHNOLOGY',
        'RETIREMENT', 'BANKING', 'PUBLIC', 'JUDICIAL', 'NATURAL',
        'CULTURAL', 'RACING', 'LANDS', 'STATE', 'DEVELOPMENT',
        'PARI-MUTUEL', 'EQUALIZATION', 'ADJUTANT', 'CHILD ADVOCATE',
        'RECOVERY', 'HOUSING', 'ENERGY', 'FINANCE'
    ]

    def is_dept_line(line):
        """Check if a line is a department header (all-caps with dept keywords)."""
        if not re.match(r'^[A-Z][A-Z\s,&.\'\-/()]+$', line):
            return False
        if len(line) < 10:
            return False
        return any(kw in line.upper() for kw in dept_keywords)

    def is_sub_dept_line(line):
        """Check if line is a sub-department."""
        return bool(re.match(
            r'^\s*((?:Office of|Division of|Bureau of|Board of|New Hampshire)[^.#]+)\s*$',
            line, re.IGNORECASE
        ))

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Track section markers
        if 'CONSENT CALENDAR' in line.upper():
            is_consent = True
        if re.match(r'^\*+$', line) or 'REGULAR CALENDAR' in line.upper() or 'REGULAR AGENDA' in line.upper():
            is_consent = False
        if 'TABLED ITEMS' in line.upper() or 'TABLED ITEM' in line.upper():
            is_tabled_section = True
        if 'LATE ITEMS' in line.upper() or 'LATE ITEM' in line.upper():
            is_late_items = True

        # Detect department names
        if is_dept_line(line):
            current_department = clean_text(line)
            current_sub_dept = ''
            i += 1
            continue

        # Detect sub-department lines
        if is_sub_dept_line(line) and not re.match(r'^[A-Z]\.\s', line) and not re.match(r'^\d+', line):
            current_sub_dept = clean_text(line)
            i += 1
            continue

        # Match numbered item in multiple formats:
        # "#N Description" (hash format)
        # "N. Description" or "N.Description" (dot format)
        # "N<spaces>Description" (space format, 2+ spaces)
        # With optional letter suffix: #NA, NA., NA<spaces>
        item_match = re.match(r'^#?(\d+)([A-Z])?(?:\.\s*|\s{2,})(.*)', line)
        if item_match:
            item_num = item_match.group(1)
            item_suffix = item_match.group(2) or ''
            item_text = item_match.group(3).strip()

            # Skip MOP section headers (consent calendar categories)
            if item_text.startswith('MOP ') or item_text.startswith('RSA ') or 'MOP 150' in item_text:
                current_section = item_text
                current_consent_num = item_num  # Save for sub-items
                # Check if this is a "NONE" section
                if '- NONE' in item_text or 'NONE' in item_text.split(':')[-1].strip():
                    i += 1
                    continue
                i += 1
                continue

            # Skip CONSENT CALENDAR header
            if 'CONSENT CALENDAR' in item_text.upper():
                current_section = item_text
                is_consent = True
                i += 1
                continue

            # Skip empty/NONE items
            if not item_text or item_text == '- NONE' or item_text.strip() == 'NONE':
                i += 1
                continue

            # Collect continuation lines
            full_desc = item_text
            j = i + 1
            saw_break = False
            while j < len(lines):
                next_line = lines[j].strip()

                # Stop: next numbered item (any format)
                if re.match(r'^#?\d+[A-Z]?(?:\.\s*|\s{2,})\w', next_line):
                    break
                # Stop: next sub-item letter
                if re.match(r'^[A-Z]\.\s+', next_line) and len(next_line) > 5:
                    break
                # Skip download lines
                if next_line == 'Download' or next_line.startswith('Download '):
                    j += 1
                    saw_break = True
                    continue
                if next_line.startswith('For additional publicly-available'):
                    j += 1
                    saw_break = True
                    continue
                if next_line.startswith('Supplemental Information'):
                    j += 1
                    continue
                # Empty line
                if not next_line:
                    saw_break = True
                    j += 1
                    continue
                # After a break (Download/empty), check if next content is a new section
                if saw_break:
                    if is_dept_line(next_line):
                        break
                    if is_sub_dept_line(next_line):
                        break
                    if 'TABLED ITEMS' in next_line.upper() or 'LATE ITEMS' in next_line.upper():
                        break
                    if 'REGULAR AGENDA' in next_line.upper() or 'REGULAR CALENDAR' in next_line.upper():
                        break
                    if re.match(r'^\*+$', next_line):
                        break
                    # After download/empty, probably a new context - stop
                    break
                # Dept header without empty line
                if is_dept_line(next_line):
                    break
                # Regular continuation
                full_desc += ' ' + next_line
                j += 1

            full_desc = clean_text(full_desc)
            full_desc = re.sub(r'\s*Download\s*\.?pdf\s*', ' ', full_desc)
            full_desc = re.sub(r'\s*For additional publicly-available information:.*$', '', full_desc)
            full_desc = clean_text(full_desc)

            if not full_desc:
                i = j
                continue

            sub_item = item_suffix
            amount, amount_text = extract_amount(full_desc)
            vendor, vendor_city, vendor_state = extract_vendor(full_desc)
            start_date, end_date = extract_dates(full_desc)
            funding = extract_funding_source(full_desc)
            item_type = classify_item(full_desc, current_section)
            is_tabled = is_tabled_section or 'TABLED' in full_desc.upper()[:20]

            # Find download URL from HTML
            search_key = f'{item_num.zfill(2)}{item_suffix or ""}'
            dl_match = re.search(
                rf'href="(https://media\.sos\.nh\.gov/[^"]*?{search_key}[^"]*?\.pdf)"',
                page_html, re.IGNORECASE
            )
            download_url = dl_match.group(1) if dl_match else None

            items.append({
                'item_number': item_num,
                'sub_item': sub_item,
                'section': current_section,
                'department': current_department,
                'sub_department': current_sub_dept,
                'description': full_desc,
                'amount': amount,
                'amount_text': amount_text,
                'vendor': vendor,
                'vendor_city': vendor_city,
                'vendor_state': vendor_state,
                'funding_source': funding,
                'effective_date_start': start_date,
                'effective_date_end': end_date,
                'item_type': item_type,
                'is_consent_calendar': 1 if is_consent else 0,
                'is_tabled': 1 if is_tabled else 0,
                'is_late_item': 1 if is_late_items else 0,
                'download_url': download_url,
                'business_record_url': None,
                'raw_text': full_desc
            })

            i = j
            continue

        # Match consent calendar sub-items: A. Description...
        sub_match = re.match(r'^([A-Z])\.\s+(.*)', line)
        if sub_match and is_consent:
            sub_letter = sub_match.group(1)
            desc_start = sub_match.group(2).strip()

            if not desc_start:
                i += 1
                continue

            # Collect full description
            full_desc = desc_start
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if re.match(r'^[A-Z]\.\s+', next_line) and len(next_line) > 5:
                    break
                if re.match(r'^\d+[A-Z]?\.\s+', next_line):
                    break
                if not next_line:
                    break
                if is_dept_line(next_line):
                    break
                if next_line == 'Download' or next_line.startswith('Download '):
                    j += 1
                    continue
                if next_line.startswith('For additional publicly-available'):
                    j += 1
                    continue
                full_desc += ' ' + next_line
                j += 1

            full_desc = clean_text(full_desc)
            full_desc = re.sub(r'\s*Download\s*\.?pdf\s*', ' ', full_desc)
            full_desc = re.sub(r'\s*For additional publicly-available information:.*$', '', full_desc)
            full_desc = clean_text(full_desc)

            if not full_desc:
                i = j
                continue

            amount, amount_text = extract_amount(full_desc)
            vendor, vendor_city, vendor_state = extract_vendor(full_desc)
            start_date, end_date = extract_dates(full_desc)
            funding = extract_funding_source(full_desc)
            item_type = classify_item(full_desc, current_section)

            items.append({
                'item_number': current_consent_num,  # Parent MOP section number
                'sub_item': sub_letter,
                'section': current_section,
                'department': current_department,
                'sub_department': current_sub_dept,
                'description': full_desc,
                'amount': amount,
                'amount_text': amount_text,
                'vendor': vendor,
                'vendor_city': vendor_city,
                'vendor_state': vendor_state,
                'funding_source': funding,
                'effective_date_start': start_date,
                'effective_date_end': end_date,
                'item_type': item_type,
                'is_consent_calendar': 1,
                'is_tabled': 0,
                'is_late_item': 1 if is_late_items else 0,
                'download_url': None,
                'business_record_url': None,
                'raw_text': full_desc
            })

            i = j
            continue

        i += 1

    return items


def rescrape_meeting(page, meeting_id, meeting_url, conn):
    """Re-scrape a single meeting and insert missing items."""
    full_url = f"{BASE_URL}{meeting_url}"

    try:
        page.goto(full_url, timeout=45000)
        page.wait_for_timeout(3000)
    except Exception as e:
        print(f"  ERROR loading {full_url}: {e}")
        return 0, 0

    page_text = page.inner_text('body')
    page_html = page.content()

    # Save debug text
    DEBUG_DIR.mkdir(exist_ok=True)
    slug = meeting_url.strip('/').replace('/', '_')
    with open(DEBUG_DIR / f"{slug}.txt", 'w') as f:
        f.write(page_text)

    # Always use the unified parser that handles all formats (#N, N., N<spaces>)
    items = parse_meeting_dot_format(page_text, page_html)
    print(f"  Parser found {len(items)} items")

    if not items:
        return 0, 0

    # Get existing item keys to avoid overwriting
    c = conn.cursor()
    c.execute("SELECT item_number, sub_item FROM agenda_items WHERE meeting_id = ?", (meeting_id,))
    existing_keys = {(row[0], row[1]) for row in c.fetchall()}

    inserted = 0
    skipped = 0
    for item in items:
        key = (item['item_number'], item['sub_item'])
        if key in existing_keys:
            skipped += 1
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
            skipped += 1

    # Update meeting item count
    c.execute("SELECT COUNT(*) FROM agenda_items WHERE meeting_id = ?", (meeting_id,))
    new_total = c.fetchone()[0]
    c.execute("UPDATE meetings SET item_count = ?, scraped_at = ? WHERE id = ?",
              (new_total, datetime.now().isoformat(), meeting_id))
    conn.commit()

    return inserted, skipped


def main():
    conn = sqlite3.connect(DB_PATH)

    target_ids = None
    if len(sys.argv) > 1:
        target_ids = [int(x) for x in sys.argv[1:]]

    meetings = MEETINGS_TO_RESCRAPE
    if target_ids:
        meetings = [(mid, url) for mid, url in meetings if mid in target_ids]

    print(f"Re-scraping {len(meetings)} meetings...")
    print(f"Database: {DB_PATH}")
    print()

    total_inserted = 0
    total_skipped = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            channel="chrome",
            args=[
                '--disable-blink-features=AutomationControlled',
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        # Hide webdriver property to evade bot detection
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = { runtime: {} };
        """)

        # Visit main site first for cookies
        print("Loading main site for cookies...")
        page.goto(f"{BASE_URL}/administration/governor-executive-council/meetings", timeout=30000)
        page.wait_for_timeout(3000)

        for idx, (meeting_id, meeting_url) in enumerate(meetings):
            c = conn.cursor()
            c.execute("SELECT meeting_date FROM meetings WHERE id = ?", (meeting_id,))
            row = c.fetchone()
            date_str = row[0] if row else '?'

            c.execute("SELECT COUNT(*) FROM agenda_items WHERE meeting_id = ?", (meeting_id,))
            before = c.fetchone()[0]

            print(f"[{idx+1}/{len(meetings)}] {date_str} (id={meeting_id}, {before} existing items)")

            inserted, skipped = rescrape_meeting(page, meeting_id, meeting_url, conn)

            print(f"  -> {inserted} new items inserted, {skipped} skipped")
            total_inserted += inserted
            total_skipped += skipped

            time.sleep(2)

        browser.close()

    print(f"\n{'='*60}")
    print(f"RE-SCRAPE COMPLETE")
    print(f"  Total new items: {total_inserted}")
    print(f"  Skipped (existing): {total_skipped}")
    print(f"  Debug pages saved to: {DEBUG_DIR}")
    print(f"{'='*60}")

    conn.close()


if __name__ == '__main__':
    main()
