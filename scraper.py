#!/usr/bin/env python3
"""
NH Governor & Executive Council Meeting Scraper

Scrapes all G&C meeting agendas from the NH Secretary of State website
(https://www.sos.nh.gov/administration/governor-executive-council/meetings)
and extracts individual agenda items into a SQLite database.

Uses Playwright with a visible Chrome browser to bypass Akamai CDN blocking.
Data goes back to January 2012.
"""

import json
import re
import sqlite3
import sys
import time
import html
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

DB_PATH = Path(__file__).parent / "executive_council.db"
MEETINGS_CACHE = Path(__file__).parent / "cache" / "meetings_list.json"
BASE_URL = "https://www.sos.nh.gov"


def create_database():
    """Create the SQLite database schema."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS meetings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nid INTEGER UNIQUE,
            title TEXT,
            meeting_date DATE,
            url TEXT,
            scraped_at TIMESTAMP,
            item_count INTEGER DEFAULT 0
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS agenda_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meeting_id INTEGER REFERENCES meetings(id),
            item_number TEXT,
            sub_item TEXT,
            section TEXT,
            department TEXT,
            sub_department TEXT,
            description TEXT,
            amount REAL,
            amount_text TEXT,
            vendor TEXT,
            vendor_city TEXT,
            vendor_state TEXT,
            funding_source TEXT,
            effective_date_start TEXT,
            effective_date_end TEXT,
            item_type TEXT,
            is_consent_calendar INTEGER DEFAULT 0,
            is_tabled INTEGER DEFAULT 0,
            is_late_item INTEGER DEFAULT 0,
            download_url TEXT,
            business_record_url TEXT,
            raw_text TEXT,
            UNIQUE(meeting_id, item_number, sub_item)
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_items_meeting ON agenda_items(meeting_id)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_items_department ON agenda_items(department)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_items_vendor ON agenda_items(vendor)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_items_amount ON agenda_items(amount)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_items_type ON agenda_items(item_type)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_meetings_date ON meetings(meeting_date)
    """)

    conn.commit()
    return conn


def fetch_all_meeting_urls(page):
    """Fetch all meeting URLs from the API."""
    if MEETINGS_CACHE.exists():
        with open(MEETINGS_CACHE) as f:
            meetings = json.load(f)
        print(f"Loaded {len(meetings)} meetings from cache")
        return meetings

    print("Fetching meeting list from API...")
    page.goto(f"{BASE_URL}/administration/governor-executive-council/meetings", timeout=30000)
    page.wait_for_timeout(3000)

    # Get total count first
    result_text = page.evaluate("""
        async () => {
            const params = new URLSearchParams({
                q: '@field_categories|=|1036',
                sort: 'field_date|desc',
                size: '5',
                show_body: '0',
                show_date: '1',
                show_audio: '1',
                link_to_content: '1',
                page: '1'
            });
            const resp = await fetch('/content/api/meetings?' + params.toString());
            return await resp.text();
        }
    """)
    first_page = json.loads(result_text)
    total = first_page['total']
    last_page = first_page['last_page']
    print(f"Total meetings: {total}, pages: {last_page}")

    all_meetings = []
    for pg in range(1, last_page + 1):
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

        if pg % 10 == 0:
            print(f"  Page {pg}/{last_page} ({len(all_meetings)} meetings)")

    # Cache results
    MEETINGS_CACHE.parent.mkdir(exist_ok=True)
    with open(MEETINGS_CACHE, 'w') as f:
        json.dump(all_meetings, f, indent=2)
    print(f"Collected {len(all_meetings)} meetings")
    return all_meetings


def clean_text(text):
    """Clean HTML entities and whitespace from text."""
    if not text:
        return ''
    text = html.unescape(text)
    text = re.sub(r'\xa0', ' ', text)  # non-breaking space
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def extract_amount(text):
    """Extract dollar amount from text."""
    # Match patterns like $1,482.75 or $900,000 or $25,000,000.00
    amounts = re.findall(r'\$[\d,]+(?:\.\d{2})?', text)
    if amounts:
        # Return the largest amount found (usually the total)
        parsed = []
        for a in amounts:
            val = float(a.replace('$', '').replace(',', ''))
            parsed.append(val)
        return max(parsed), amounts[-1] if len(amounts) == 1 else ', '.join(amounts)
    return None, None


def extract_vendor(text):
    """Extract vendor name, city, and state from contract text."""
    # Common patterns:
    # "contract with VENDOR NAME, CITY, STATE,"
    # "agreement with VENDOR NAME, CITY, STATE,"
    # Vendor names are capped at ~120 chars to avoid runaway matches on broken data
    patterns = [
        r'(?:contract|agreement|grant|award)\s+(?:with|to)\s+(?:the\s+)?(.{3,120}?),\s+([A-Za-z\s.]{2,30}),\s+([A-Z]{2})\b',
        r'(?:to enter into a (?:sole[- ]source )?(?:contract|agreement))\s+with\s+(.{3,120}?),\s+([A-Za-z\s.]{2,30}),\s+([A-Z]{2})\b',
        r'(?:contract amendment with|amend a (?:contract|grant)(?: agreement)? (?:with|for))\s+(?:the\s+)?(.{3,120}?),\s+([A-Za-z\s.]{2,30}),\s+([A-Z]{2})\b',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            vendor = clean_text(match.group(1))
            city = clean_text(match.group(2))
            state = match.group(3)
            # Reject if vendor contains item markers (sign of broken parsing)
            if re.search(r'#\d+\s', vendor):
                continue
            # Clean up vendor name - remove leading articles
            vendor = re.sub(r'^(the|a|an)\s+', '', vendor, flags=re.IGNORECASE)
            return vendor, city, state

    # Fallback: "contract with VENDOR for/to/,..." (no city/state)
    fallback = re.search(
        r'(?:contract|agreement|grant|award)\s+(?:with|to)\s+(?:the\s+)?([A-Z][A-Za-z\s&.\'-]{2,80}?)(?:\s+for\s|\s+to\s|,\s+(?:for|to|by|effective|in|of|providing))',
        text, re.IGNORECASE
    )
    if fallback:
        vendor = clean_text(fallback.group(1))
        if not re.search(r'#\d+\s', vendor):
            vendor = re.sub(r'^(the|a|an)\s+', '', vendor, flags=re.IGNORECASE)
            return vendor, None, None

    return None, None, None


def extract_dates(text):
    """Extract effective date range from text."""
    # "Effective upon G&C approval for the period DATE through DATE"
    # "from DATE to DATE"
    date_pattern = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}'

    # Look for "period ... through ..."
    period_match = re.search(
        rf'(?:for the period|from)\s+({date_pattern})\s+(?:through|to|thru)\s+({date_pattern})',
        text, re.IGNORECASE
    )
    if period_match:
        return period_match.group(1), period_match.group(2)

    # Look for "through DATE"
    through_match = re.search(rf'through\s+({date_pattern})', text, re.IGNORECASE)
    if through_match:
        return None, through_match.group(1)

    return None, None


def classify_item(text, section):
    """Classify the type of agenda item."""
    text_lower = text.lower()
    if 'expenditure approval' in section.lower() or 'tuition agreement' in text_lower:
        return 'expenditure'
    if 'acceptance' in section.lower() or 'accept a donation' in text_lower:
        return 'acceptance'
    if 'nomination' in section.lower() or 'confirmation' in section.lower() or 'appoint' in text_lower:
        return 'nomination'
    if 'report' in section.lower() or 'finding' in section.lower():
        return 'report'
    if any(w in text_lower for w in ['contract', 'sole source', 'sole-source']):
        return 'contract'
    if 'amend' in text_lower:
        return 'amendment'
    if 'grant' in text_lower:
        return 'grant'
    if 'lease' in text_lower:
        return 'lease'
    if 'settlement' in text_lower:
        return 'settlement'
    if 'waiver' in text_lower:
        return 'waiver'
    if 'permit' in text_lower:
        return 'permit'
    if any(w in text_lower for w in ['authorize to pay', 'authorize payment', 'dues']):
        return 'payment'
    if 'reclassif' in text_lower or 'position' in text_lower:
        return 'personnel'
    if 'transfer' in text_lower:
        return 'transfer'
    return 'other'


def extract_funding_source(text):
    """Extract funding source information."""
    # Match patterns like "100% Federal Funds" or "27.49% Federal, 67.95% General, 4.56% Other"
    funding = re.findall(r'[\d.]+%\s+[A-Za-z\s()]+(?:Funds?)?', text)
    if funding:
        return '; '.join(f.strip() for f in funding)

    # Look for bold text at end (funding is often in bold)
    bold_match = re.findall(r'<strong>([\d.]+%[^<]+)</strong>', text)
    if bold_match:
        return '; '.join(clean_text(b) for b in bold_match)

    return None


def parse_meeting_page(page_text, page_html):
    """Parse a meeting agenda page and extract all items.

    The page has two main formats:
    1. Consent calendar items (#1-#5) with MOP section headers and sub-letters (A., B., C.):
       #1    MOP 150, I, B (1): Expenditure Approvals
           DEPARTMENT NAME
       A.    Description text...
       B.    Description text...

    2. Regular/numbered items (#6+) where description follows the number directly:
       DEPARTMENT NAME
       #6    Authorize to enter into a contract with VENDOR, CITY, STATE...
       #7    Authorize to...

    Some items like #13A have a sub-letter appended to the number.
    """
    items = []
    lines = page_text.split('\n')

    current_section = ''
    current_department = ''
    current_sub_dept = ''
    is_consent = False
    is_tabled_section = False
    is_late_items = False
    last_dept_before_item = ''

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

        # Detect department names (all-caps lines containing key words)
        if re.match(r'^[A-Z][A-Z\s,&.\'-]+$', line) and len(line) > 10:
            dept_keywords = ['DEPARTMENT', 'OFFICE', 'COMMISSION', 'AUTHORITY', 'DIVISION',
                             'SERVICES', 'TREASURY', 'GUARD', 'JUSTICE', 'COURT', 'POLICE',
                             'GAME', 'SAFETY', 'LABOR', 'EDUCATION', 'RESOURCES',
                             'TRANSPORTATION', 'CORRECTIONS', 'INSURANCE', 'VETERANS',
                             'MILITARY', 'AGRICULTURE', 'LOTTERY', 'ENVIRONMENTAL',
                             'GOVERNOR', 'ADMINISTRATIVE', 'LIQUOR', 'FISH', 'COUNCIL',
                             'EMPLOYMENT', 'REVENUE', 'PERSONNEL', 'PROFESSIONAL',
                             'COMMUNITY', 'POSTSECONDARY', 'INFORMATION', 'TECHNOLOGY',
                             'RETIREMENT', 'BANKING', 'PUBLIC', 'JUDICIAL', 'NATURAL',
                             'CULTURAL', 'RACING', 'LANDS', 'STATE', 'DEVELOPMENT',
                             'PARI-MUTUEL', 'EQUALIZATION', 'ADJUTANT']
            if any(kw in line.upper() for kw in dept_keywords):
                current_department = clean_text(line)
                current_sub_dept = ''
                i += 1
                continue

        # Detect sub-department lines
        sub_dept_match = re.match(r'^\s*((?:Office of|Division of|Bureau of|Board of|New Hampshire)[^.#]+)\s*$', line, re.IGNORECASE)
        if sub_dept_match and not re.match(r'^[A-Z]\.\s', line) and not re.match(r'^#', line):
            current_sub_dept = clean_text(sub_dept_match.group(1))
            i += 1
            continue

        # Match item line: #N or #NA (number with optional letter suffix)
        # Some years use 2+ spaces, others use single space
        item_match = re.match(r'^#(\d+)([A-Z])?\s+(.*)', line)
        if item_match:
            item_num = item_match.group(1)
            item_suffix = item_match.group(2) or ''
            item_text = item_match.group(3).strip()

            # Check if this is a MOP section header (consent calendar categories)
            if item_text.startswith('MOP ') or item_text.startswith('RSA '):
                current_section = item_text
                i += 1
                continue

            # Check if this is a CONSENT CALENDAR section header
            if 'CONSENT CALENDAR' in item_text.upper():
                current_section = item_text
                is_consent = True
                i += 1
                continue

            # Check if description is "- NONE" or empty
            if not item_text or item_text == '- NONE' or item_text.strip() == 'NONE':
                i += 1
                continue

            # This is an actual agenda item with description
            # Collect continuation lines (descriptions can wrap across multiple lines)
            full_desc = item_text
            j = i + 1
            saw_empty = False
            while j < len(lines):
                next_line = lines[j].strip()
                # Stop conditions - next item
                if re.match(r'^#\d+', next_line):
                    break
                # Stop - next sub-item
                if re.match(r'^[A-Z]\.\s{2,}', next_line) or re.match(r'^[A-Z]\.\s+Authorize', next_line):
                    break
                # Skip download/info lines
                if next_line == 'Download' or next_line.startswith('Download '):
                    j += 1
                    saw_empty = True  # Treat as break point
                    continue
                if next_line.startswith('For additional publicly-available'):
                    j += 1
                    saw_empty = True
                    continue
                if next_line.startswith('Supplemental Information'):
                    j += 1
                    continue
                # Empty line
                if not next_line:
                    saw_empty = True
                    j += 1
                    continue
                # After we've seen a Download or empty line, check if next content
                # is a department header or a new section
                if saw_empty:
                    # Department header for next item
                    if re.match(r'^[A-Z][A-Z\s,&.\'-]+$', next_line) and len(next_line) > 10:
                        break
                    if re.match(r'^\s*((?:Office of|Division of|Bureau of|Board of)[^.#]+)\s*$', next_line, re.IGNORECASE):
                        break
                    if 'TABLED ITEMS' in next_line.upper() or 'LATE ITEMS' in next_line.upper():
                        break
                    if 'REGULAR AGENDA' in next_line.upper() or 'REGULAR CALENDAR' in next_line.upper():
                        break
                    if re.match(r'^\*+$', next_line):
                        break
                    # If we saw empty + Download already, new text is likely continuation
                    # only if it doesn't look like a new department
                    if saw_empty and not full_desc.rstrip().endswith('.'):
                        # Description continuation (wrapped text)
                        full_desc += ' ' + next_line
                        saw_empty = False
                        j += 1
                        continue
                    else:
                        break
                # Department header check even without empty line
                if re.match(r'^[A-Z][A-Z\s,&.\'-]+$', next_line) and len(next_line) > 10:
                    dept_kw = ['DEPARTMENT', 'OFFICE', 'COMMISSION', 'AUTHORITY', 'DIVISION',
                               'SERVICES', 'TREASURY', 'JUSTICE', 'SAFETY', 'TRANSPORTATION']
                    if any(kw in next_line for kw in dept_kw):
                        break
                # Regular continuation
                full_desc += ' ' + next_line
                j += 1

            full_desc = clean_text(full_desc)
            # Remove trailing Download references
            full_desc = re.sub(r'\s*Download\s*\.?pdf\s*', ' ', full_desc)
            full_desc = re.sub(r'\s*For additional publicly-available information:.*$', '', full_desc)
            full_desc = clean_text(full_desc)

            # For sub-lettered items on consent calendar, use the suffix
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

        # Match consent calendar sub-items: A.    Description text... or A. Description...
        sub_match = re.match(r'^([A-Z])\.\s+(.*)', line)
        if sub_match:
            sub_letter = sub_match.group(1)
            desc_start = sub_match.group(2)

            # Collect full description
            full_desc = desc_start
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if re.match(r'^[A-Z]\.\s{2,}', next_line):
                    break
                if re.match(r'^#\d+', next_line):
                    break
                if not next_line:
                    break
                if re.match(r'^[A-Z][A-Z\s,&.\'-]+$', next_line) and len(next_line) > 10:
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
                'item_number': '',  # Will be set to parent item number
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
                'is_consent_calendar': 1 if is_consent else 0,
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


def scrape_meeting(page, meeting_url, meeting_nid, conn):
    """Scrape a single meeting page and store items in the database."""
    full_url = f"{BASE_URL}{meeting_url}" if meeting_url.startswith('/') else meeting_url

    try:
        page.goto(full_url, timeout=30000)
        page.wait_for_timeout(2000)
    except Exception as e:
        print(f"  ERROR loading {full_url}: {e}")
        return 0

    page_text = page.inner_text('body')
    page_html = page.content()

    items = parse_meeting_page(page_text, page_html)

    # Store items
    c = conn.cursor()
    c.execute("SELECT id FROM meetings WHERE nid = ?", (meeting_nid,))
    row = c.fetchone()
    if not row:
        return 0
    meeting_id = row[0]

    stored = 0
    for item in items:
        try:
            c.execute("""
                INSERT OR REPLACE INTO agenda_items
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
            stored += 1
        except Exception as e:
            pass  # Skip duplicates silently

    # Update meeting item count
    c.execute("UPDATE meetings SET item_count = ?, scraped_at = ? WHERE id = ?",
              (stored, datetime.now().isoformat(), meeting_id))
    conn.commit()

    return stored


def main():
    """Main scraper entry point."""
    # Optional: resume from a specific year
    start_year = int(sys.argv[1]) if len(sys.argv) > 1 else None

    conn = create_database()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        # Step 1: Get all meeting URLs
        meetings = fetch_all_meeting_urls(page)
        print(f"\nTotal meetings to process: {len(meetings)}")

        # Store meetings in DB
        c = conn.cursor()
        for m in meetings:
            try:
                c.execute("""
                    INSERT OR IGNORE INTO meetings (nid, title, meeting_date, url)
                    VALUES (?, ?, ?, ?)
                """, (m['nid'], m['title'], m['date'], m['url']))
            except:
                pass
        conn.commit()

        # Step 2: Scrape each meeting page
        # Filter by year if specified
        if start_year:
            meetings = [m for m in meetings if m['date'][:4] >= str(start_year)]
            print(f"Filtered to {len(meetings)} meetings from {start_year}+")

        # Skip already-scraped meetings
        c.execute("SELECT nid FROM meetings WHERE scraped_at IS NOT NULL AND item_count > 0")
        scraped_nids = {row[0] for row in c.fetchall()}
        to_scrape = [m for m in meetings if m['nid'] not in scraped_nids]
        print(f"Already scraped: {len(scraped_nids)}, remaining: {len(to_scrape)}")

        for idx, meeting in enumerate(to_scrape):
            url = meeting['url']
            if not url:
                print(f"  [{idx+1}/{len(to_scrape)}] {meeting['title']} - NO URL, skipping")
                continue

            print(f"  [{idx+1}/{len(to_scrape)}] {meeting['title']}...", end=' ', flush=True)

            count = scrape_meeting(page, url, meeting['nid'], conn)
            print(f"{count} items")

            # Small delay to be respectful
            time.sleep(1)

        browser.close()

    # Print summary
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM meetings WHERE scraped_at IS NOT NULL")
    scraped = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM agenda_items")
    total_items = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM agenda_items WHERE item_type = 'contract'")
    contracts = c.fetchone()[0]
    c.execute("SELECT SUM(amount) FROM agenda_items WHERE amount IS NOT NULL")
    total_amount = c.fetchone()[0] or 0

    print(f"\n{'='*60}")
    print(f"SCRAPE COMPLETE")
    print(f"  Meetings scraped: {scraped}")
    print(f"  Total agenda items: {total_items}")
    print(f"  Contracts: {contracts}")
    print(f"  Total dollar value: ${total_amount:,.2f}")
    print(f"  Database: {DB_PATH}")
    print(f"{'='*60}")

    conn.close()


if __name__ == '__main__':
    main()
