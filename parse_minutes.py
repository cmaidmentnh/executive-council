#!/usr/bin/env python3
"""
Parse G&C Minutes PDFs to extract comprehensive council actions.

Extracts:
- Vote outcomes for each item (approved/denied/tabled/withdrawn)
- Who motioned and seconded (with councilor_id resolution)
- Dissenting votes (Councilor X voting no)
- Resignations
- Nominations
- Confirmations
- Tabled items
- Failed items
- Items removed from consent calendar

Usage:
    python3 parse_minutes.py              # Parse all downloaded Minutes
    python3 parse_minutes.py --date 2026-01-28  # Parse one meeting's Minutes
    python3 parse_minutes.py --stats      # Show what's been extracted
"""

import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pdfplumber

DB_PATH = Path(__file__).parent / "executive_council.db"
DOWNLOAD_DIR = Path(__file__).parent / "downloads"

# Regex for a councilor name: one or more capitalized words (handles "Liot Hill", "St. Hilaire", "Van Ostern")
_NAME = r'([A-Z][a-z]+(?:\.?\s+[A-Z][a-z]+)*)'

# Precompiled motion pattern
MOTION_RE = re.compile(
    rf'motion of Councilor\s+{_NAME},?\s+seconded by Councilor\s+{_NAME}',
    re.IGNORECASE
)

# Dissenter pattern — anchored on "with" to avoid capturing motion clause
DISSENT_RE = re.compile(
    r'with\s+Councilors?\s+([\w\s,.]+?)\s+voting\s+(?:no|nay|in the negative)',
    re.IGNORECASE
)

# Abstention pattern
ABSTAIN_RE = re.compile(r'Councilor\s+(\w+)\s+abstaining', re.IGNORECASE)

# Alias lookup cache (loaded once)
_ALIAS_MAP = None  # alias_lower -> councilor_id


def _load_alias_map(conn):
    """Load councilor alias map from DB. Returns {alias_lower: councilor_id}."""
    global _ALIAS_MAP
    if _ALIAS_MAP is not None:
        return _ALIAS_MAP
    c = conn.cursor()
    c.execute('SELECT alias, councilor_id FROM councilor_aliases')
    _ALIAS_MAP = {row[0].lower(): row[1] for row in c.fetchall()}
    return _ALIAS_MAP


def resolve_councilor(name, conn, meeting_date=None):
    """Resolve a councilor name to councilor_id using aliases table."""
    if not name:
        return None
    alias_map = _load_alias_map(conn)
    name_clean = name.strip()
    # Try exact match (case-insensitive)
    cid = alias_map.get(name_clean.lower())
    if cid:
        return cid
    # Try last-name-only match
    parts = name_clean.split()
    if len(parts) > 1:
        cid = alias_map.get(parts[-1].lower())
        if cid:
            return cid
    return None


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    return conn


def ensure_schema():
    conn = get_conn()
    c = conn.cursor()

    # Comprehensive actions table — one row per council action
    c.execute("""CREATE TABLE IF NOT EXISTS council_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        meeting_id INTEGER REFERENCES meetings(id),
        meeting_date DATE,
        action_type TEXT,       -- 'vote', 'resignation', 'nomination', 'confirmation',
                                -- 'civil_commission', 'ceremonial', 'minutes_approval'
        item_number TEXT,       -- NULL for non-item actions
        sub_item TEXT,
        outcome TEXT,           -- 'approved', 'denied', 'tabled', 'withdrawn', 'confirmed', etc.
        motion_by TEXT,         -- Councilor who made motion
        motion_by_id INTEGER REFERENCES councilors(id),
        seconded_by TEXT,       -- Councilor who seconded
        seconded_by_id INTEGER REFERENCES councilors(id),
        description TEXT,       -- Full text of the action
        person_name TEXT,       -- For nominations/resignations/confirmations
        person_city TEXT,
        position_title TEXT,    -- Position being filled/vacated
        rsa_reference TEXT,     -- RSA citation
        effective_date TEXT,
        term_end TEXT,
        salary TEXT,
        department TEXT,
        dissenting_votes TEXT,  -- Comma-separated councilor names who voted no
        abstaining TEXT,        -- Comma-separated councilor names who abstained
        raw_text TEXT,
        UNIQUE(meeting_id, action_type, item_number, sub_item, person_name)
    )""")

    c.execute("""CREATE INDEX IF NOT EXISTS idx_actions_meeting
                 ON council_actions(meeting_id)""")
    c.execute("""CREATE INDEX IF NOT EXISTS idx_actions_type
                 ON council_actions(action_type)""")
    c.execute("""CREATE INDEX IF NOT EXISTS idx_actions_person
                 ON council_actions(person_name)""")
    c.execute("""CREATE INDEX IF NOT EXISTS idx_actions_date
                 ON council_actions(meeting_date)""")

    conn.commit()
    conn.close()


def find_minutes_files(date_filter=None):
    """Find all downloaded Minutes PDFs."""
    conn = get_conn()
    c = conn.cursor()

    if date_filter:
        c.execute("""SELECT md.meeting_id, md.file_path, m.meeting_date
                     FROM meeting_downloads md JOIN meetings m ON md.meeting_id = m.id
                     WHERE md.doc_type = 'minutes' AND m.meeting_date = ?""", (date_filter,))
    else:
        c.execute("""SELECT md.meeting_id, md.file_path, m.meeting_date
                     FROM meeting_downloads md JOIN meetings m ON md.meeting_id = m.id
                     WHERE md.doc_type = 'minutes' ORDER BY m.meeting_date DESC""")

    results = c.fetchall()
    conn.close()
    return results


def read_minutes_text(file_path):
    """Extract text from a Minutes PDF, falling back to OCR .txt file if PDF text is empty."""
    # First try: OCR text file (same name but .txt extension)
    txt_path = Path(file_path).with_suffix('.txt')

    # Try pdfplumber first
    text = ''
    try:
        with pdfplumber.open(file_path) as pdf:
            pages = []
            for pg in pdf.pages:
                t = pg.extract_text()
                if t:
                    t = re.sub(r'Page \d+ of \d+\n?', '', t)
                    pages.append(t)
            text = '\n'.join(pages)
    except Exception:
        pass

    # If PDF text is too short (scanned image), try OCR text file
    if len(text.strip()) < 200 and txt_path.exists():
        ocr_text = txt_path.read_text(encoding='utf-8', errors='replace')
        if len(ocr_text.strip()) > len(text.strip()):
            return ocr_text

    return text


def parse_minutes(text, meeting_id, meeting_date):
    """Parse Minutes text and extract all council actions."""
    actions = []
    lines = text.split('\n')

    # Track state
    current_section = None  # 'resignations', 'confirmations', 'nominations', 'consent', 'regular'
    current_dept = ''
    consent_motion_by = None
    consent_seconded_by = None

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if not line or line == '*':
            i += 1
            continue

        # === SECTION HEADERS ===
        if line == 'RESIGNATIONS' or line.startswith('RESIGNATIONS'):
            current_section = 'resignations'
            i += 1
            # Skip "The Governor and Council accepted..."
            while i < len(lines) and not re.match(r'^[A-Z].*RSA', lines[i].strip()):
                if 'accepted the following' not in lines[i].lower():
                    break
                i += 1
            continue

        if re.match(r'^CONFIRMATION[S]?$', line) or line.startswith('CONFIRMATIONS') or 'NOMINATION & CONFIRMATION' in line.upper() or 'NOMINATION &AMP; CONFIRMATION' in line.upper():
            current_section = 'confirmations'
            i += 1
            while i < len(lines) and ('confirmed the following' in lines[i].lower() or 'voted to nominate' in lines[i].lower() or 'voted to confirm' in lines[i].lower()):
                # For inline "voted to confirm" lines, also extract the motion
                voted_line = lines[i].strip()
                motion_match = MOTION_RE.search(voted_line)
                if motion_match:
                    consent_motion_by = motion_match.group(1).strip()
                    consent_seconded_by = motion_match.group(2).strip()
                i += 1
            continue

        if re.match(r'^NOMINATION[S]?$', line) or line.startswith('NOMINATIONS'):
            current_section = 'nominations'
            i += 1
            while i < len(lines) and 'nominations were submitted' in lines[i].lower():
                i += 1
            continue

        if 'CONSENT CALENDAR' in line.upper():
            current_section = 'consent'
            i += 1
            continue

        # === MOTION LINE (for consent calendar and regular items) ===
        motion_match = MOTION_RE.search(line)
        if motion_match:
            motion_by = motion_match.group(1).strip()
            seconded_by = motion_match.group(2).strip()
            if current_section == 'consent':
                consent_motion_by = motion_by
                consent_seconded_by = seconded_by

        # === RESIGNATIONS ===
        if current_section == 'resignations':
            # Pattern: Position Title - RSA XX:XX\n Name, City  Effective: Date
            rsa_match = re.search(r'(.+?)\s*[-–—]\s*(RSA\s+[\d\w:,\-]+(?:\s*,\s*[\d\w:,\-]+)*)', line)
            if rsa_match:
                position = rsa_match.group(1).strip()
                rsa_ref = rsa_match.group(2).strip()
                # Gather next few non-blank lines
                i += 1
                raw_lines = [line]
                block_lines = []
                j = i
                while j < len(lines) and j < i + 8:
                    nl = lines[j].strip()
                    if not nl:
                        j += 1
                        continue
                    if nl == '*':
                        break
                    if re.search(r'[-–—]\s*RSA\s+\d', nl):
                        break
                    block_lines.append(nl)
                    raw_lines.append(nl)
                    j += 1

                person = None
                city = None
                eff_date = None
                block_text = ' '.join(block_lines)
                name_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s+(\w+)\s+Effective:\s+(.+?)(?:\s{2,}|$)', block_text)
                if name_match:
                    person = name_match.group(1).strip()
                    city = name_match.group(2).strip()
                    eff_date = name_match.group(3).strip()
                else:
                    for bl in block_lines:
                        if not person:
                            nm = re.match(r'^([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s+(\w+)', bl)
                            if nm:
                                person = nm.group(1).strip()
                                city = nm.group(2).strip()
                        if not eff_date:
                            em = re.search(r'Effective:\s+(.+?)(?:\s{2,}|$)', bl)
                            if em:
                                eff_date = em.group(1).strip()

                if person:
                    actions.append({
                        'action_type': 'resignation',
                        'outcome': 'accepted',
                        'person_name': person,
                        'person_city': city,
                        'position_title': position,
                        'rsa_reference': rsa_ref,
                        'effective_date': eff_date,
                        'raw_text': '\n'.join(raw_lines)
                    })
                i = j
                continue

        # === CONFIRMATIONS ===
        if current_section == 'confirmations':
            rsa_match = re.search(r'(.+?)\s*[-–—]\s*(RSA\s+[\d\w:,\-]+(?:\s*,\s*[\d\w:,\-]+)*)', line)
            if rsa_match:
                position = rsa_match.group(1).strip()
                rsa_ref = rsa_match.group(2).strip()
                # Gather all following non-blank lines into a block (up to 12 lines)
                i += 1
                raw_lines = [line]
                qualifier = ''
                block_lines = []
                j = i
                while j < len(lines) and j < i + 12:
                    nl = lines[j].strip()
                    if not nl:
                        j += 1
                        continue
                    if nl == '*':
                        break
                    # Stop if we hit another RSA line or section header
                    if re.search(r'[-–—]\s*RSA\s+\d', nl) and j > i:
                        break
                    if nl in ('RESIGNATIONS', 'NOMINATIONS', 'CONFIRMATIONS') or nl.startswith('NOMINATION'):
                        break
                    if nl.startswith('The Governor and Council') and j > i + 1:
                        break
                    if nl.startswith('Next regular') or nl.startswith('The Honorable Board') or nl.startswith('Honorable Board'):
                        break
                    # Stop if we hit an item number (#1, #A, etc.)
                    if re.match(r'^#\d+|^#[A-Z]\.?\s', nl):
                        break
                    # Stop if we hit a department header (ALL CAPS, >10 chars)
                    if re.match(r'^[A-Z][A-Z\s,&.\'-]+$', nl) and len(nl) > 15:
                        break
                    block_lines.append(nl)
                    raw_lines.append(nl)
                    j += 1

                # Parse block: look for qualifier, name, city, effective date, term, salary
                if block_lines and block_lines[0].startswith('('):
                    qualifier = block_lines.pop(0)

                person = None
                city = None
                eff_date = None
                term_end = None
                salary = None

                block_text = ' '.join(block_lines)
                # Try combined "Name, City  Effective: Date" pattern
                name_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s+(\w+)\s+Effective:\s+(.+?)(?:\s{2,}|$)', block_text)
                if name_match:
                    person = name_match.group(1).strip()
                    city = name_match.group(2).strip()
                    eff_date = name_match.group(3).strip()
                else:
                    # Try split across lines: "Name, City" on one line, "Effective:" on another
                    for bl in block_lines:
                        if not person:
                            nm = re.match(r'^([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?(?:\s+[IVXLCDM]+\.?)?),\s+(\w+)', bl)
                            if nm:
                                person = nm.group(1).strip()
                                city = nm.group(2).strip()
                        if not eff_date:
                            em = re.search(r'Effective:\s+(.+?)(?:\s{2,}|$)', bl)
                            if em:
                                eff_date = em.group(1).strip()
                        tm = re.search(r'Term:\s+(.+?)(?:\s{2,}|$)', bl)
                        if tm:
                            term_end = tm.group(1).strip()
                        sm = re.search(r'(?:Salary|Grade\s+\w+).*?:\s*\$?([\d,]+)', bl)
                        if sm:
                            salary = sm.group(0).strip()

                if person:
                    actions.append({
                        'action_type': 'confirmation',
                        'outcome': 'confirmed',
                        'person_name': person,
                        'person_city': city,
                        'position_title': f"{position} {qualifier}".strip(),
                        'rsa_reference': rsa_ref,
                        'effective_date': eff_date,
                        'term_end': term_end,
                        'salary': salary,
                        'raw_text': '\n'.join(raw_lines)
                    })
                i = j
                continue

        # === NOMINATIONS ===
        if current_section == 'nominations':
            rsa_match = re.search(r'(.+?)\s*[-–—]\s*(RSA\s+[\d\w:,\-]+(?:\s*,\s*[\d\w:,\-]+)*)', line)
            if rsa_match:
                position = rsa_match.group(1).strip()
                rsa_ref = rsa_match.group(2).strip()
                i += 1
                raw_lines = [line]
                block_lines = []
                qualifier = ''
                j = i
                while j < len(lines) and j < i + 12:
                    nl = lines[j].strip()
                    if not nl:
                        j += 1
                        continue
                    if nl == '*':
                        break
                    if re.search(r'[-–—]\s*RSA\s+\d', nl):
                        break
                    if nl in ('RESIGNATIONS', 'NOMINATIONS', 'CONFIRMATIONS') or nl.startswith('NOMINATION'):
                        break
                    if nl.startswith('The Governor and Council') and j > i + 1:
                        break
                    if nl.startswith('Next regular') or nl.startswith('The Honorable Board') or nl.startswith('Honorable Board'):
                        break
                    block_lines.append(nl)
                    raw_lines.append(nl)
                    j += 1

                if block_lines and block_lines[0].startswith('('):
                    qualifier = block_lines.pop(0)

                person = None
                city = None
                eff_date = None
                term_end = None
                salary = None

                block_text = ' '.join(block_lines)
                name_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s+(\w+)\s+Effective:\s+(.+?)(?:\s{2,}|$)', block_text)
                if name_match:
                    person = name_match.group(1).strip()
                    city = name_match.group(2).strip()
                    eff_date = name_match.group(3).strip()
                else:
                    for bl in block_lines:
                        if not person:
                            nm = re.match(r'^([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?(?:\s+[IVXLCDM]+\.?)?),\s+(\w+)', bl)
                            if nm:
                                person = nm.group(1).strip()
                                city = nm.group(2).strip()
                        if not eff_date:
                            em = re.search(r'Effective:\s+(.+?)(?:\s{2,}|$)', bl)
                            if em:
                                eff_date = em.group(1).strip()
                        tm = re.search(r'Term:\s+(.+?)(?:\s{2,}|$)', bl)
                        if tm:
                            term_end = tm.group(1).strip()
                        sm = re.search(r'(?:Salary|Grade\s+\w+).*?:\s*\$?([\d,]+)', bl)
                        if sm:
                            salary = sm.group(0).strip()

                if person:
                    actions.append({
                        'action_type': 'nomination',
                        'outcome': 'nominated',
                        'person_name': person,
                        'person_city': city,
                        'position_title': f"{position} {qualifier}".strip(),
                        'rsa_reference': rsa_ref,
                        'effective_date': eff_date,
                        'term_end': term_end,
                        'salary': salary,
                        'raw_text': '\n'.join(raw_lines)
                    })
                i = j
                continue

        # === ITEM VOTES (#N lines or #A letter-based items) ===
        item_match = re.match(r'^#(\d+)([A-Z])?\.?\s+(.*)', line)
        # Also match letter-based items like "#A." used in special meetings
        letter_item_match = None
        if not item_match:
            letter_item_match = re.match(r'^#([A-Z])\.?\s+(.*)', line)
        if item_match or letter_item_match:
            if item_match:
                item_num = item_match.group(1)
                sub_item = item_match.group(2)
                rest = item_match.group(3)
            else:
                item_num = letter_item_match.group(1)
                sub_item = None
                rest = letter_item_match.group(2)

            # Collect the full item text (may span multiple lines)
            full_text = rest
            j = i + 1
            while j < len(lines):
                nl = lines[j].strip()
                if re.match(r'^#\d+', nl) or re.match(r'^#[A-Z]\.?\s', nl):
                    break
                if nl == '*':
                    break
                if not nl:
                    j += 1
                    continue
                # New department header
                if re.match(r'^[A-Z][A-Z\s,&.\'-]+$', nl) and len(nl) > 10:
                    dept_kws = ['DEPARTMENT', 'OFFICE', 'COMMISSION', 'DIVISION', 'TREASURY',
                                'JUSTICE', 'SAFETY', 'TRANSPORTATION', 'CORRECTIONS', 'EDUCATION']
                    if any(kw in nl for kw in dept_kws):
                        break
                full_text += ' ' + nl
                j += 1

            # Parse the vote details from the full text
            action = parse_item_vote(item_num, sub_item, full_text,
                                     consent_motion_by if current_section == 'consent' else None,
                                     consent_seconded_by if current_section == 'consent' else None)
            if action:
                actions.append(action)

            i = j
            continue

        # === DEPARTMENT LINES (track for context) ===
        if re.match(r'^[A-Z][A-Z\s,&.\'-]+$', line) and len(line) > 10:
            current_dept = line.strip()

        # === MOTION LINES WITH ITEM CONTEXT ===
        # "The Governor and Council on motion of Councilor X, seconded by Councilor Y acted as follows:"
        # These set the motion/second for the next group of items
        if 'acted as follows' in line.lower():
            motion_match = MOTION_RE.search(line)
            if motion_match:
                consent_motion_by = motion_match.group(1).strip()
                consent_seconded_by = motion_match.group(2).strip()

        # === MINUTES APPROVAL ===
        if 'approve the minutes' in line.lower() or 'accepted the minutes' in line.lower():
            motion_match = MOTION_RE.search(line)
            abstain_match = ABSTAIN_RE.search(line)
            actions.append({
                'action_type': 'minutes_approval',
                'outcome': 'approved',
                'motion_by': motion_match.group(1).strip() if motion_match else None,
                'seconded_by': motion_match.group(2).strip() if motion_match else None,
                'abstaining': abstain_match.group(1) if abstain_match else None,
                'description': line,
                'raw_text': line
            })

        # === INLINE "voted to confirm" in narrative text (special meetings) ===
        if ('voted to confirm' in line.lower() or 'voted to nominate' in line.lower()) and current_section != 'confirmations' and current_section != 'nominations':
            # Collect multi-line block
            full_block = line
            j = i + 1
            while j < len(lines) and j < i + 15:
                nl = lines[j].strip()
                if not nl or nl == '*':
                    break
                if nl.startswith('The Governor and Council') and j > i + 1:
                    break
                if nl.startswith('Next regular') or nl.startswith('The Honorable Board'):
                    break
                full_block += '\n' + nl
                j += 1

            motion_match = MOTION_RE.search(full_block)
            dissent_match = DISSENT_RE.search(full_block)
            action_type = 'confirmation' if 'confirm' in full_block.lower() else 'nomination'

            # Try to extract person name from subsequent lines
            person_name = None
            person_city = None
            position = None
            rsa_ref = None
            eff_date = None
            for bl in full_block.split('\n')[1:]:
                bl = bl.strip()
                rsa_m = re.search(r'(.+?)\s*[-–—]\s*(RSA\s+[\d\w:,-]+)', bl)
                if rsa_m:
                    position = rsa_m.group(1).strip()
                    rsa_ref = rsa_m.group(2).strip()
                    continue
                name_m = re.match(r'^([A-Z][a-z]+ (?:[A-Z]\.?\s+)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s+(\w+)', bl)
                if name_m and not person_name:
                    person_name = name_m.group(1).strip()
                    person_city = name_m.group(2).strip()
                eff_m = re.search(r'Effective:\s+(.+?)(?:\s+|$)', bl)
                if eff_m:
                    eff_date = eff_m.group(1).strip()

            actions.append({
                'action_type': action_type,
                'outcome': 'confirmed' if action_type == 'confirmation' else 'nominated',
                'motion_by': motion_match.group(1).strip() if motion_match else None,
                'seconded_by': motion_match.group(2).strip() if motion_match else None,
                'person_name': person_name,
                'person_city': person_city,
                'position_title': position,
                'rsa_reference': rsa_ref,
                'effective_date': eff_date,
                'dissenting_votes': ', '.join(re.split(r',\s*(?:and\s+)?|\s+and\s+', dissent_match.group(1).strip())) if dissent_match else None,
                'description': full_block[:500],
                'raw_text': full_block
            })

        i += 1

    return actions


def parse_item_vote(item_num, sub_item, text, default_motion_by=None, default_seconded_by=None):
    """Parse vote details from an item's text in the Minutes."""
    action = {
        'action_type': 'vote',
        'item_number': item_num,
        'sub_item': sub_item,
        'description': text[:500],
        'raw_text': text,
        'motion_by': default_motion_by,
        'seconded_by': default_seconded_by,
    }

    text_lower = text.lower()

    # Override motion/second if specified in item text
    motion_match = MOTION_RE.search(text)
    if motion_match:
        action['motion_by'] = motion_match.group(1).strip()
        action['seconded_by'] = motion_match.group(2).strip()

    # Determine outcome
    if 'FAILED' in text or 'failed to authorize' in text_lower:
        action['outcome'] = 'denied'
    elif 'TABLED' in text or 'tabled' in text_lower[:50]:
        action['outcome'] = 'tabled'
    elif 'WITHDRAWN' in text or 'withdrawn' in text_lower[:50]:
        action['outcome'] = 'withdrawn'
    elif 'NOT USED' in text:
        action['outcome'] = 'not_used'
    elif 'authorized' in text_lower or 'approved' in text_lower:
        action['outcome'] = 'approved'
    else:
        action['outcome'] = 'approved'  # default for items in minutes

    # Extract dissenting votes: "with Councilor(s) X voting no"
    # Anchored on "with" to avoid matching back into the motion clause
    dissenting = []
    for m in DISSENT_RE.finditer(text):
        names_text = m.group(1).strip()
        # Split on commas and "and"
        names = re.split(r',\s*(?:and\s+)?|\s+and\s+', names_text)
        for name in names:
            name = re.sub(r'^Councilor\s+', '', name.strip())
            name = name.strip()
            if name and len(name) > 1 and name[0].isupper():
                dissenting.append(name)
    action['dissenting_votes'] = ', '.join(dissenting) if dissenting else None

    # Extract abstentions
    abstaining = []
    for m in ABSTAIN_RE.finditer(text):
        abstaining.append(m.group(1))
    action['abstaining'] = ', '.join(abstaining) if abstaining else None

    # Check if removed from consent calendar
    if 'removed from the consent calendar' in text_lower:
        action['description'] = '[Removed from consent] ' + action['description']

    # "all Councilors voting no" = unanimous denial
    if 'all councilors voting no' in text_lower:
        action['dissenting_votes'] = 'ALL'

    return action


def save_actions(actions, meeting_id, meeting_date, conn):
    """Save parsed actions to the database, resolving councilor IDs.
    Idempotent: deletes existing actions for this meeting before inserting."""
    c = conn.cursor()
    # Delete existing actions for this meeting to prevent duplicates on re-parse
    c.execute("DELETE FROM councilor_votes WHERE vote_outcome_id IN (SELECT id FROM council_actions WHERE meeting_id = ?)", (meeting_id,))
    c.execute("DELETE FROM council_actions WHERE meeting_id = ?", (meeting_id,))
    saved = 0
    for a in actions:
        # Resolve councilor IDs
        motion_by_id = resolve_councilor(a.get('motion_by'), conn, meeting_date)
        seconded_by_id = resolve_councilor(a.get('seconded_by'), conn, meeting_date)

        # Also populate councilor_votes table for dissenters and abstainers
        try:
            c.execute("""INSERT INTO council_actions
                (meeting_id, meeting_date, action_type, item_number, sub_item,
                 outcome, motion_by, motion_by_id, seconded_by, seconded_by_id,
                 description, person_name,
                 person_city, position_title, rsa_reference, effective_date,
                 term_end, salary, department, dissenting_votes, abstaining, raw_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (meeting_id, meeting_date,
                 a.get('action_type'), a.get('item_number'), a.get('sub_item'),
                 a.get('outcome'), a.get('motion_by'), motion_by_id,
                 a.get('seconded_by'), seconded_by_id,
                 a.get('description'), a.get('person_name'), a.get('person_city'),
                 a.get('position_title'), a.get('rsa_reference'), a.get('effective_date'),
                 a.get('term_end'), a.get('salary'), a.get('department'),
                 a.get('dissenting_votes'), a.get('abstaining'),
                 a.get('raw_text', '')[:2000]))
            saved += 1

            # Insert per-councilor vote records for contested votes
            action_id = c.lastrowid
            if a.get('dissenting_votes') and a['dissenting_votes'] != 'ALL':
                for name in a['dissenting_votes'].split(', '):
                    cid = resolve_councilor(name, conn, meeting_date)
                    if cid:
                        c.execute("""INSERT OR IGNORE INTO councilor_votes
                            (vote_outcome_id, councilor_name, vote)
                            VALUES (?, ?, 'no')""", (action_id, name))
            if a.get('abstaining'):
                for name in a['abstaining'].split(', '):
                    cid = resolve_councilor(name, conn, meeting_date)
                    if cid:
                        c.execute("""INSERT OR IGNORE INTO councilor_votes
                            (vote_outcome_id, councilor_name, vote)
                            VALUES (?, ?, 'abstain')""", (action_id, name))

        except Exception as e:
            pass  # skip duplicates
    conn.commit()
    return saved


def show_stats():
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='council_actions'")
    if not c.fetchone():
        print("No actions parsed yet.")
        conn.close()
        return

    c.execute("SELECT COUNT(*) FROM council_actions")
    total = c.fetchone()[0]

    c.execute("SELECT action_type, COUNT(*) FROM council_actions GROUP BY action_type ORDER BY COUNT(*) DESC")
    types = c.fetchall()

    c.execute("SELECT COUNT(DISTINCT meeting_id) FROM council_actions")
    meetings = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM council_actions WHERE dissenting_votes IS NOT NULL")
    contested = c.fetchone()[0]

    c.execute("SELECT outcome, COUNT(*) FROM council_actions WHERE action_type='vote' GROUP BY outcome ORDER BY COUNT(*) DESC")
    outcomes = c.fetchall()

    # Top dissenters
    c.execute("""SELECT dissenting_votes, COUNT(*) as cnt FROM council_actions
                 WHERE dissenting_votes IS NOT NULL AND dissenting_votes != 'ALL'
                 GROUP BY dissenting_votes ORDER BY cnt DESC LIMIT 10""")
    dissenters = c.fetchall()

    print(f"{'='*60}")
    print(f"COUNCIL ACTIONS DATABASE")
    print(f"{'='*60}")
    print(f"Total actions:     {total}")
    print(f"Meetings parsed:   {meetings}")
    print(f"Contested votes:   {contested}")
    print()
    print(f"{'Action Type':<25} {'Count':>8}")
    print(f"{'-'*35}")
    for t, cnt in types:
        print(f"  {t:<23} {cnt:>8}")
    print()
    print(f"{'Vote Outcome':<25} {'Count':>8}")
    print(f"{'-'*35}")
    for o, cnt in outcomes:
        print(f"  {o:<23} {cnt:>8}")
    print()
    print(f"Top dissenters:")
    for name, cnt in dissenters:
        print(f"  {name:<30} {cnt:>5} votes")

    conn.close()


def main():
    ensure_schema()

    if len(sys.argv) > 1:
        if sys.argv[1] == '--stats':
            show_stats()
            return
        elif sys.argv[1] == '--date':
            date_filter = sys.argv[2] if len(sys.argv) > 2 else None
            files = find_minutes_files(date_filter)
        else:
            files = find_minutes_files()
    else:
        files = find_minutes_files()

    if not files:
        print("No Minutes PDFs found. Run download_minutes.py first.")
        return

    conn = get_conn()
    total_actions = 0

    print(f"Parsing {len(files)} Minutes PDFs...")

    for meeting_id, file_path, meeting_date in files:
        if not Path(file_path).exists():
            continue

        try:
            text = read_minutes_text(file_path)
        except Exception as e:
            print(f"  Error reading {meeting_date}: {e}")
            continue

        actions = parse_minutes(text, meeting_id, meeting_date)
        saved = save_actions(actions, meeting_id, meeting_date, conn)
        total_actions += saved

        if saved > 0:
            contested = sum(1 for a in actions if a.get('dissenting_votes'))
            print(f"  {meeting_date}: {saved} actions ({contested} contested)")

    print(f"\nTotal: {total_actions} actions extracted")
    conn.close()

    # Show summary
    show_stats()


if __name__ == '__main__':
    main()
