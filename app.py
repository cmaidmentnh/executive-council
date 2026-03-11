#!/usr/bin/env python3
"""
NH Executive Council Database — Public Web App
Flask app serving 14+ years of G&C meeting data.
"""

import sqlite3
import os
from datetime import datetime
from flask import Flask, render_template, request, jsonify, abort, Response
import csv
import io

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), "executive_council.db")
R2_BASE = "https://pub-53c5014580f5456185d5efde8511a616.r2.dev"

# SQL expression to normalize vendor names (strip comma before Inc/LLC/LLP/Corp/Co./Ltd)
# Use with .format(col='ai.vendor') or .format(col='vendor')
VENDOR_NORM_SQL = ("REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("
                   "{col}, ', Inc', ' Inc'), ', LLC', ' LLC'), ', LLP', ' LLP'),"
                   " ', Corp', ' Corp'), ', Co.', ' Co.'), ', Ltd', ' Ltd')")


def normalize_vendor(name):
    """Python-side vendor name normalization matching VENDOR_NORM_SQL."""
    if not name:
        return name
    for suffix in [', Inc', ', LLC', ', LLP', ', Corp', ', Co.', ', Ltd']:
        name = name.replace(suffix, suffix.replace(',', ''))
    return name


import re

# Known councilor name corrections
_NAME_FIXES = {
    'Volinky': 'Volinsky',
    'Steven': 'Stevens',
}

def normalize_dissent_names(raw):
    """Parse raw dissenting_votes string into a clean, deduplicated list of last names."""
    if not raw:
        return []
    names = []
    for part in raw.split(','):
        part = part.strip()
        if not part:
            continue
        # Remove prefixes
        part = re.sub(r'^Councilors?\s+', '', part)
        # Remove suffixes like "abstaining", "recused", "recusing himself", "originally", "all"
        part = re.sub(r'\s+(abstain(ing|ed)?|recused?\s*(himself)?|recusing\s+himself|originally|all|seconded by .*)$', '', part, flags=re.IGNORECASE)
        part = part.strip()
        if not part:
            continue
        # Apply known fixes
        part = _NAME_FIXES.get(part, part)
        if part and part not in names:
            names.append(part)
    return names


# Department name normalization (fixes typos, &/AND variants, prefix inconsistencies)
_DEPT_FIXES = {
    'DEPARTMENT OF AGRICULTURAL, MARKETS & FOOD': 'DEPARTMENT OF AGRICULTURE, MARKETS & FOOD',
    'DEPARTMENT OF MILITARY AFFAIRS AND VETERAN SERVICES': 'DEPARTMENT OF MILITARY AFFAIRS AND VETERANS SERVICES',
    'NEW HAMPSHIRE DEPARTMENT OF BUSINESS AND ECONOMIC AFFAIRS': 'DEPARTMENT OF BUSINESS AND ECONOMIC AFFAIRS',
    'NEW HAMPSHIRE DEPARTMENT OF STATE': 'DEPARTMENT OF STATE',
    'FISH AND GAME DEPARTMENT': 'NEW HAMPSHIRE FISH AND GAME DEPARTMENT',
    'NEW HAMPSHIRE FISH AND GAME COMMISSION': 'NEW HAMPSHIRE FISH AND GAME DEPARTMENT',
}

def normalize_dept(name):
    """Normalize department name — fix typos and variants."""
    if not name:
        return name
    fixed = _DEPT_FIXES.get(name, name)
    # Normalize & to AND for consistency
    fixed = fixed.replace(' & ', ' AND ')
    return fixed


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ─── TEMPLATE FILTERS ───

@app.template_filter('clean_dissenters')
def clean_dissenters_filter(value):
    """Template filter to display clean dissenter names."""
    names = normalize_dissent_names(value)
    return ', '.join(names) if names else value or ''


@app.template_filter('norm_dept')
def norm_dept_filter(value):
    """Template filter to normalize department names."""
    return normalize_dept(value)


@app.template_filter('currency')
def currency_filter(value):
    if value is None:
        return ""
    try:
        v = float(value)
        if v >= 1_000_000_000:
            return f"${v/1_000_000_000:,.2f}B"
        if v >= 1_000_000:
            return f"${v/1_000_000:,.1f}M"
        return f"${v:,.0f}"
    except (ValueError, TypeError):
        return ""


@app.template_filter('short_date')
def short_date_filter(value):
    if not value:
        return ""
    try:
        dt = datetime.strptime(value, '%Y-%m-%d')
        return dt.strftime('%b %-d, %Y')
    except ValueError:
        return value


@app.template_filter('compact_date')
def compact_date_filter(value):
    if not value:
        return ""
    try:
        dt = datetime.strptime(value, '%Y-%m-%d')
        return dt.strftime('%m/%d/%y')
    except ValueError:
        return value


# ─── ROUTES ───

@app.route('/')
def index():
    db = get_db()
    stats = {}
    stats['meetings'] = db.execute("SELECT COUNT(*) FROM meetings").fetchone()[0]
    stats['agenda_items'] = db.execute("SELECT COUNT(*) FROM agenda_items").fetchone()[0]
    stats['vote_records'] = db.execute("SELECT COUNT(*) FROM councilor_vote_records").fetchone()[0]
    stats['pdfs'] = db.execute("SELECT COUNT(*) FROM item_downloads").fetchone()[0]
    stats['total_spending'] = db.execute("SELECT SUM(amount) FROM agenda_items WHERE amount > 0").fetchone()[0]
    stats['vendors'] = db.execute("SELECT COUNT(DISTINCT vendor) FROM agenda_items WHERE vendor IS NOT NULL").fetchone()[0]
    stats['contested'] = db.execute("SELECT COUNT(*) FROM council_actions WHERE dissenting_votes IS NOT NULL AND dissenting_votes <> ''").fetchone()[0]

    # Latest meeting with full details
    latest = db.execute("""
        SELECT m.id, m.meeting_date, m.item_count,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id AND action_type = 'vote') as votes,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id
                AND dissenting_votes IS NOT NULL AND dissenting_votes <> '') as contested,
               (SELECT SUM(ai.amount) FROM agenda_items ai WHERE ai.meeting_id = m.id AND ai.amount > 0) as total_value,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id AND action_type = 'confirmation') as confirmations,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id AND action_type = 'nomination') as nominations
        FROM meetings m ORDER BY m.meeting_date DESC LIMIT 1
    """).fetchone()

    # Recent meetings
    recent = db.execute("""
        SELECT m.id, m.meeting_date, m.item_count,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id AND action_type = 'vote') as votes,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id
                AND dissenting_votes IS NOT NULL AND dissenting_votes <> '') as contested
        FROM meetings m ORDER BY m.meeting_date DESC LIMIT 8 OFFSET 1
    """).fetchall()

    # Recent contested votes
    recent_contested = db.execute("""
        SELECT ca.description, ca.dissenting_votes, ca.outcome, ca.item_number,
               m.meeting_date, m.id as meeting_id,
               (SELECT MIN(a2.id) FROM agenda_items a2
                WHERE a2.meeting_id = ca.meeting_id AND a2.item_number = ca.item_number) as item_id
        FROM council_actions ca
        JOIN meetings m ON m.id = ca.meeting_id
        WHERE ca.dissenting_votes IS NOT NULL AND ca.dissenting_votes <> ''
        ORDER BY m.meeting_date DESC, ca.item_number
        LIMIT 5
    """).fetchall()

    # Current councilors
    councilors = db.execute("""
        SELECT c.*,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'no') as no_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id) as total_votes
        FROM councilors c WHERE c.end_date IS NULL ORDER BY c.district
    """).fetchall()

    # Top 5 vendors by total spending
    vnorm = VENDOR_NORM_SQL.format(col='ai.vendor')
    top_vendors = db.execute(f"""
        SELECT {vnorm} as vendor, SUM(ai.amount) as total, COUNT(*) as items
        FROM agenda_items ai
        WHERE ai.vendor IS NOT NULL AND ai.amount > 0
        GROUP BY {vnorm}
        ORDER BY total DESC
        LIMIT 5
    """).fetchall()

    db.close()
    return render_template('index.html', stats=stats, latest=latest, recent=recent,
                           recent_contested=recent_contested, councilors=councilors,
                           top_vendors=top_vendors)


@app.route('/meetings')
def meetings():
    db = get_db()
    year = request.args.get('year', '')
    page = int(request.args.get('page', 1))
    per_page = 50

    query = """
        SELECT m.id, m.meeting_date, m.title, m.item_count,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id AND action_type = 'vote') as votes,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id
                AND dissenting_votes IS NOT NULL AND dissenting_votes <> '') as contested,
               (SELECT COUNT(*) FROM item_downloads WHERE meeting_id = m.id) as pdfs,
               (SELECT SUM(ai.amount) FROM agenda_items ai WHERE ai.meeting_id = m.id AND ai.amount > 0) as total_value
        FROM meetings m
    """
    params = []
    if year:
        query += " WHERE SUBSTR(m.meeting_date, 1, 4) = ?"
        params.append(year)
    query += " ORDER BY m.meeting_date DESC LIMIT ? OFFSET ?"
    params.extend([per_page, (page - 1) * per_page])

    meetings_list = db.execute(query, params).fetchall()

    total = db.execute(
        "SELECT COUNT(*) FROM meetings" + (" WHERE SUBSTR(meeting_date, 1, 4) = ?" if year else ""),
        [year] if year else []
    ).fetchone()[0]

    years = db.execute("SELECT DISTINCT SUBSTR(meeting_date, 1, 4) as y FROM meetings ORDER BY y DESC").fetchall()
    db.close()

    return render_template('meetings.html', meetings=meetings_list, years=years,
                           current_year=year, page=page, total=total, per_page=per_page)


@app.route('/meeting/<int:meeting_id>')
def meeting_detail(meeting_id):
    db = get_db()
    meeting = db.execute("SELECT * FROM meetings WHERE id = ?", (meeting_id,)).fetchone()
    if not meeting:
        abort(404)

    items = db.execute("""
        SELECT ai.*,
               ca.outcome, ca.dissenting_votes, ca.motion_by,
               (SELECT id2.filename FROM item_downloads id2
                WHERE id2.meeting_id = ai.meeting_id AND id2.item_number = ai.item_number
                  AND (id2.sub_item IS NULL OR id2.sub_item = '' OR id2.sub_item = ai.sub_item)
                LIMIT 1) as pdf_filename
        FROM agenda_items ai
        LEFT JOIN council_actions ca ON ca.id = (
            SELECT ca2.id FROM council_actions ca2
            WHERE ca2.meeting_id = ai.meeting_id AND ca2.item_number = ai.item_number
              AND ca2.action_type = 'vote'
              AND (ca2.sub_item IS NULL OR ca2.sub_item = '' OR ca2.sub_item = ai.sub_item)
            ORDER BY ca2.dissenting_votes IS NOT NULL DESC, ca2.id
            LIMIT 1
        )
        WHERE ai.meeting_id = ?
        ORDER BY CAST(CASE WHEN ai.item_number GLOB '[0-9]*' THEN ai.item_number ELSE '9999' END AS INTEGER),
                 ai.item_number, ai.sub_item
    """, (meeting_id,)).fetchall()

    # Non-vote actions
    other_actions = db.execute("""
        SELECT * FROM council_actions
        WHERE meeting_id = ? AND action_type != 'vote' AND action_type != 'minutes_approval'
        ORDER BY action_type, item_number
    """, (meeting_id,)).fetchall()

    # Meeting docs
    docs = db.execute("SELECT * FROM meeting_downloads WHERE meeting_id = ?", (meeting_id,)).fetchall()

    # Meeting summary stats
    summary = db.execute("""
        SELECT SUM(ai.amount) as total_value,
               COUNT(CASE WHEN ai.item_type = 'contract' THEN 1 END) as contracts,
               COUNT(CASE WHEN ai.item_type = 'grant' THEN 1 END) as grants,
               COUNT(CASE WHEN ai.is_consent_calendar = 1 THEN 1 END) as consent_items
        FROM agenda_items ai WHERE ai.meeting_id = ?
    """, (meeting_id,)).fetchone()

    # Prev/next meetings
    prev_meeting = db.execute(
        "SELECT id, meeting_date FROM meetings WHERE meeting_date < ? ORDER BY meeting_date DESC LIMIT 1",
        (meeting['meeting_date'],)).fetchone()
    next_meeting = db.execute(
        "SELECT id, meeting_date FROM meetings WHERE meeting_date > ? ORDER BY meeting_date ASC LIMIT 1",
        (meeting['meeting_date'],)).fetchone()

    db.close()
    return render_template('meeting_detail.html', meeting=meeting, items=items,
                           other_actions=other_actions, docs=docs, r2_base=R2_BASE,
                           summary=summary, prev_meeting=prev_meeting, next_meeting=next_meeting)


@app.route('/item/<int:item_id>')
def item_detail(item_id):
    db = get_db()
    item = db.execute("""
        SELECT ai.*, m.meeting_date, m.id as mid,
               ca.outcome, ca.dissenting_votes, ca.motion_by, ca.seconded_by, ca.abstaining,
               ca.id as action_id
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        LEFT JOIN council_actions ca ON ca.meeting_id = ai.meeting_id
            AND ca.item_number = ai.item_number AND ca.action_type = 'vote'
        WHERE ai.id = ?
    """, (item_id,)).fetchone()
    if not item:
        abort(404)

    # Sub-items (other items sharing same item_number but different sub_item)
    sub_items = db.execute("""
        SELECT ai.*, ca.outcome, ca.dissenting_votes
        FROM agenda_items ai
        LEFT JOIN council_actions ca ON ca.meeting_id = ai.meeting_id
            AND ca.item_number = ai.item_number AND ca.action_type = 'vote'
        WHERE ai.meeting_id = ? AND ai.item_number = ? AND ai.id != ?
        ORDER BY ai.sub_item
    """, (item['meeting_id'], item['item_number'], item_id)).fetchall()

    # PDFs for this item
    pdfs = db.execute("""
        SELECT * FROM item_downloads
        WHERE meeting_id = ? AND item_number = ?
        ORDER BY sub_item
    """, (item['meeting_id'], item['item_number'])).fetchall()

    # Individual councilor votes
    votes = []
    if item['action_id']:
        votes = db.execute("""
            SELECT cvr.councilor_name, cvr.vote, cvr.district, c.party
            FROM councilor_vote_records cvr
            LEFT JOIN councilors c ON c.id = cvr.councilor_id
            WHERE cvr.action_id = ?
            ORDER BY cvr.district
        """, (item['action_id'],)).fetchall()

    db.close()
    return render_template('item_detail.html', item=item, sub_items=sub_items,
                           pdfs=pdfs, votes=votes, r2_base=R2_BASE)


@app.route('/councilors')
def councilors():
    db = get_db()
    current = db.execute("""
        SELECT c.*,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'yes') as yes_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'no') as no_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'abstain') as abstain_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id) as total_votes
        FROM councilors c WHERE c.end_date IS NULL ORDER BY c.district
    """).fetchall()

    # Merge historical councilors by name (combine terms)
    hist_raw = db.execute("""
        SELECT c.name, c.party, MIN(c.district) as district,
               MIN(c.id) as id,
               MIN(c.start_date) as start_date, MAX(c.end_date) as end_date,
               SUM((SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'yes')) as yes_votes,
               SUM((SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'no')) as no_votes,
               SUM((SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id)) as total_votes
        FROM councilors c WHERE c.end_date IS NOT NULL
        GROUP BY c.name
        ORDER BY MAX(c.end_date) DESC
    """).fetchall()
    # Exclude anyone who is also a current councilor (they show in 'current' section)
    current_names = {c['name'] for c in current}
    historical = [h for h in hist_raw if h['name'] not in current_names]

    db.close()
    return render_template('councilors.html', current=current, historical=historical)


@app.route('/councilor/<int:councilor_id>')
def councilor_detail(councilor_id):
    db = get_db()
    councilor = db.execute("SELECT * FROM councilors WHERE id = ?", (councilor_id,)).fetchone()
    if not councilor:
        abort(404)

    terms = db.execute("SELECT * FROM councilors WHERE name = ? ORDER BY start_date",
                       (councilor['name'],)).fetchall()
    term_ids = [t['id'] for t in terms]
    ph = ','.join('?' * len(term_ids))

    vote_summary = db.execute(f"""
        SELECT vote, COUNT(*) as cnt
        FROM councilor_vote_records WHERE councilor_id IN ({ph})
        GROUP BY vote
    """, term_ids).fetchall()

    dissents = db.execute(f"""
        SELECT cvr.meeting_date, cvr.meeting_id, cvr.item_number,
               ca.description, ca.outcome,
               ai.vendor, ai.amount, ai.id as item_id
        FROM councilor_vote_records cvr
        JOIN council_actions ca ON ca.id = cvr.action_id
        LEFT JOIN agenda_items ai ON ai.id = (
            SELECT MIN(a2.id) FROM agenda_items a2
            WHERE a2.meeting_id = cvr.meeting_id AND a2.item_number = cvr.item_number
        )
        WHERE cvr.councilor_id IN ({ph}) AND cvr.vote = 'no'
        ORDER BY cvr.meeting_date DESC
        LIMIT 200
    """, term_ids).fetchall()

    db.close()
    return render_template('councilor_detail.html', councilor=councilor, terms=terms,
                           vote_summary=vote_summary, dissents=dissents)


@app.route('/vendors')
def vendors():
    db = get_db()
    page = int(request.args.get('page', 1))
    search = request.args.get('q', '')
    per_page = 50

    where = "WHERE ai.vendor IS NOT NULL"
    params = []
    if search:
        where += " AND ai.vendor LIKE ?"
        params.append(f'%{search}%')
    else:
        where += " AND ai.amount > 0"

    vnorm = VENDOR_NORM_SQL.format(col='ai.vendor')
    vendors_list = db.execute(f"""
        SELECT {vnorm} as vendor, COUNT(*) as item_count, SUM(ai.amount) as total_amount,
               MIN(m.meeting_date) as first_seen, MAX(m.meeting_date) as last_seen
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        {where}
        GROUP BY {vnorm}
        ORDER BY total_amount DESC
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    total = db.execute(f"""
        SELECT COUNT(DISTINCT {vnorm}) FROM agenda_items ai {where}
    """, params).fetchone()[0]

    db.close()
    return render_template('vendors.html', vendors=vendors_list, search=search,
                           page=page, total=total, per_page=per_page)


@app.route('/vendor/<path:vendor_name>')
def vendor_detail(vendor_name):
    db = get_db()
    page = int(request.args.get('page', 1))
    per_page = 50
    # Normalize the incoming name so "Pike Industries, Inc." and "Pike Industries Inc." both work
    normalized = normalize_vendor(vendor_name)
    vnorm = VENDOR_NORM_SQL.format(col='ai.vendor')
    items = db.execute(f"""
        SELECT ai.*, m.meeting_date,
               ca.outcome, ca.dissenting_votes
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        LEFT JOIN council_actions ca ON ca.meeting_id = ai.meeting_id
            AND ca.item_number = ai.item_number AND ca.action_type = 'vote'
        WHERE {vnorm} = ?
        ORDER BY m.meeting_date DESC
        LIMIT ? OFFSET ?
    """, (normalized, per_page, (page - 1) * per_page)).fetchall()

    if not items and page == 1:
        abort(404)

    total = db.execute(f"SELECT COUNT(*) FROM agenda_items ai WHERE {vnorm} = ?",
                       (normalized,)).fetchone()[0]
    total_value = db.execute(f"SELECT SUM(ai.amount) FROM agenda_items ai WHERE {vnorm} = ? AND ai.amount > 0",
                             (normalized,)).fetchone()[0] or 0
    db.close()
    return render_template('vendor_detail.html', vendor_name=normalized,
                           items=items, total_value=total_value,
                           total=total, page=page, per_page=per_page)


@app.route('/departments')
def departments():
    db = get_db()
    raw_depts = db.execute("""
        SELECT ai.department, COUNT(*) as item_count, SUM(ai.amount) as total_amount,
               COUNT(DISTINCT ai.meeting_id) as meetings,
               MIN(m.meeting_date) as first_seen, MAX(m.meeting_date) as last_seen
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE ai.department IS NOT NULL AND ai.department != ''
        GROUP BY ai.department
    """).fetchall()
    # Merge variants under normalized names
    merged = {}
    for d in raw_depts:
        norm = normalize_dept(d['department'])
        if norm in merged:
            m = merged[norm]
            m['item_count'] += d['item_count']
            m['total_amount'] = (m['total_amount'] or 0) + (d['total_amount'] or 0)
            m['meetings'] += d['meetings']
            if d['first_seen'] and (not m['first_seen'] or d['first_seen'] < m['first_seen']):
                m['first_seen'] = d['first_seen']
            if d['last_seen'] and (not m['last_seen'] or d['last_seen'] > m['last_seen']):
                m['last_seen'] = d['last_seen']
        else:
            merged[norm] = {
                'department': norm,
                'item_count': d['item_count'],
                'total_amount': d['total_amount'] or 0,
                'meetings': d['meetings'],
                'first_seen': d['first_seen'],
                'last_seen': d['last_seen'],
            }
    depts = sorted(merged.values(), key=lambda x: x['total_amount'] or 0, reverse=True)
    db.close()
    return render_template('departments.html', departments=depts)


@app.route('/department/<path:dept_name>')
def department_detail(dept_name):
    db = get_db()
    page = int(request.args.get('page', 1))
    per_page = 50

    # Find all raw department names that normalize to this name
    norm_name = normalize_dept(dept_name)
    all_raw = db.execute("SELECT DISTINCT department FROM agenda_items WHERE department IS NOT NULL").fetchall()
    matching_raw = [r['department'] for r in all_raw if normalize_dept(r['department']) == norm_name]
    if not matching_raw:
        abort(404)
    ph = ','.join('?' * len(matching_raw))

    # Top vendors for this department (normalized names)
    vnorm = VENDOR_NORM_SQL.format(col='vendor')
    top_vendors = db.execute(f"""
        SELECT {vnorm} as vendor, COUNT(*) as item_count, SUM(amount) as total_amount
        FROM agenda_items
        WHERE department IN ({ph}) AND vendor IS NOT NULL AND amount > 0
        GROUP BY {vnorm} ORDER BY total_amount DESC LIMIT 15
    """, matching_raw).fetchall()

    # Item type breakdown
    type_breakdown = db.execute(f"""
        SELECT item_type, COUNT(*) as cnt, SUM(amount) as total
        FROM agenda_items WHERE department IN ({ph})
        GROUP BY item_type ORDER BY cnt DESC
    """, matching_raw).fetchall()

    # Recent items
    items = db.execute(f"""
        SELECT ai.*, m.meeting_date,
               ca.outcome, ca.dissenting_votes
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        LEFT JOIN council_actions ca ON ca.id = (
            SELECT ca2.id FROM council_actions ca2
            WHERE ca2.meeting_id = ai.meeting_id AND ca2.item_number = ai.item_number
              AND ca2.action_type = 'vote'
            ORDER BY ca2.dissenting_votes IS NOT NULL DESC, ca2.id
            LIMIT 1
        )
        WHERE ai.department IN ({ph})
        ORDER BY m.meeting_date DESC
        LIMIT ? OFFSET ?
    """, matching_raw + [per_page, (page - 1) * per_page]).fetchall()

    total = db.execute(f"SELECT COUNT(*) FROM agenda_items WHERE department IN ({ph})",
                       matching_raw).fetchone()[0]
    total_value = db.execute(f"SELECT SUM(amount) FROM agenda_items WHERE department IN ({ph}) AND amount > 0",
                             matching_raw).fetchone()[0]

    db.close()
    return render_template('department_detail.html', dept_name=norm_name, items=items,
                           top_vendors=top_vendors, type_breakdown=type_breakdown,
                           total=total, total_value=total_value, page=page, per_page=per_page)


@app.route('/items')
def items_browse():
    db = get_db()
    page = int(request.args.get('page', 1))
    item_type = request.args.get('type', '')
    year = request.args.get('year', '')
    dept = request.args.get('dept', '')
    per_page = 50

    where_parts = ['1=1']
    params = []
    if item_type:
        where_parts.append("ai.item_type = ?")
        params.append(item_type)
    if year:
        where_parts.append("SUBSTR(m.meeting_date, 1, 4) = ?")
        params.append(year)
    if dept:
        # Match all raw variants of this normalized department
        norm = normalize_dept(dept)
        all_raw = db.execute("SELECT DISTINCT department FROM agenda_items WHERE department IS NOT NULL").fetchall()
        matching = [r['department'] for r in all_raw if normalize_dept(r['department']) == norm]
        if matching:
            ph = ','.join('?' * len(matching))
            where_parts.append(f"ai.department IN ({ph})")
            params.extend(matching)

    where = ' AND '.join(where_parts)

    items = db.execute(f"""
        SELECT ai.*, m.meeting_date,
               ca.outcome, ca.dissenting_votes
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        LEFT JOIN council_actions ca ON ca.id = (
            SELECT ca2.id FROM council_actions ca2
            WHERE ca2.meeting_id = ai.meeting_id AND ca2.item_number = ai.item_number
              AND ca2.action_type = 'vote'
            ORDER BY ca2.dissenting_votes IS NOT NULL DESC, ca2.id
            LIMIT 1
        )
        WHERE {where}
        ORDER BY m.meeting_date DESC, ai.item_number
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    total = db.execute(f"""
        SELECT COUNT(*) FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE {where}
    """, params).fetchone()[0]

    # Get filter options
    years = db.execute("SELECT DISTINCT SUBSTR(meeting_date, 1, 4) as y FROM meetings ORDER BY y DESC").fetchall()
    types = db.execute("SELECT DISTINCT item_type FROM agenda_items WHERE item_type IS NOT NULL ORDER BY item_type").fetchall()

    db.close()
    return render_template('items.html', items=items, total=total,
                           page=page, per_page=per_page,
                           item_type=item_type, year=year, dept=dept,
                           years=years, types=types)


@app.route('/search')
def search():
    db = get_db()
    q = request.args.get('q', '')
    item_type = request.args.get('type', '')
    page = int(request.args.get('page', 1))
    per_page = 50

    if not q or len(q) < 3:
        db.close()
        return render_template('search.html', results=[], query=q, item_type=item_type,
                               total=0, page=1, per_page=per_page,
                               personnel=[], personnel_total=0)

    where = "(ai.description LIKE ? OR ai.vendor LIKE ? OR ai.department LIKE ?)"
    params = [f'%{q}%', f'%{q}%', f'%{q}%']
    if item_type:
        where += " AND ai.item_type = ?"
        params.append(item_type)

    results = db.execute(f"""
        SELECT ai.*, m.meeting_date
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE {where}
        ORDER BY m.meeting_date DESC
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    total = db.execute(f"""
        SELECT COUNT(*) FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE {where}
    """, params).fetchone()[0]

    # Also search personnel actions (person_name, position_title, description)
    personnel = db.execute("""
        SELECT ca.*, m.meeting_date as mdate
        FROM council_actions ca
        JOIN meetings m ON m.id = ca.meeting_id
        WHERE ca.action_type IN ('confirmation', 'nomination', 'resignation')
          AND (ca.person_name LIKE ? OR ca.position_title LIKE ? OR ca.description LIKE ?)
        ORDER BY m.meeting_date DESC
        LIMIT 20
    """, [f'%{q}%', f'%{q}%', f'%{q}%']).fetchall()

    personnel_total = db.execute("""
        SELECT COUNT(*) FROM council_actions ca
        WHERE ca.action_type IN ('confirmation', 'nomination', 'resignation')
          AND (ca.person_name LIKE ? OR ca.position_title LIKE ? OR ca.description LIKE ?)
    """, [f'%{q}%', f'%{q}%', f'%{q}%']).fetchone()[0]

    db.close()
    return render_template('search.html', results=results, query=q, item_type=item_type,
                           total=total, page=page, per_page=per_page,
                           personnel=personnel, personnel_total=personnel_total)


@app.route('/contested')
def contested():
    db = get_db()
    page = int(request.args.get('page', 1))
    councilor = request.args.get('councilor', '')
    per_page = 50

    where = "ca.dissenting_votes IS NOT NULL AND ca.dissenting_votes <> ''"
    params = []
    # Reverse-lookup: if the normalized name has a known fix, also search the original
    _REVERSE_FIXES = {v: k for k, v in _NAME_FIXES.items()}
    if councilor:
        original = _REVERSE_FIXES.get(councilor)
        if original:
            where += " AND (ca.dissenting_votes LIKE ? OR ca.dissenting_votes LIKE ?)"
            params.extend([f'%{councilor}%', f'%{original}%'])
        else:
            where += " AND ca.dissenting_votes LIKE ?"
            params.append(f'%{councilor}%')

    # Join to FIRST agenda_item only (MIN(ai.id)) to avoid sub-item duplication
    actions = db.execute(f"""
        SELECT ca.*, m.meeting_date as mdate,
               ai.vendor, ai.amount, ai.item_type, ai.id as item_id
        FROM council_actions ca
        JOIN meetings m ON m.id = ca.meeting_id
        LEFT JOIN agenda_items ai ON ai.id = (
            SELECT MIN(a2.id) FROM agenda_items a2
            WHERE a2.meeting_id = ca.meeting_id AND a2.item_number = ca.item_number
        )
        WHERE {where}
        ORDER BY m.meeting_date DESC
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    total = db.execute(f"SELECT COUNT(*) FROM council_actions ca WHERE {where}", params).fetchone()[0]

    # Get all councilor names who have dissented — normalized and deduplicated
    dissenters = db.execute("""
        SELECT DISTINCT dissenting_votes FROM council_actions
        WHERE dissenting_votes IS NOT NULL AND dissenting_votes <> ''
    """).fetchall()
    all_names = set()
    for row in dissenters:
        for name in normalize_dissent_names(row['dissenting_votes']):
            all_names.add(name)
    all_names = sorted(all_names)

    db.close()
    return render_template('contested.html', actions=actions, total=total,
                           page=page, per_page=per_page, councilor=councilor,
                           all_names=all_names)


@app.route('/nominations')
def nominations():
    db = get_db()
    page = int(request.args.get('page', 1))
    action_type = request.args.get('type', '')
    per_page = 50

    where = "ca.action_type IN ('confirmation', 'nomination', 'resignation')"
    params = []
    if action_type:
        where = "ca.action_type = ?"
        params.append(action_type)

    actions = db.execute(f"""
        SELECT ca.*, m.meeting_date as mdate,
               (SELECT MIN(a2.id) FROM agenda_items a2
                WHERE a2.meeting_id = ca.meeting_id AND a2.item_number = ca.item_number) as item_id
        FROM council_actions ca
        JOIN meetings m ON m.id = ca.meeting_id
        WHERE {where}
        ORDER BY m.meeting_date DESC
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    total = db.execute(f"SELECT COUNT(*) FROM council_actions ca WHERE {where}", params).fetchone()[0]

    counts = db.execute("""
        SELECT action_type, COUNT(*) as cnt FROM council_actions
        WHERE action_type IN ('confirmation', 'nomination', 'resignation')
        GROUP BY action_type
    """).fetchall()

    db.close()
    return render_template('nominations.html', actions=actions, total=total,
                           page=page, per_page=per_page, action_type=action_type,
                           counts={r['action_type']: r['cnt'] for r in counts})


# ─── API + EXPORT ───

@app.route('/api/stats')
def api_stats():
    db = get_db()
    stats = {
        'meetings': db.execute("SELECT COUNT(*) FROM meetings").fetchone()[0],
        'agenda_items': db.execute("SELECT COUNT(*) FROM agenda_items").fetchone()[0],
        'vote_records': db.execute("SELECT COUNT(*) FROM councilor_vote_records").fetchone()[0],
        'total_spending': db.execute("SELECT SUM(amount) FROM agenda_items WHERE amount > 0").fetchone()[0],
    }
    db.close()
    return jsonify(stats)


@app.route('/export/meeting/<int:meeting_id>')
def export_meeting_csv(meeting_id):
    db = get_db()
    meeting = db.execute("SELECT meeting_date FROM meetings WHERE id = ?", (meeting_id,)).fetchone()
    if not meeting:
        abort(404)

    items = db.execute("""
        SELECT ai.item_number, ai.department, ai.description, ai.vendor,
               ai.amount, ai.item_type, ca.outcome, ca.dissenting_votes
        FROM agenda_items ai
        LEFT JOIN council_actions ca ON ca.meeting_id = ai.meeting_id
            AND ca.item_number = ai.item_number AND ca.action_type = 'vote'
        WHERE ai.meeting_id = ?
        ORDER BY CAST(CASE WHEN ai.item_number GLOB '[0-9]*' THEN ai.item_number ELSE '9999' END AS INTEGER)
    """, (meeting_id,)).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Item', 'Department', 'Description', 'Vendor', 'Amount', 'Type', 'Outcome', 'Dissenting'])
    for item in items:
        writer.writerow([item['item_number'], item['department'], item['description'],
                         item['vendor'], item['amount'], item['item_type'],
                         item['outcome'], item['dissenting_votes']])

    db.close()
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=gc_{meeting["meeting_date"]}.csv'}
    )


_footer_cache = {}
_footer_cache_time = 0

@app.context_processor
def footer_stats():
    """Make basic counts available to all templates for the footer. Cached for 1 hour."""
    import time
    global _footer_cache, _footer_cache_time
    now = time.time()
    if _footer_cache and (now - _footer_cache_time) < 3600:
        return _footer_cache
    db = get_db()
    stats = {
        'footer_meetings': db.execute("SELECT COUNT(*) FROM meetings").fetchone()[0],
        'footer_docs': db.execute("SELECT COUNT(*) FROM item_downloads").fetchone()[0],
        'footer_votes': db.execute("SELECT COUNT(*) FROM councilor_vote_records").fetchone()[0],
    }
    years = db.execute("SELECT MIN(SUBSTR(meeting_date,1,4)), MAX(SUBSTR(meeting_date,1,4)) FROM meetings").fetchone()
    stats['footer_year_start'] = years[0]
    stats['footer_year_end'] = years[1]
    db.close()
    _footer_cache = stats
    _footer_cache_time = now
    return stats


@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404


if __name__ == '__main__':
    app.run(debug=True, port=5050)
