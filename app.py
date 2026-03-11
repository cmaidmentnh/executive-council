#!/usr/bin/env python3
"""
NH Executive Council Database — Public Web App
Flask app serving 14+ years of G&C meeting data.
"""

import sqlite3
import os
import secrets
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, render_template, request, jsonify, abort, Response, session, redirect, url_for, flash
from werkzeug.security import generate_password_hash, check_password_hash
import csv
import io

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))
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
        SELECT m.id, m.meeting_date,
               (SELECT COUNT(*) FROM agenda_items WHERE meeting_id = m.id) as item_count,
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
        SELECT m.id, m.meeting_date,
               (SELECT COUNT(*) FROM agenda_items WHERE meeting_id = m.id) as item_count,
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

    # Yearly spending for chart
    yearly_spending = db.execute("""
        SELECT SUBSTR(m.meeting_date, 1, 4) as year,
               SUM(ai.amount) as spending,
               COUNT(*) as items
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE ai.amount > 0
        GROUP BY SUBSTR(m.meeting_date, 1, 4)
        ORDER BY year
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
                           top_vendors=top_vendors, yearly_spending=yearly_spending)


@app.route('/meetings')
def meetings():
    db = get_db()
    year = request.args.get('year', '')
    page = int(request.args.get('page', 1))
    per_page = 50

    query = """
        SELECT m.id, m.meeting_date, m.title,
               (SELECT COUNT(*) FROM agenda_items WHERE meeting_id = m.id) as item_count,
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

    # Orphan PDFs — downloads with no matching agenda_item
    orphan_pdfs = db.execute("""
        SELECT id2.item_number, id2.sub_item, id2.filename
        FROM item_downloads id2
        WHERE id2.meeting_id = ?
          AND NOT EXISTS (
              SELECT 1 FROM agenda_items ai
              WHERE ai.meeting_id = id2.meeting_id AND ai.item_number = id2.item_number
          )
        ORDER BY CAST(CASE WHEN id2.item_number GLOB '[0-9]*' THEN id2.item_number ELSE '9999' END AS INTEGER),
                 id2.item_number, id2.sub_item
    """, (meeting_id,)).fetchall()

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
                           summary=summary, prev_meeting=prev_meeting, next_meeting=next_meeting,
                           orphan_pdfs=orphan_pdfs)


@app.route('/item/<int:item_id>')
def item_detail(item_id):
    db = get_db()
    item = db.execute("""
        SELECT ai.*, m.meeting_date, m.id as mid,
               ca.outcome, ca.dissenting_votes, ca.motion_by, ca.seconded_by, ca.abstaining,
               ca.id as action_id
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        LEFT JOIN council_actions ca ON ca.id = (
            SELECT ca2.id FROM council_actions ca2
            WHERE ca2.meeting_id = ai.meeting_id AND ca2.item_number = ai.item_number
              AND ca2.action_type = 'vote'
              AND (ca2.sub_item IS NULL OR ca2.sub_item = '' OR ca2.sub_item = ai.sub_item)
            ORDER BY ca2.dissenting_votes IS NOT NULL DESC, ca2.id
            LIMIT 1
        )
        WHERE ai.id = ?
    """, (item_id,)).fetchone()
    if not item:
        abort(404)

    # Sub-items (other items sharing same item_number but different sub_item)
    sub_items = db.execute("""
        SELECT ai.*, ca.outcome, ca.dissenting_votes
        FROM agenda_items ai
        LEFT JOIN council_actions ca ON ca.id = (
            SELECT ca2.id FROM council_actions ca2
            WHERE ca2.meeting_id = ai.meeting_id AND ca2.item_number = ai.item_number
              AND ca2.action_type = 'vote'
              AND (ca2.sub_item IS NULL OR ca2.sub_item = '' OR ca2.sub_item = ai.sub_item)
            ORDER BY ca2.dissenting_votes IS NOT NULL DESC, ca2.id
            LIMIT 1
        )
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

    page = int(request.args.get('page', 1))
    per_page = 50

    terms = db.execute("SELECT * FROM councilors WHERE name = ? ORDER BY start_date",
                       (councilor['name'],)).fetchall()
    term_ids = [t['id'] for t in terms]
    ph = ','.join('?' * len(term_ids))

    vote_summary = db.execute(f"""
        SELECT vote, COUNT(*) as cnt
        FROM councilor_vote_records WHERE councilor_id IN ({ph})
        GROUP BY vote
    """, term_ids).fetchall()

    dissent_total = db.execute(f"""
        SELECT COUNT(*) FROM councilor_vote_records
        WHERE councilor_id IN ({ph}) AND vote = 'no'
    """, term_ids).fetchone()[0]

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
        LIMIT ? OFFSET ?
    """, term_ids + [per_page, (page - 1) * per_page]).fetchall()

    db.close()
    return render_template('councilor_detail.html', councilor=councilor, terms=terms,
                           vote_summary=vote_summary, dissents=dissents,
                           dissent_total=dissent_total, page=page, per_page=per_page)


@app.route('/vendors')
def vendors():
    db = get_db()
    page = int(request.args.get('page', 1))
    search = request.args.get('q', '')
    sort = request.args.get('sort', 'amount')
    per_page = 50

    where = "WHERE ai.vendor IS NOT NULL"
    params = []
    if search:
        where += " AND ai.vendor LIKE ?"
        params.append(f'%{search}%')
    else:
        where += " AND ai.amount > 0"

    sort_map = {
        'amount': 'total_amount DESC',
        'items': 'item_count DESC',
        'name': 'vendor ASC',
        'recent': 'last_seen DESC',
    }
    order = sort_map.get(sort, 'total_amount DESC')

    vnorm = VENDOR_NORM_SQL.format(col='ai.vendor')
    vendors_list = db.execute(f"""
        SELECT {vnorm} as vendor, COUNT(*) as item_count, SUM(ai.amount) as total_amount,
               MIN(m.meeting_date) as first_seen, MAX(m.meeting_date) as last_seen
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        {where}
        GROUP BY {vnorm}
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    total = db.execute(f"""
        SELECT COUNT(DISTINCT {vnorm}) FROM agenda_items ai {where}
    """, params).fetchone()[0]

    db.close()
    return render_template('vendors.html', vendors=vendors_list, search=search,
                           page=page, total=total, per_page=per_page, sort=sort)


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
        LEFT JOIN council_actions ca ON ca.id = (
            SELECT ca2.id FROM council_actions ca2
            WHERE ca2.meeting_id = ai.meeting_id AND ca2.item_number = ai.item_number
              AND ca2.action_type = 'vote'
            ORDER BY ca2.dissenting_votes IS NOT NULL DESC, ca2.id
            LIMIT 1
        )
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

    # Summary data
    date_range = db.execute(f"""
        SELECT MIN(m.meeting_date) as first_seen, MAX(m.meeting_date) as last_seen
        FROM agenda_items ai JOIN meetings m ON m.id = ai.meeting_id
        WHERE {vnorm} = ?
    """, (normalized,)).fetchone()

    top_depts = db.execute(f"""
        SELECT ai.department, COUNT(*) as cnt, SUM(ai.amount) as total
        FROM agenda_items ai WHERE {vnorm} = ? AND ai.department IS NOT NULL
        GROUP BY ai.department ORDER BY total DESC LIMIT 5
    """, (normalized,)).fetchall()

    type_breakdown = db.execute(f"""
        SELECT ai.item_type, COUNT(*) as cnt, SUM(ai.amount) as total
        FROM agenda_items ai WHERE {vnorm} = ?
        GROUP BY ai.item_type ORDER BY cnt DESC
    """, (normalized,)).fetchall()

    # Vendor location from most recent item
    location = db.execute(f"""
        SELECT ai.vendor_city, ai.vendor_state FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE {vnorm} = ? AND (ai.vendor_city IS NOT NULL OR ai.vendor_state IS NOT NULL)
        ORDER BY m.meeting_date DESC LIMIT 1
    """, (normalized,)).fetchone()

    db.close()
    return render_template('vendor_detail.html', vendor_name=normalized,
                           items=items, total_value=total_value,
                           total=total, page=page, per_page=per_page,
                           date_range=date_range, top_depts=top_depts,
                           type_breakdown=type_breakdown, location=location)


@app.route('/departments')
def departments():
    db = get_db()
    search_q = request.args.get('q', '')
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
    if search_q:
        sq = search_q.upper()
        depts = [d for d in depts if sq in d['department'].upper()]
    db.close()
    return render_template('departments.html', departments=depts,
                           search_q=search_q, total=len(depts))


@app.route('/department/<path:dept_name>')
def department_detail(dept_name):
    db = get_db()
    page = int(request.args.get('page', 1))
    year = request.args.get('year', '')
    per_page = 50

    # Find all raw department names that normalize to this name
    norm_name = normalize_dept(dept_name)
    all_raw = db.execute("SELECT DISTINCT department FROM agenda_items WHERE department IS NOT NULL").fetchall()
    matching_raw = [r['department'] for r in all_raw if normalize_dept(r['department']) == norm_name]
    if not matching_raw:
        abort(404)
    ph = ','.join('?' * len(matching_raw))

    year_filter = ""
    year_params = []
    if year:
        year_filter = " AND SUBSTR(m.meeting_date, 1, 4) = ?"
        year_params = [year]

    # Top vendors for this department (normalized names)
    vnorm = VENDOR_NORM_SQL.format(col='ai.vendor')
    top_vendors = db.execute(f"""
        SELECT {vnorm} as vendor, COUNT(*) as item_count, SUM(ai.amount) as total_amount
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE ai.department IN ({ph}) AND ai.vendor IS NOT NULL AND ai.amount > 0{year_filter}
        GROUP BY {vnorm} ORDER BY total_amount DESC LIMIT 15
    """, matching_raw + year_params).fetchall()

    # Item type breakdown
    type_breakdown = db.execute(f"""
        SELECT ai.item_type, COUNT(*) as cnt, SUM(ai.amount) as total
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE ai.department IN ({ph}){year_filter}
        GROUP BY ai.item_type ORDER BY cnt DESC
    """, matching_raw + year_params).fetchall()

    # Items
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
        WHERE ai.department IN ({ph}){year_filter}
        ORDER BY m.meeting_date DESC
        LIMIT ? OFFSET ?
    """, matching_raw + year_params + [per_page, (page - 1) * per_page]).fetchall()

    total = db.execute(f"""
        SELECT COUNT(*) FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE ai.department IN ({ph}){year_filter}
    """, matching_raw + year_params).fetchone()[0]
    total_value = db.execute(f"""
        SELECT SUM(ai.amount) FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE ai.department IN ({ph}) AND ai.amount > 0{year_filter}
    """, matching_raw + year_params).fetchone()[0]

    # Available years for this department
    years = db.execute(f"""
        SELECT DISTINCT SUBSTR(m.meeting_date, 1, 4) as y
        FROM agenda_items ai JOIN meetings m ON m.id = ai.meeting_id
        WHERE ai.department IN ({ph})
        ORDER BY y DESC
    """, matching_raw).fetchall()

    # Yearly spending for chart
    yearly_spending = db.execute(f"""
        SELECT SUBSTR(m.meeting_date, 1, 4) as year, SUM(ai.amount) as spending
        FROM agenda_items ai JOIN meetings m ON m.id = ai.meeting_id
        WHERE ai.department IN ({ph}) AND ai.amount > 0
        GROUP BY SUBSTR(m.meeting_date, 1, 4)
        ORDER BY year
    """, matching_raw).fetchall()

    db.close()
    return render_template('department_detail.html', dept_name=norm_name, items=items,
                           top_vendors=top_vendors, type_breakdown=type_breakdown,
                           total=total, total_value=total_value or 0,
                           page=page, per_page=per_page,
                           year=year, years=years,
                           yearly_spending=yearly_spending)


@app.route('/items')
def items_browse():
    db = get_db()
    page = int(request.args.get('page', 1))
    item_type = request.args.get('type', '')
    year = request.args.get('year', '')
    dept = request.args.get('dept', '')
    search_q = request.args.get('q', '')
    per_page = 50

    where_parts = ['1=1']
    params = []
    if search_q:
        where_parts.append("(ai.description LIKE ? OR ai.vendor LIKE ?)")
        params.extend([f'%{search_q}%', f'%{search_q}%'])
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
                           search_q=search_q, years=years, types=types)


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
        SELECT ca.*, m.meeting_date as mdate,
               (SELECT MIN(a2.id) FROM agenda_items a2
                WHERE a2.meeting_id = ca.meeting_id AND a2.item_number = ca.item_number) as item_id
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
    outcome_filter = request.args.get('outcome', '')
    per_page = 50

    where = "ca.dissenting_votes IS NOT NULL AND ca.dissenting_votes <> ''"
    params = []
    if outcome_filter:
        where += " AND ca.outcome = ?"
        params.append(outcome_filter)
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

    # Outcome counts for filter buttons
    outcome_counts = db.execute("""
        SELECT outcome, COUNT(*) as cnt FROM council_actions
        WHERE dissenting_votes IS NOT NULL AND dissenting_votes <> ''
        GROUP BY outcome
    """).fetchall()
    oc = {r['outcome']: r['cnt'] for r in outcome_counts}

    db.close()
    return render_template('contested.html', actions=actions, total=total,
                           page=page, per_page=per_page, councilor=councilor,
                           outcome_filter=outcome_filter,
                           all_names=all_names,
                           denied_count=oc.get('denied', 0),
                           tabled_count=oc.get('tabled', 0),
                           approved_count=oc.get('approved', 0))


@app.route('/nominations')
def nominations():
    db = get_db()
    page = int(request.args.get('page', 1))
    action_type = request.args.get('type', '')
    search_q = request.args.get('q', '')
    per_page = 50

    where = "ca.action_type IN ('confirmation', 'nomination', 'resignation')"
    params = []
    if action_type:
        where = "ca.action_type = ?"
        params.append(action_type)
    if search_q:
        where += " AND (ca.person_name LIKE ? OR ca.position_title LIKE ?)"
        params.extend([f'%{search_q}%', f'%{search_q}%'])

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
                           search_q=search_q,
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
        SELECT ai.item_number, ai.sub_item, ai.department, ai.description, ai.vendor,
               ai.amount, ai.item_type, ca.outcome, ca.dissenting_votes
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


# ─── AUTH TABLES ───

def init_auth_db():
    """Create user-related tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            notify_new_meetings INTEGER DEFAULT 1,
            email_verified INTEGER DEFAULT 0,
            verify_token TEXT,
            reset_token TEXT,
            reset_token_expires TIMESTAMP,
            unsubscribe_token TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS notification_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            meeting_id INTEGER REFERENCES meetings(id),
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT,
            error TEXT
        )
    """)
    # Add new columns to existing tables (safe if they already exist)
    for col, default in [
        ('email_verified', '0'), ('verify_token', 'NULL'),
        ('reset_token', 'NULL'), ('reset_token_expires', 'NULL'),
        ('unsubscribe_token', 'NULL'),
    ]:
        try:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} DEFAULT {default}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()

init_auth_db()


def get_current_user():
    uid = session.get('user_id')
    if not uid:
        return None
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id = ? AND is_active = 1", (uid,)).fetchone()
    db.close()
    return user


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get('user_id'):
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return wrapper


@app.context_processor
def inject_user():
    return {'current_user': get_current_user()}


# ─── AUTH ROUTES ───

def _send_email(to, subject, html, text):
    """Send an email via SES. Returns True on success."""
    try:
        import boto3
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        ses = boto3.client('ses', region_name='us-east-1')
        sender = "Granite State G&C Tracker <alerts@executivecouncilnh.com>"
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = to
        msg.attach(MIMEText(text, 'plain'))
        msg.attach(MIMEText(html, 'html'))
        ses.send_raw_email(Source=sender, Destinations=[to], RawMessage={'Data': msg.as_string()})
        return True
    except Exception:
        return False

SITE_URL = "https://executivecouncilnh.com"


@app.route('/register', methods=['GET', 'POST'])
def register():
    if session.get('user_id'):
        return redirect('/')
    error = None
    success = False
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        confirm = request.form.get('confirm', '')
        if not email or '@' not in email:
            error = 'Valid email required.'
        elif len(password) < 8:
            error = 'Password must be at least 8 characters.'
        elif password != confirm:
            error = 'Passwords do not match.'
        else:
            db = get_db()
            existing = db.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
            if existing:
                error = 'An account with that email already exists.'
            else:
                verify_token = secrets.token_urlsafe(32)
                unsub_token = secrets.token_urlsafe(32)
                db.execute(
                    "INSERT INTO users (email, password_hash, verify_token, unsubscribe_token, email_verified, notify_new_meetings) VALUES (?, ?, ?, ?, 0, 0)",
                    (email, generate_password_hash(password), verify_token, unsub_token)
                )
                db.commit()
                user = db.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
                session['user_id'] = user['id']
                # Send verification email
                verify_url = f"{SITE_URL}/verify/{verify_token}"
                _send_email(email, "Verify your email — Granite State G&C Tracker",
                    f'<p>Click to verify your email and enable notifications:</p><p><a href="{verify_url}" style="display:inline-block;padding:10px 24px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;font-weight:600;">Verify Email</a></p><p style="font-size:12px;color:#94a3b8;">Or copy: {verify_url}</p>',
                    f"Verify your email: {verify_url}")
                db.close()
                return redirect('/account?verify=sent')
            db.close()
    return render_template('register.html', error=error)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if session.get('user_id'):
        return redirect('/')
    error = None
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE email = ? AND is_active = 1", (email,)).fetchone()
        if user and check_password_hash(user['password_hash'], password):
            session['user_id'] = user['id']
            db.execute("UPDATE users SET last_login = ? WHERE id = ?",
                       (datetime.now().isoformat(), user['id']))
            db.commit()
            db.close()
            next_url = request.args.get('next', '/')
            return redirect(next_url)
        error = 'Invalid email or password.'
        db.close()
    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')


@app.route('/account')
@login_required
def account():
    user = get_current_user()
    return render_template('account.html', user=user)


@app.route('/account/notifications', methods=['POST'])
@login_required
def update_notifications():
    notify = 1 if request.form.get('notify_new_meetings') else 0
    db = get_db()
    db.execute("UPDATE users SET notify_new_meetings = ? WHERE id = ?",
               (notify, session['user_id']))
    db.commit()
    db.close()
    return redirect(url_for('account'))


@app.route('/account/resend-verification', methods=['POST'])
@login_required
def resend_verification():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id = ?", (session['user_id'],)).fetchone()
    if user and not user['email_verified']:
        token = user['verify_token'] or secrets.token_urlsafe(32)
        if not user['verify_token']:
            db.execute("UPDATE users SET verify_token = ? WHERE id = ?", (token, user['id']))
            db.commit()
        verify_url = f"{SITE_URL}/verify/{token}"
        _send_email(user['email'], "Verify your email — Granite State G&C Tracker",
            f'<p>Click to verify your email:</p><p><a href="{verify_url}" style="display:inline-block;padding:10px 24px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;font-weight:600;">Verify Email</a></p><p style="font-size:12px;color:#94a3b8;">Or copy: {verify_url}</p>',
            f"Verify your email: {verify_url}")
    db.close()
    return redirect('/account?verify=sent')


@app.route('/verify/<token>')
def verify_email(token):
    db = get_db()
    user = db.execute("SELECT id FROM users WHERE verify_token = ?", (token,)).fetchone()
    if user:
        db.execute("UPDATE users SET email_verified = 1, verify_token = NULL, notify_new_meetings = 1 WHERE id = ?", (user['id'],))
        db.commit()
        session['user_id'] = user['id']
        db.close()
        return redirect('/account?verified=1')
    db.close()
    return render_template('message.html', title='Invalid Link', message='This verification link is invalid or has already been used.'), 400


@app.route('/forgot', methods=['GET', 'POST'])
def forgot_password():
    sent = False
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        db = get_db()
        user = db.execute("SELECT id FROM users WHERE email = ? AND is_active = 1", (email,)).fetchone()
        if user:
            token = secrets.token_urlsafe(32)
            expires = (datetime.now().replace(microsecond=0) + timedelta(hours=1)).isoformat()
            db.execute("UPDATE users SET reset_token = ?, reset_token_expires = ? WHERE id = ?",
                       (token, expires, user['id']))
            db.commit()
            reset_url = f"{SITE_URL}/reset/{token}"
            _send_email(email, "Reset your password — Granite State G&C Tracker",
                f'<p>Click to reset your password (expires in 1 hour):</p><p><a href="{reset_url}" style="display:inline-block;padding:10px 24px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;font-weight:600;">Reset Password</a></p><p style="font-size:12px;color:#94a3b8;">Or copy: {reset_url}</p><p style="font-size:12px;color:#94a3b8;">If you didn\'t request this, ignore this email.</p>',
                f"Reset your password (1 hour): {reset_url}\n\nIf you didn't request this, ignore this email.")
        db.close()
        sent = True  # Always show sent message (don't reveal if email exists)
    return render_template('forgot.html', sent=sent)


@app.route('/reset/<token>', methods=['GET', 'POST'])
def reset_password(token):
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE reset_token = ? AND is_active = 1", (token,)).fetchone()
    if not user:
        db.close()
        return render_template('message.html', title='Invalid Link', message='This reset link is invalid or has already been used.'), 400
    # Check expiry
    if user['reset_token_expires']:
        try:
            expires = datetime.fromisoformat(user['reset_token_expires'])
            if datetime.now() > expires:
                db.close()
                return render_template('message.html', title='Link Expired', message='This reset link has expired. Please request a new one.'), 400
        except ValueError:
            pass
    error = None
    if request.method == 'POST':
        password = request.form.get('password', '')
        confirm = request.form.get('confirm', '')
        if len(password) < 8:
            error = 'Password must be at least 8 characters.'
        elif password != confirm:
            error = 'Passwords do not match.'
        else:
            db.execute("UPDATE users SET password_hash = ?, reset_token = NULL, reset_token_expires = NULL WHERE id = ?",
                       (generate_password_hash(password), user['id']))
            db.commit()
            session['user_id'] = user['id']
            db.close()
            return redirect('/account?reset=1')
    db.close()
    return render_template('reset.html', token=token, error=error)


@app.route('/unsubscribe/<token>')
def unsubscribe(token):
    db = get_db()
    user = db.execute("SELECT id, email FROM users WHERE unsubscribe_token = ?", (token,)).fetchone()
    if user:
        db.execute("UPDATE users SET notify_new_meetings = 0 WHERE id = ?", (user['id'],))
        db.commit()
        db.close()
        return render_template('message.html', title='Unsubscribed',
            message=f"You've been unsubscribed from email notifications. You can re-enable them anytime from your account settings.")
    db.close()
    return render_template('message.html', title='Invalid Link', message='This unsubscribe link is invalid.'), 400


# ─── RSS FEED ───

@app.route('/feed.xml')
@app.route('/rss')
def rss_feed():
    db = get_db()
    meetings = db.execute("""
        SELECT m.id, m.meeting_date, m.title, m.item_count,
               COALESCE((SELECT SUM(amount) FROM agenda_items WHERE meeting_id = m.id AND amount IS NOT NULL), 0) as total_value,
               COALESCE((SELECT COUNT(*) FROM agenda_items WHERE meeting_id = m.id AND item_type = 'contract'), 0) as contracts
        FROM meetings m WHERE m.item_count > 0
        ORDER BY m.meeting_date DESC LIMIT 20
    """).fetchall()

    items_xml = ""
    for m in meetings:
        try:
            dt = datetime.strptime(m['meeting_date'], '%Y-%m-%d')
            pub_date = dt.strftime('%a, %d %b %Y 07:00:00 -0500')
            date_display = dt.strftime('%B %-d, %Y')
        except ValueError:
            pub_date = ""
            date_display = m['meeting_date']

        from notifications import format_currency
        value_str = format_currency(m['total_value'])
        link = f"{SITE_URL}/meeting/{m['id']}"

        # Get top 5 items for description
        top = db.execute("""
            SELECT description, amount, vendor, department FROM agenda_items
            WHERE meeting_id = ? AND amount IS NOT NULL ORDER BY amount DESC LIMIT 5
        """, (m['id'],)).fetchall()
        desc_lines = [f"<p><strong>{m['item_count']} items</strong> | Total value: {value_str} | {m['contracts']} contracts</p><ul>"]
        for t in top:
            amt = format_currency(t['amount'])
            desc = (t['description'] or '')[:150]
            desc_lines.append(f"<li>{amt} — {desc}</li>")
        desc_lines.append("</ul>")
        description = "\n".join(desc_lines)
        # Escape for XML CDATA
        title_text = f"G&amp;C Agenda: {date_display} — {m['item_count']} Items, {value_str}"

        items_xml += f"""    <item>
      <title>{title_text}</title>
      <link>{link}</link>
      <guid isPermaLink="true">{link}</guid>
      <pubDate>{pub_date}</pubDate>
      <description><![CDATA[{description}]]></description>
    </item>\n"""

    db.close()

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>Granite State G&amp;C Tracker</title>
    <link>{SITE_URL}</link>
    <description>NH Governor &amp; Executive Council meeting agendas, contracts, and votes. Unofficial tracker.</description>
    <language>en-us</language>
    <atom:link href="{SITE_URL}/feed.xml" rel="self" type="application/rss+xml"/>
{items_xml}  </channel>
</rss>"""
    return Response(xml, mimetype='application/rss+xml')


@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404


if __name__ == '__main__':
    app.run(debug=True, port=5050)
