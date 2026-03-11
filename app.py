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


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ─── TEMPLATE FILTERS ───

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
               m.meeting_date, m.id as meeting_id
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

    db.close()
    return render_template('index.html', stats=stats, latest=latest, recent=recent,
                           recent_contested=recent_contested, councilors=councilors)


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
               id2.filename as pdf_filename
        FROM agenda_items ai
        LEFT JOIN council_actions ca ON ca.meeting_id = ai.meeting_id
            AND ca.item_number = ai.item_number AND ca.action_type = 'vote'
        LEFT JOIN item_downloads id2 ON id2.meeting_id = ai.meeting_id
            AND id2.item_number = ai.item_number AND id2.sub_item IS NULL
        WHERE ai.meeting_id = ?
        ORDER BY CAST(CASE WHEN ai.item_number GLOB '[0-9]*' THEN ai.item_number ELSE '9999' END AS INTEGER),
                 ai.item_number
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

    historical = db.execute("""
        SELECT c.*,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'yes') as yes_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'no') as no_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id) as total_votes
        FROM councilors c WHERE c.end_date IS NOT NULL ORDER BY c.end_date DESC
    """).fetchall()

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
               ai.vendor, ai.amount
        FROM councilor_vote_records cvr
        JOIN council_actions ca ON ca.id = cvr.action_id
        LEFT JOIN agenda_items ai ON ai.meeting_id = cvr.meeting_id AND ai.item_number = cvr.item_number
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

    vendors_list = db.execute(f"""
        SELECT ai.vendor, COUNT(*) as item_count, SUM(ai.amount) as total_amount,
               MIN(m.meeting_date) as first_seen, MAX(m.meeting_date) as last_seen
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        {where}
        GROUP BY ai.vendor
        ORDER BY total_amount DESC
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    total = db.execute(f"""
        SELECT COUNT(DISTINCT ai.vendor) FROM agenda_items ai {where}
    """, params).fetchone()[0]

    db.close()
    return render_template('vendors.html', vendors=vendors_list, search=search,
                           page=page, total=total, per_page=per_page)


@app.route('/vendor/<path:vendor_name>')
def vendor_detail(vendor_name):
    db = get_db()
    items = db.execute("""
        SELECT ai.*, m.meeting_date,
               ca.outcome, ca.dissenting_votes
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        LEFT JOIN council_actions ca ON ca.meeting_id = ai.meeting_id
            AND ca.item_number = ai.item_number AND ca.action_type = 'vote'
        WHERE ai.vendor = ?
        ORDER BY m.meeting_date DESC
    """, (vendor_name,)).fetchall()

    if not items:
        abort(404)

    total_value = sum(i['amount'] or 0 for i in items)
    db.close()
    return render_template('vendor_detail.html', vendor_name=vendor_name,
                           items=items, total_value=total_value)


@app.route('/departments')
def departments():
    db = get_db()
    depts = db.execute("""
        SELECT ai.department, COUNT(*) as item_count, SUM(ai.amount) as total_amount,
               COUNT(DISTINCT ai.meeting_id) as meetings,
               MIN(m.meeting_date) as first_seen, MAX(m.meeting_date) as last_seen
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE ai.department IS NOT NULL
        GROUP BY ai.department
        ORDER BY total_amount DESC
    """).fetchall()
    db.close()
    return render_template('departments.html', departments=depts)


@app.route('/department/<path:dept_name>')
def department_detail(dept_name):
    db = get_db()
    page = int(request.args.get('page', 1))
    per_page = 50

    # Top vendors for this department
    top_vendors = db.execute("""
        SELECT vendor, COUNT(*) as item_count, SUM(amount) as total_amount
        FROM agenda_items
        WHERE department = ? AND vendor IS NOT NULL AND amount > 0
        GROUP BY vendor ORDER BY total_amount DESC LIMIT 15
    """, (dept_name,)).fetchall()

    # Item type breakdown
    type_breakdown = db.execute("""
        SELECT item_type, COUNT(*) as cnt, SUM(amount) as total
        FROM agenda_items WHERE department = ?
        GROUP BY item_type ORDER BY cnt DESC
    """, (dept_name,)).fetchall()

    # Recent items
    items = db.execute("""
        SELECT ai.*, m.meeting_date,
               ca.outcome, ca.dissenting_votes
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        LEFT JOIN council_actions ca ON ca.meeting_id = ai.meeting_id
            AND ca.item_number = ai.item_number AND ca.action_type = 'vote'
        WHERE ai.department = ?
        ORDER BY m.meeting_date DESC
        LIMIT ? OFFSET ?
    """, (dept_name, per_page, (page - 1) * per_page)).fetchall()

    total = db.execute("SELECT COUNT(*) FROM agenda_items WHERE department = ?", (dept_name,)).fetchone()[0]
    total_value = db.execute("SELECT SUM(amount) FROM agenda_items WHERE department = ? AND amount > 0",
                             (dept_name,)).fetchone()[0]

    db.close()
    return render_template('department_detail.html', dept_name=dept_name, items=items,
                           top_vendors=top_vendors, type_breakdown=type_breakdown,
                           total=total, total_value=total_value, page=page, per_page=per_page)


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
                               total=0, page=1, per_page=per_page)

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

    db.close()
    return render_template('search.html', results=results, query=q, item_type=item_type,
                           total=total, page=page, per_page=per_page)


@app.route('/contested')
def contested():
    db = get_db()
    page = int(request.args.get('page', 1))
    councilor = request.args.get('councilor', '')
    per_page = 50

    where = "ca.dissenting_votes IS NOT NULL AND ca.dissenting_votes <> ''"
    params = []
    if councilor:
        where += " AND ca.dissenting_votes LIKE ?"
        params.append(f'%{councilor}%')

    actions = db.execute(f"""
        SELECT ca.*, m.meeting_date as mdate,
               ai.vendor, ai.amount, ai.item_type
        FROM council_actions ca
        JOIN meetings m ON m.id = ca.meeting_id
        LEFT JOIN agenda_items ai ON ai.meeting_id = ca.meeting_id AND ai.item_number = ca.item_number
        WHERE {where}
        ORDER BY m.meeting_date DESC
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    total = db.execute(f"SELECT COUNT(*) FROM council_actions ca WHERE {where}", params).fetchone()[0]

    # Get all councilor names who have dissented for the filter
    dissenters = db.execute("""
        SELECT DISTINCT dissenting_votes FROM council_actions
        WHERE dissenting_votes IS NOT NULL AND dissenting_votes <> ''
    """).fetchall()
    # Parse out individual names
    all_names = set()
    for row in dissenters:
        for name in row['dissenting_votes'].split(','):
            name = name.strip()
            if name:
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
        SELECT ca.*, m.meeting_date as mdate
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


if __name__ == '__main__':
    app.run(debug=True, port=5050)
