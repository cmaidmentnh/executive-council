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
            return f"${v/1_000_000:,.2f}M"
        return f"${v:,.0f}"
    except (ValueError, TypeError):
        return ""


@app.template_filter('short_date')
def short_date_filter(value):
    if not value:
        return ""
    try:
        dt = datetime.strptime(value, '%Y-%m-%d')
        return dt.strftime('%b %d, %Y')
    except ValueError:
        return value


# ─── ROUTES ───

@app.route('/')
def index():
    db = get_db()
    # Key stats
    stats = {}
    stats['meetings'] = db.execute("SELECT COUNT(*) FROM meetings").fetchone()[0]
    stats['agenda_items'] = db.execute("SELECT COUNT(*) FROM agenda_items").fetchone()[0]
    stats['actions'] = db.execute("SELECT COUNT(*) FROM council_actions").fetchone()[0]
    stats['vote_records'] = db.execute("SELECT COUNT(*) FROM councilor_vote_records").fetchone()[0]
    stats['pdfs'] = db.execute("SELECT COUNT(*) FROM item_downloads").fetchone()[0]
    stats['total_spending'] = db.execute("SELECT SUM(amount) FROM agenda_items WHERE amount > 0").fetchone()[0]
    stats['vendors'] = db.execute("SELECT COUNT(DISTINCT vendor) FROM agenda_items WHERE vendor IS NOT NULL").fetchone()[0]
    stats['departments'] = db.execute("SELECT COUNT(DISTINCT department) FROM agenda_items WHERE department IS NOT NULL").fetchone()[0]
    stats['contested'] = db.execute("SELECT COUNT(*) FROM council_actions WHERE dissenting_votes IS NOT NULL AND dissenting_votes <> ''").fetchone()[0]

    # Recent meetings
    recent = db.execute("""
        SELECT m.id, m.meeting_date, m.item_count,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id) as actions,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id
                AND dissenting_votes IS NOT NULL AND dissenting_votes <> '') as contested
        FROM meetings m ORDER BY m.meeting_date DESC LIMIT 10
    """).fetchall()

    # Yearly spending
    yearly = db.execute("""
        SELECT SUBSTR(m.meeting_date, 1, 4) as year,
               COUNT(DISTINCT m.id) as meetings,
               SUM(ai.amount) as total
        FROM meetings m
        JOIN agenda_items ai ON ai.meeting_id = m.id AND ai.amount > 0
        GROUP BY year ORDER BY year
    """).fetchall()

    db.close()
    return render_template('index.html', stats=stats, recent=recent, yearly=yearly)


@app.route('/meetings')
def meetings():
    db = get_db()
    year = request.args.get('year', '')
    page = int(request.args.get('page', 1))
    per_page = 50

    query = """
        SELECT m.id, m.meeting_date, m.title, m.item_count,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id) as actions,
               (SELECT COUNT(*) FROM council_actions WHERE meeting_id = m.id
                AND dissenting_votes IS NOT NULL AND dissenting_votes <> '') as contested,
               (SELECT COUNT(*) FROM item_downloads WHERE meeting_id = m.id) as pdfs
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

    # Non-vote actions (confirmations, resignations, nominations)
    other_actions = db.execute("""
        SELECT * FROM council_actions
        WHERE meeting_id = ? AND action_type != 'vote' AND action_type != 'minutes_approval'
        ORDER BY action_type, item_number
    """, (meeting_id,)).fetchall()

    # Meeting docs
    docs = db.execute("SELECT * FROM meeting_downloads WHERE meeting_id = ?", (meeting_id,)).fetchall()

    db.close()
    return render_template('meeting_detail.html', meeting=meeting, items=items,
                           other_actions=other_actions, docs=docs, r2_base=R2_BASE)


@app.route('/councilors')
def councilors():
    db = get_db()

    # Current councilors
    current = db.execute("""
        SELECT c.*,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'yes') as yes_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'no') as no_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'abstain') as abstain_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id) as total_votes
        FROM councilors c
        WHERE c.end_date IS NULL
        ORDER BY c.district
    """).fetchall()

    # All historical councilors
    historical = db.execute("""
        SELECT c.*,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'yes') as yes_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id AND vote = 'no') as no_votes,
               (SELECT COUNT(*) FROM councilor_vote_records WHERE councilor_id = c.id) as total_votes
        FROM councilors c
        WHERE c.end_date IS NOT NULL
        ORDER BY c.end_date DESC
    """).fetchall()

    db.close()
    return render_template('councilors.html', current=current, historical=historical)


@app.route('/councilor/<int:councilor_id>')
def councilor_detail(councilor_id):
    db = get_db()
    councilor = db.execute("SELECT * FROM councilors WHERE id = ?", (councilor_id,)).fetchone()
    if not councilor:
        abort(404)

    # All terms for this person
    terms = db.execute("""
        SELECT * FROM councilors WHERE name = ? ORDER BY start_date
    """, (councilor['name'],)).fetchall()

    # Get all councilor IDs for this person (multiple terms)
    term_ids = [t['id'] for t in terms]
    placeholders = ','.join('?' * len(term_ids))

    # Vote summary
    vote_summary = db.execute(f"""
        SELECT vote, COUNT(*) as cnt
        FROM councilor_vote_records WHERE councilor_id IN ({placeholders})
        GROUP BY vote
    """, term_ids).fetchall()

    # Dissenting votes (most recent first)
    dissents = db.execute(f"""
        SELECT cvr.meeting_date, cvr.item_number, ca.description, ca.outcome
        FROM councilor_vote_records cvr
        JOIN council_actions ca ON ca.id = cvr.action_id
        WHERE cvr.councilor_id IN ({placeholders}) AND cvr.vote = 'no'
        ORDER BY cvr.meeting_date DESC
        LIMIT 100
    """, term_ids).fetchall()

    # Monthly voting pattern
    monthly = db.execute(f"""
        SELECT SUBSTR(meeting_date, 1, 7) as month,
               SUM(CASE WHEN vote='yes' THEN 1 ELSE 0 END) as yes_ct,
               SUM(CASE WHEN vote='no' THEN 1 ELSE 0 END) as no_ct
        FROM councilor_vote_records
        WHERE councilor_id IN ({placeholders})
        GROUP BY month ORDER BY month
    """, term_ids).fetchall()

    db.close()
    return render_template('councilor_detail.html', councilor=councilor, terms=terms,
                           vote_summary=vote_summary, dissents=dissents, monthly=monthly)


@app.route('/vendors')
def vendors():
    db = get_db()
    page = int(request.args.get('page', 1))
    search = request.args.get('q', '')
    per_page = 50

    if search:
        vendors_list = db.execute("""
            SELECT vendor, COUNT(*) as item_count, SUM(amount) as total_amount,
                   MIN(m.meeting_date) as first_seen, MAX(m.meeting_date) as last_seen
            FROM agenda_items ai
            JOIN meetings m ON m.id = ai.meeting_id
            WHERE ai.vendor IS NOT NULL AND ai.vendor LIKE ?
            GROUP BY ai.vendor
            ORDER BY total_amount DESC
            LIMIT ? OFFSET ?
        """, (f'%{search}%', per_page, (page - 1) * per_page)).fetchall()
        total = db.execute(
            "SELECT COUNT(DISTINCT vendor) FROM agenda_items WHERE vendor IS NOT NULL AND vendor LIKE ?",
            (f'%{search}%',)
        ).fetchone()[0]
    else:
        vendors_list = db.execute("""
            SELECT vendor, COUNT(*) as item_count, SUM(amount) as total_amount,
                   MIN(m.meeting_date) as first_seen, MAX(m.meeting_date) as last_seen
            FROM agenda_items ai
            JOIN meetings m ON m.id = ai.meeting_id
            WHERE ai.vendor IS NOT NULL AND ai.amount > 0
            GROUP BY ai.vendor
            ORDER BY total_amount DESC
            LIMIT ? OFFSET ?
        """, (per_page, (page - 1) * per_page)).fetchall()
        total = db.execute(
            "SELECT COUNT(DISTINCT vendor) FROM agenda_items WHERE vendor IS NOT NULL AND amount > 0"
        ).fetchone()[0]

    db.close()
    return render_template('vendors.html', vendors=vendors_list, search=search,
                           page=page, total=total, per_page=per_page)


@app.route('/departments')
def departments():
    db = get_db()
    depts = db.execute("""
        SELECT ai.department, COUNT(*) as item_count, SUM(ai.amount) as total_amount,
               COUNT(DISTINCT ai.meeting_id) as meetings
        FROM agenda_items ai
        WHERE ai.department IS NOT NULL
        GROUP BY ai.department
        ORDER BY total_amount DESC
    """).fetchall()
    db.close()
    return render_template('departments.html', departments=depts)


@app.route('/search')
def search():
    db = get_db()
    q = request.args.get('q', '')
    page = int(request.args.get('page', 1))
    per_page = 50

    if not q or len(q) < 3:
        db.close()
        return render_template('search.html', results=[], query=q, total=0, page=1, per_page=per_page)

    results = db.execute("""
        SELECT ai.*, m.meeting_date
        FROM agenda_items ai
        JOIN meetings m ON m.id = ai.meeting_id
        WHERE ai.description LIKE ? OR ai.vendor LIKE ? OR ai.department LIKE ?
        ORDER BY m.meeting_date DESC
        LIMIT ? OFFSET ?
    """, (f'%{q}%', f'%{q}%', f'%{q}%', per_page, (page - 1) * per_page)).fetchall()

    total = db.execute("""
        SELECT COUNT(*) FROM agenda_items
        WHERE description LIKE ? OR vendor LIKE ? OR department LIKE ?
    """, (f'%{q}%', f'%{q}%', f'%{q}%')).fetchone()[0]

    db.close()
    return render_template('search.html', results=results, query=q,
                           total=total, page=page, per_page=per_page)


@app.route('/contested')
def contested():
    db = get_db()
    page = int(request.args.get('page', 1))
    per_page = 50

    actions = db.execute("""
        SELECT ca.*, m.meeting_date as mdate
        FROM council_actions ca
        JOIN meetings m ON m.id = ca.meeting_id
        WHERE ca.dissenting_votes IS NOT NULL AND ca.dissenting_votes <> ''
        ORDER BY m.meeting_date DESC
        LIMIT ? OFFSET ?
    """, (per_page, (page - 1) * per_page)).fetchall()

    total = db.execute("""
        SELECT COUNT(*) FROM council_actions
        WHERE dissenting_votes IS NOT NULL AND dissenting_votes <> ''
    """).fetchone()[0]

    db.close()
    return render_template('contested.html', actions=actions, total=total,
                           page=page, per_page=per_page)


# ─── API ENDPOINTS ───

@app.route('/api/stats')
def api_stats():
    db = get_db()
    stats = {
        'meetings': db.execute("SELECT COUNT(*) FROM meetings").fetchone()[0],
        'agenda_items': db.execute("SELECT COUNT(*) FROM agenda_items").fetchone()[0],
        'council_actions': db.execute("SELECT COUNT(*) FROM council_actions").fetchone()[0],
        'vote_records': db.execute("SELECT COUNT(*) FROM councilor_vote_records").fetchone()[0],
        'item_pdfs': db.execute("SELECT COUNT(*) FROM item_downloads").fetchone()[0],
        'total_spending': db.execute("SELECT SUM(amount) FROM agenda_items WHERE amount > 0").fetchone()[0],
        'contested_votes': db.execute("SELECT COUNT(*) FROM council_actions WHERE dissenting_votes IS NOT NULL AND dissenting_votes <> ''").fetchone()[0],
    }
    db.close()
    return jsonify(stats)


@app.route('/api/yearly-spending')
def api_yearly_spending():
    db = get_db()
    data = db.execute("""
        SELECT SUBSTR(m.meeting_date, 1, 4) as year,
               SUM(ai.amount) as total,
               COUNT(*) as items
        FROM meetings m
        JOIN agenda_items ai ON ai.meeting_id = m.id AND ai.amount > 0
        GROUP BY year ORDER BY year
    """).fetchall()
    db.close()
    return jsonify([dict(r) for r in data])


@app.route('/api/councilor-votes/<int:councilor_id>')
def api_councilor_votes(councilor_id):
    db = get_db()
    # Get all term IDs for this councilor
    councilor = db.execute("SELECT name FROM councilors WHERE id = ?", (councilor_id,)).fetchone()
    if not councilor:
        abort(404)
    terms = db.execute("SELECT id FROM councilors WHERE name = ?", (councilor['name'],)).fetchall()
    ids = [t['id'] for t in terms]
    ph = ','.join('?' * len(ids))

    data = db.execute(f"""
        SELECT SUBSTR(meeting_date, 1, 7) as month,
               SUM(CASE WHEN vote='yes' THEN 1 ELSE 0 END) as yes_votes,
               SUM(CASE WHEN vote='no' THEN 1 ELSE 0 END) as no_votes,
               SUM(CASE WHEN vote='abstain' THEN 1 ELSE 0 END) as abstain_votes
        FROM councilor_vote_records WHERE councilor_id IN ({ph})
        GROUP BY month ORDER BY month
    """, ids).fetchall()
    db.close()
    return jsonify([dict(r) for r in data])


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
