#!/usr/bin/env python3
"""
Query tool for the NH Executive Council contracts database.

Usage:
    python3 query.py                          # Show summary stats
    python3 query.py search "vendor name"     # Search by vendor
    python3 query.py dept "DHHS"              # Search by department
    python3 query.py top [N]                  # Top N contracts by amount
    python3 query.py year YYYY                # All items in a year
    python3 query.py vendor-totals [N]        # Top N vendors by total amount
    python3 query.py dept-totals [N]          # Departments by total spending
    python3 query.py contracts [year]         # All contracts (optionally by year)
    python3 query.py item NUMBER DATE         # Specific item by number and date
"""

import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / "executive_council.db"


def get_conn():
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("Run scraper.py first to build the database.")
        sys.exit(1)
    return sqlite3.connect(DB_PATH)


def summary():
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM meetings WHERE scraped_at IS NOT NULL")
    meetings = c.fetchone()[0]

    c.execute("SELECT MIN(meeting_date), MAX(meeting_date) FROM meetings WHERE scraped_at IS NOT NULL")
    date_range = c.fetchone()

    c.execute("SELECT COUNT(*) FROM agenda_items")
    total_items = c.fetchone()[0]

    c.execute("SELECT item_type, COUNT(*) FROM agenda_items GROUP BY item_type ORDER BY COUNT(*) DESC")
    type_counts = c.fetchall()

    c.execute("SELECT COUNT(*), SUM(amount) FROM agenda_items WHERE amount IS NOT NULL")
    amount_row = c.fetchone()

    c.execute("SELECT COUNT(DISTINCT vendor) FROM agenda_items WHERE vendor IS NOT NULL")
    vendors = c.fetchone()[0]

    c.execute("""
        SELECT strftime('%Y', m.meeting_date) as year, COUNT(*) as items,
               SUM(CASE WHEN a.amount IS NOT NULL THEN a.amount ELSE 0 END) as total
        FROM agenda_items a
        JOIN meetings m ON a.meeting_id = m.id
        WHERE m.meeting_date IS NOT NULL
        GROUP BY year ORDER BY year
    """)
    yearly = c.fetchall()

    print(f"{'='*70}")
    print(f"NH EXECUTIVE COUNCIL DATABASE SUMMARY")
    print(f"{'='*70}")
    print(f"Date range:      {date_range[0]} to {date_range[1]}")
    print(f"Meetings:        {meetings}")
    print(f"Total items:     {total_items}")
    print(f"Items w/amounts: {amount_row[0]}")
    print(f"Total value:     ${amount_row[1]:,.2f}" if amount_row[1] else "Total value:     $0")
    print(f"Unique vendors:  {vendors}")
    print()
    print(f"{'Item Type':<20} {'Count':>8}")
    print(f"{'-'*28}")
    for item_type, count in type_counts:
        print(f"{item_type:<20} {count:>8}")
    print()
    print(f"{'Year':<6} {'Items':>8} {'Total Value':>20}")
    print(f"{'-'*36}")
    for year, items, total in yearly:
        print(f"{year:<6} {items:>8} ${total:>18,.2f}")

    conn.close()


def search_vendor(query):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT m.meeting_date, a.item_number, a.sub_item, a.vendor,
               a.vendor_city, a.vendor_state, a.amount, a.description, a.department
        FROM agenda_items a
        JOIN meetings m ON a.meeting_id = m.id
        WHERE a.vendor LIKE ? OR a.description LIKE ?
        ORDER BY m.meeting_date DESC
    """, (f'%{query}%', f'%{query}%'))
    rows = c.fetchall()
    print(f"\nSearch results for '{query}': {len(rows)} items\n")
    for date, num, sub, vendor, city, state, amount, desc, dept in rows:
        amt = f"${amount:,.2f}" if amount else "N/A"
        loc = f"{city}, {state}" if city else ""
        print(f"{date} #{num}{sub or ''} | {amt:>15} | {vendor or 'N/A':<40} {loc}")
        print(f"  {dept} | {desc[:100]}")
        print()
    conn.close()


def search_dept(query):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT m.meeting_date, a.item_number, a.sub_item, a.vendor,
               a.amount, a.description, a.department, a.item_type
        FROM agenda_items a
        JOIN meetings m ON a.meeting_id = m.id
        WHERE a.department LIKE ?
        ORDER BY m.meeting_date DESC
    """, (f'%{query}%',))
    rows = c.fetchall()
    print(f"\nDepartment search '{query}': {len(rows)} items\n")
    total = sum(r[4] or 0 for r in rows)
    print(f"Total value: ${total:,.2f}\n")
    for date, num, sub, vendor, amount, desc, dept, itype in rows[:50]:
        amt = f"${amount:,.2f}" if amount else "N/A"
        print(f"{date} #{num}{sub or ''} [{itype}] {amt:>15} | {vendor or '':<35} | {desc[:70]}")
    if len(rows) > 50:
        print(f"\n... and {len(rows) - 50} more items")
    conn.close()


def top_contracts(n=25):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT m.meeting_date, a.item_number, a.sub_item, a.vendor,
               a.vendor_city, a.vendor_state, a.amount, a.description,
               a.department, a.item_type
        FROM agenda_items a
        JOIN meetings m ON a.meeting_id = m.id
        WHERE a.amount IS NOT NULL
        ORDER BY a.amount DESC
        LIMIT ?
    """, (n,))
    rows = c.fetchall()
    print(f"\nTop {n} items by dollar amount:\n")
    for i, (date, num, sub, vendor, city, state, amount, desc, dept, itype) in enumerate(rows, 1):
        loc = f"{city}, {state}" if city else ""
        print(f"{i:>3}. ${amount:>18,.2f} | {date} #{num}{sub or ''} [{itype}]")
        print(f"     {dept}")
        print(f"     {vendor or 'N/A'} {loc}")
        print(f"     {desc[:90]}")
        print()
    conn.close()


def year_items(year):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT m.meeting_date, m.title, a.item_number, a.sub_item, a.vendor,
               a.amount, a.description, a.department, a.item_type
        FROM agenda_items a
        JOIN meetings m ON a.meeting_id = m.id
        WHERE strftime('%Y', m.meeting_date) = ?
        ORDER BY m.meeting_date, CAST(a.item_number AS INTEGER), a.sub_item
    """, (str(year),))
    rows = c.fetchall()

    total = sum(r[5] or 0 for r in rows)
    contracts = [r for r in rows if r[8] == 'contract']
    contract_total = sum(r[5] or 0 for r in contracts)

    print(f"\n{year} Summary: {len(rows)} items, ${total:,.2f} total")
    print(f"Contracts: {len(contracts)}, ${contract_total:,.2f}")
    print()

    from collections import Counter
    types = Counter(r[8] for r in rows)
    for t, count in types.most_common():
        print(f"  {t}: {count}")
    print()

    # Show contracts only
    print(f"{'='*70}")
    print(f"CONTRACTS ({year})")
    print(f"{'='*70}")
    for date, title, num, sub, vendor, amount, desc, dept, itype in contracts:
        amt = f"${amount:,.2f}" if amount else "N/A"
        print(f"{date} #{num}{sub or ''} {amt:>15} | {vendor or 'N/A':<40}")
        print(f"  {dept} | {desc[:90]}")
        print()

    conn.close()


def vendor_totals(n=25):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT a.vendor, COUNT(*) as item_count, SUM(a.amount) as total,
               MIN(m.meeting_date) as first_date, MAX(m.meeting_date) as last_date
        FROM agenda_items a
        JOIN meetings m ON a.meeting_id = m.id
        WHERE a.vendor IS NOT NULL AND a.amount IS NOT NULL
        GROUP BY a.vendor
        ORDER BY total DESC
        LIMIT ?
    """, (n,))
    rows = c.fetchall()
    print(f"\nTop {n} vendors by total contract value:\n")
    print(f"{'Vendor':<45} {'Items':>6} {'Total Value':>18} {'First':>12} {'Last':>12}")
    print(f"{'-'*95}")
    for vendor, count, total, first, last in rows:
        print(f"{vendor[:44]:<45} {count:>6} ${total:>16,.2f} {first:>12} {last:>12}")
    conn.close()


def dept_totals(n=25):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT a.department, COUNT(*) as item_count, SUM(a.amount) as total,
               COUNT(DISTINCT strftime('%Y', m.meeting_date)) as years_active
        FROM agenda_items a
        JOIN meetings m ON a.meeting_id = m.id
        WHERE a.department IS NOT NULL AND a.department != '' AND a.amount IS NOT NULL
        GROUP BY a.department
        ORDER BY total DESC
        LIMIT ?
    """, (n,))
    rows = c.fetchall()
    print(f"\nTop {n} departments by total spending:\n")
    print(f"{'Department':<50} {'Items':>6} {'Years':>6} {'Total Value':>18}")
    print(f"{'-'*82}")
    for dept, count, total, years in rows:
        print(f"{dept[:49]:<50} {count:>6} {years:>6} ${total:>16,.2f}")
    conn.close()


def main():
    if len(sys.argv) < 2:
        summary()
        return

    cmd = sys.argv[1].lower()

    if cmd == 'search':
        search_vendor(' '.join(sys.argv[2:]))
    elif cmd == 'dept':
        search_dept(' '.join(sys.argv[2:]))
    elif cmd == 'top':
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 25
        top_contracts(n)
    elif cmd == 'year':
        year_items(sys.argv[2])
    elif cmd == 'vendor-totals':
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 25
        vendor_totals(n)
    elif cmd == 'dept-totals':
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 25
        dept_totals(n)
    else:
        print(__doc__)


if __name__ == '__main__':
    main()
