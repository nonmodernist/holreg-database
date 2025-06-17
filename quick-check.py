#!/usr/bin/env python3
"""
Quick check of what's actually in the database
"""

import sqlite3

def quick_check(db_path='data/databases/holreg_research.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 80)
    print("QUICK DATABASE CHECK")
    print("=" * 80)
    
    # 1. Total films
    cursor.execute("SELECT COUNT(*) FROM films")
    total = cursor.fetchone()[0]
    print(f"\nTotal films in database: {total}")
    
    # 2. Check Alice Hegan Rice specifically
    print("\n--- ALICE HEGAN RICE CHECK ---")
    cursor.execute("""
        SELECT literary_credits, COUNT(*) as count
        FROM films
        WHERE LOWER(literary_credits) LIKE '%alice%hegan%rice%'
        GROUP BY literary_credits
        ORDER BY count DESC
    """)
    
    for row in cursor.fetchall():
        print(f"'{row[0]}': {row[1]} films")
        
        # Show first few films
        cursor2 = conn.cursor()
        cursor2.execute("""
            SELECT title, release_year
            FROM films
            WHERE literary_credits = ?
            LIMIT 5
        """, (row[0],))
        
        for film in cursor2.fetchall():
            print(f"  - {film[0]} ({film[1]})")
    
    # 3. Check Gene Stratton-Porter
    print("\n--- GENE STRATTON-PORTER CHECK ---")
    cursor.execute("""
        SELECT literary_credits, COUNT(*) as count
        FROM films
        WHERE LOWER(literary_credits) LIKE '%stratton%porter%'
        GROUP BY literary_credits
        ORDER BY count DESC
    """)
    
    for row in cursor.fetchall():
        print(f"'{row[0]}': {row[1]} films")
        
        # Show all films for this author
        cursor2 = conn.cursor()
        cursor2.execute("""
            SELECT title, release_year
            FROM films
            WHERE literary_credits = ?
            ORDER BY release_year
        """, (row[0],))
        
        for film in cursor2.fetchall():
            print(f"  - {film[0]} ({film[1]})")
    
    # 4. Top 10 authors by film count
    print("\n--- TOP 10 AUTHORS BY FILM COUNT ---")
    cursor.execute("""
        SELECT literary_credits, COUNT(*) as count
        FROM films
        WHERE literary_credits IS NOT NULL
        GROUP BY literary_credits
        ORDER BY count DESC
        LIMIT 10
    """)
    
    for i, row in enumerate(cursor.fetchall(), 1):
        print(f"{i}. '{row[0]}': {row[1]} films")
    
    # 5. Check for weird data
    print("\n--- DATA QUALITY ISSUES ---")
    
    # Check longest author name
    cursor.execute("""
        SELECT literary_credits, LENGTH(literary_credits) as len
        FROM films
        WHERE literary_credits IS NOT NULL
        ORDER BY len DESC
        LIMIT 5
    """)
    
    print("\nLongest author names:")
    for row in cursor.fetchall():
        print(f"  Length {row[1]}: '{row[0]}'")
    
    conn.close()

if __name__ == "__main__":
    import sys
    db_path = sys.argv[1] if len(sys.argv) > 1 else 'data/databases/holreg_research.db'
    quick_check(db_path)