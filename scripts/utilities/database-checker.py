#!/usr/bin/env python3
"""
Database Status Checker for Hollywood Adaptations Project
Shows current state of the database and identifies gaps
"""

import sqlite3
import os
from pathlib import Path

def check_database_status(db_path="data/databases/holreg_research.db"):
    """Check the current status of the research database"""
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        print("\nLooks like you need to create the database first!")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 70)
    print("HOLLYWOOD ADAPTATIONS DATABASE STATUS CHECK")
    print("=" * 70)
    
    # Check what tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    print("\n📊 EXISTING TABLES:")
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  ✓ {table_name}: {count} records")
    
    # Check core films table structure
    if any('films' in t[0] for t in tables):
        print("\n🎬 FILMS TABLE STRUCTURE:")
        cursor.execute("PRAGMA table_info(films)")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
    
    # Data completeness check
    print("\n📈 DATA COMPLETENESS:")
    
    # Check for missing critical data
    if any('films' in t[0] for t in tables):
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN literary_credits IS NULL OR literary_credits = '' THEN 1 END) as missing_author,
                COUNT(CASE WHEN subjects IS NULL OR subjects = '' THEN 1 END) as missing_subjects,
                COUNT(CASE WHEN director IS NULL OR director = '' THEN 1 END) as missing_director
            FROM films
        """)
        stats = cursor.fetchone()
        
        print(f"  Total films: {stats[0]}")
        print(f"  Missing author info: {stats[1]} ({stats[1]/stats[0]*100:.1f}%)")
        print(f"  Missing subjects: {stats[2]} ({stats[2]/stats[0]*100:.1f}%)")
        print(f"  Missing director: {stats[3]} ({stats[3]/stats[0]*100:.1f}%)")
    
    # Check controlled vocabulary mapping
    print("\n🏷️  CONTROLLED VOCABULARY STATUS:")
    if any('controlled_terms' in t[0] for t in tables):
        cursor.execute("SELECT facet, COUNT(*) as count FROM controlled_terms GROUP BY facet")
        facets = cursor.fetchall()
        for facet, count in facets:
            print(f"  {facet}: {count} terms")
    else:
        print("  ⚠️  Controlled vocabulary table not found")
    
    # Check if mappings exist
    if any('film_subjects_controlled' in t[0] for t in tables):
        cursor.execute("""
            SELECT COUNT(DISTINCT film_id) as mapped_films
            FROM film_subjects_controlled
        """)
        mapped = cursor.fetchone()[0]
        print(f"\n  Films with controlled vocabulary mappings: {mapped}")
    
    # Identify what's missing
    print("\n⚠️  POTENTIAL GAPS:")
    
    missing_items = []
    
    # Check for expected tables
    expected_tables = ['films', 'controlled_terms', 'film_subjects_controlled', 'authors']
    for table in expected_tables:
        if not any(table in t[0] for t in tables):
            missing_items.append(f"Table '{table}' not found")
    
    # Check for author data
    if not any('authors' in t[0] for t in tables):
        missing_items.append("Author biographical data table")
    
    # Check for source text data
    if not any('source_texts' in t[0] for t in tables):
        missing_items.append("Source texts (novels/stories) table")
    
    if missing_items:
        for item in missing_items:
            print(f"  - {item}")
    else:
        print("  ✓ All expected components found!")
    
    # Show sample data
    print("\n📋 SAMPLE DATA (first 5 films):")
    if any('films' in t[0] for t in tables):
        cursor.execute("SELECT title, release_year, literary_credits FROM films LIMIT 5")
        for film in cursor.fetchall():
            print(f"  - {film[0]} ({film[1]}) - Author: {film[2] or 'Unknown'}")
    
    conn.close()
    
    # Suggest next steps
    print("\n💡 SUGGESTED NEXT STEPS:")
    print("  1. Import CSV data into database if not done")
    print("  2. Clean and standardize author names")
    print("  3. Map film subjects to controlled vocabulary")
    print("  4. Add author biographical data")
    print("  5. Link films to source texts")
    print("  6. Add survival/availability status")

if __name__ == "__main__":
    # Check multiple possible locations
    possible_paths = [
        "data/databases/holreg_research.db",
        "adaptation_research.db",
        "data/adaptation_research.db",
        "holreg_research.db"  # I see this in your files
    ]
    
    found = False
    for path in possible_paths:
        if os.path.exists(path):
            print(f"Found database at: {path}\n")
            check_database_status(path)
            found = True
            break
    
    if not found:
        print("No database found. Would you like me to create one from your CSV data?")