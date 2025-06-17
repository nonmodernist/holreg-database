#!/usr/bin/env python3
"""
Targeted cleaning script based on the actual data issues found
"""

import sqlite3
import re

def clean_database(db_path='data/databases/holreg_research.db', dry_run=True):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 80)
    print("CLEANING DATABASE" + (" (DRY RUN)" if dry_run else ""))
    print("=" * 80)
    
    # Create backup first
    if not dry_run:
        print("\nCreating backup table...")
        cursor.execute("DROP TABLE IF EXISTS films_backup")
        cursor.execute("CREATE TABLE films_backup AS SELECT * FROM films")
        print("✓ Backup created as 'films_backup'")
    
    changes = []
    
    # 1. Fix trailing spaces
    print("\n1. FIXING TRAILING SPACES:")
    cursor.execute("""
        SELECT id, literary_credits
        FROM films
        WHERE literary_credits != TRIM(literary_credits)
        AND literary_credits IS NOT NULL
    """)
    
    for row in cursor.fetchall():
        old_value = row[1]
        new_value = old_value.strip()
        changes.append(('literary_credits', row[0], old_value, new_value))
        print(f"  Film ID {row[0]}: '{old_value}' → '{new_value}'")
    
    # 2. Standardize co-author format
    print("\n2. STANDARDIZING CO-AUTHOR FORMAT:")
    cursor.execute("""
        SELECT id, literary_credits
        FROM films
        WHERE literary_credits LIKE '%|%'
    """)
    
    for row in cursor.fetchall():
        old_value = row[1]
        # Check if it's an AFI ID (ends with |numbers)
        if re.search(r'\|\s*\d+$', old_value):
            # It's an AFI ID, skip this for now (will be handled in step 4)
            continue
        # Clean up pipe-separated authors
        authors = [a.strip() for a in old_value.split('|') if a.strip()]
        new_value = ' | '.join(authors)
        if old_value != new_value:
            changes.append(('literary_credits', row[0], old_value, new_value))
            print(f"  Film ID {row[0]}: '{old_value}' → '{new_value}'")
    
    # 3. Fix double pipe issue
    print("\n3. FIXING DOUBLE PIPE ISSUES:")
    cursor.execute("""
        SELECT id, literary_credits
        FROM films
        WHERE literary_credits LIKE '%||%'
    """)
    
    for row in cursor.fetchall():
        old_value = row[1]
        # Split by || and clean
        parts = [p.strip() for p in old_value.split('||') if p.strip()]
        new_value = ' | '.join(parts)
        changes.append(('literary_credits', row[0], old_value, new_value))
        print(f"  Film ID {row[0]}: '{old_value}' → '{new_value}'")
    
    # 4. Clean writer field (remove AFI IDs)
    print("\n4. CLEANING WRITER FIELD:")
    cursor.execute("""
        SELECT id, writer
        FROM films
        WHERE writer LIKE '%|%'
        AND writer IS NOT NULL
    """)
    
    count = 0
    for row in cursor.fetchall():
        old_value = row[1]
        # Remove AFI ID pattern (|followed by numbers)
        new_value = re.sub(r'\s*\|\s*\d+$', '', old_value).strip()
        if old_value != new_value:
            changes.append(('writer', row[0], old_value, new_value))
            count += 1
            if count <= 10:  # Show first 10
                print(f"  Film ID {row[0]}: '{old_value}' → '{new_value}'")
    
    if count > 10:
        print(f"  ... and {count - 10} more writer field changes")
    
    # Show summary
    print("\n" + "=" * 80)
    print("SUMMARY:")
    print("=" * 80)
    print(f"Total changes to make: {len(changes)}")
    
    # Group by field
    field_counts = {}
    for field, _, _, _ in changes:
        field_counts[field] = field_counts.get(field, 0) + 1
    
    for field, count in field_counts.items():
        print(f"  {field}: {count} changes")
    
    # Apply changes if not dry run
    if not dry_run:
        print("\nApplying changes...")
        for field, film_id, old_val, new_val in changes:
            cursor.execute(f"UPDATE films SET {field} = ? WHERE id = ?", (new_val, film_id))
        
        conn.commit()
        print(f"✓ Applied {len(changes)} changes successfully!")
        
        # Verify results
        print("\n" + "=" * 80)
        print("VERIFICATION:")
        print("=" * 80)
        
        # Check Gene Stratton-Porter
        cursor.execute("""
            SELECT literary_credits, COUNT(*) as count
            FROM films
            WHERE LOWER(literary_credits) LIKE '%stratton%porter%'
            GROUP BY literary_credits
        """)
        
        print("\nGene Stratton-Porter after cleaning:")
        for row in cursor.fetchall():
            print(f"  '{row[0]}': {row[1]} films")
        
        # Check Alice Hegan Rice
        cursor.execute("""
            SELECT literary_credits, COUNT(*) as count
            FROM films
            WHERE LOWER(literary_credits) LIKE '%alice%hegan%rice%'
            GROUP BY literary_credits
        """)
        
        print("\nAlice Hegan Rice after cleaning:")
        for row in cursor.fetchall():
            print(f"  '{row[0]}': {row[1]} films")
    
    else:
        print("\n⚠️  This was a DRY RUN. To apply changes, run with --apply flag")
    
    conn.close()
    return len(changes)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean author data in the database')
    parser.add_argument('--db', default='data/databases/holreg_research.db',
                        help='Path to database')
    parser.add_argument('--apply', action='store_true',
                        help='Actually apply the changes (default is dry run)')
    
    args = parser.parse_args()
    
    changes = clean_database(args.db, dry_run=not args.apply)
    
    if args.apply and changes > 0:
        print("\n✅ Database cleaned successfully!")
        print("\n📋 NEXT STEPS:")
        print("1. Run: python scripts/db-to-json.py")
        print("2. Run: python scripts/pages.py")
        print("3. Check the Gene Stratton-Porter page!")


if __name__ == "__main__":
    main()