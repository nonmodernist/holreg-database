#!/usr/bin/env python3
"""
Database cleanup script for Hollywood Adaptations project
Cleans AFI IDs, standardizes co-author formatting, etc.
"""

import sqlite3
import re

def clean_database(db_path='data/databases/holreg_research.db'):
    """Clean up common data issues in the database"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # First, let's see what we're dealing with
    print("Analyzing data issues...")
    
    # Check for AFI IDs in names
    cursor.execute("""
        SELECT COUNT(*) FROM films 
        WHERE director LIKE '%|%' 
        OR writer LIKE '%|%' 
        OR producer LIKE '%|%'
    """)
    print(f"Films with pipe characters in crew names: {cursor.fetchone()[0]}")
    
    # Check for inconsistent co-author formatting
    cursor.execute("""
        SELECT COUNT(*) FROM films 
        WHERE literary_credits LIKE '%|%'
    """)
    print(f"Films with multiple authors: {cursor.fetchone()[0]}")
    
    # Clean up crew names (remove AFI IDs)
    print("\nCleaning crew names...")
    
    # Function to clean AFI IDs from names
    def clean_afi_ids(text):
        if not text:
            return text
        # Remove patterns like |12345 or ||12345
        text = re.sub(r'\|\|?\d+', '', text)
        # Clean up any remaining double pipes
        text = text.replace('||', '|')
        # Strip trailing/leading pipes
        text = text.strip('|')
        return text.strip()
    
    # Update director names
    cursor.execute("SELECT id, director FROM films WHERE director LIKE '%|%'")
    directors_to_fix = cursor.fetchall()
    
    for film_id, director in directors_to_fix:
        if director and '|' in director:
            # Check if it's multiple directors or just AFI ID
            parts = director.split('|')
            cleaned_parts = []
            
            for part in parts:
                part = part.strip()
                # If it's all digits, skip it (AFI ID)
                if part and not part.isdigit():
                    cleaned_parts.append(part)
            
            cleaned_director = ' | '.join(cleaned_parts)  # Standardize separator
            
            cursor.execute("UPDATE films SET director = ? WHERE id = ?", 
                         (cleaned_director, film_id))
    
    print(f"  Fixed {len(directors_to_fix)} director entries")
    
    # Standardize co-author formatting
    print("\nStandardizing co-author formatting...")
    
    cursor.execute("SELECT id, literary_credits FROM films WHERE literary_credits LIKE '%|%'")
    authors_to_fix = cursor.fetchall()
    
    for film_id, authors in authors_to_fix:
        if authors:
            # Split and clean each author
            author_list = re.split(r'\s*\|\s*', authors)
            cleaned_authors = ' | '.join([a.strip() for a in author_list if a.strip()])
            
            cursor.execute("UPDATE films SET literary_credits = ? WHERE id = ?", 
                         (cleaned_authors, film_id))
    
    print(f"  Fixed {len(authors_to_fix)} author entries")
    
    # Clean up trailing spaces in all text fields
    print("\nCleaning trailing spaces...")
    
    text_columns = ['title', 'director', 'writer', 'producer', 'literary_credits', 
                   'genre', 'subjects', 'filming_location']
    
    for column in text_columns:
        cursor.execute(f"""
            UPDATE films 
            SET {column} = TRIM({column}) 
            WHERE {column} IS NOT NULL AND {column} != TRIM({column})
        """)
        
    # Handle the specific case we saw in the data
    cursor.execute("""
        UPDATE films 
        SET director = 'Louis King'
        WHERE director = 'Louis King|101085'
    """)
    
    # Create a normalized authors table for better handling
    print("\nCreating normalized authors table...")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS authors (
            author_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS film_authors (
            film_id INTEGER,
            author_id INTEGER,
            author_order INTEGER DEFAULT 1,
            FOREIGN KEY (film_id) REFERENCES films (id),
            FOREIGN KEY (author_id) REFERENCES authors (author_id),
            PRIMARY KEY (film_id, author_id)
        )
    """)
    
    # Populate authors table
    cursor.execute("SELECT DISTINCT literary_credits FROM films WHERE literary_credits IS NOT NULL")
    all_credits = cursor.fetchall()
    
    for credit_row in all_credits:
        credits = credit_row[0]
        if credits:
            # Handle both single and multiple authors
            if '|' in credits:
                authors = [a.strip() for a in credits.split('|')]
            else:
                authors = [credits.strip()]
            
            for author in authors:
                if author:
                    cursor.execute("INSERT OR IGNORE INTO authors (name) VALUES (?)", (author,))
    
    print(f"  Added {cursor.rowcount} unique authors")
    
    # Link films to authors
    cursor.execute("SELECT id, literary_credits FROM films WHERE literary_credits IS NOT NULL")
    films_with_authors = cursor.fetchall()
    
    for film_id, credits in films_with_authors:
        if credits:
            if '|' in credits:
                authors = [a.strip() for a in credits.split('|')]
            else:
                authors = [credits.strip()]
            
            for idx, author in enumerate(authors, 1):
                if author:
                    cursor.execute("SELECT author_id FROM authors WHERE name = ?", (author,))
                    author_id = cursor.fetchone()
                    if author_id:
                        cursor.execute("""
                            INSERT OR IGNORE INTO film_authors (film_id, author_id, author_order) 
                            VALUES (?, ?, ?)
                        """, (film_id, author_id[0], idx))
    
    # Add some useful views
    print("\nCreating helpful views...")
    
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_films_with_authors AS
        SELECT 
            f.*,
            GROUP_CONCAT(a.name, ' | ') as authors_normalized
        FROM films f
        LEFT JOIN film_authors fa ON f.id = fa.film_id
        LEFT JOIN authors a ON fa.author_id = a.author_id
        GROUP BY f.id
    """)
    
    conn.commit()
    conn.close()
    
    print("\n✅ Database cleanup complete!")
    
    # Show some statistics
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(DISTINCT name) FROM authors")
    unique_authors = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM film_authors")
    total_credits = cursor.fetchone()[0]
    
    print(f"\nDatabase now contains:")
    print(f"  - {unique_authors} unique authors")
    print(f"  - {total_credits} film-author relationships")
    
    conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean up Hollywood Adaptations database')
    parser.add_argument('--db', default='data/databases/holreg_research.db',
                        help='Path to database file')
    
    args = parser.parse_args()
    clean_database(args.db)