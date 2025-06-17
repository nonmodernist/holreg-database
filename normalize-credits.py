#!/usr/bin/env python3
"""
Normalize database to properly handle multiple people per role
Creates junction tables for many-to-many relationships
"""

import sqlite3
import re
from typing import List, Tuple

def normalize_multiple_people(db_path='data/databases/holreg_research.db'):
    """Create normalized structure for handling multiple people per role"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("Creating normalized tables for people...")
    
    # 1. Create a unified 'people' table for all individuals
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS people (
            person_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            name_normalized TEXT, -- For searching/matching
            afi_id TEXT, -- If you want to preserve AFI IDs
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 2. Create junction tables for each role
    roles = ['directors', 'writers', 'producers', 'cast_members']
    
    for role in roles:
        table_name = f"film_{role}"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                film_id INTEGER,
                person_id INTEGER,
                position INTEGER DEFAULT 1, -- Order of billing/credit
                role_note TEXT, -- For additional info like "uncredited"
                FOREIGN KEY (film_id) REFERENCES films (id),
                FOREIGN KEY (person_id) REFERENCES people (person_id),
                PRIMARY KEY (film_id, person_id, position)
            )
        """)
    
    # 3. Create indexes for performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_people_name ON people(name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_people_normalized ON people(name_normalized)")
    
    print("Migrating existing data...")
    
    # 4. Helper function to parse names
    def parse_names(field_value: str) -> List[Tuple[str, str]]:
        """Parse a field containing multiple names
        Returns list of (name, afi_id) tuples"""
        if not field_value:
            return []
        
        results = []
        
        # Split by common delimiters
        # Handle patterns like "Name1|Name2" or "Name1||Name2"
        parts = re.split(r'\s*\|\s*', field_value)
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
                
            # Check if this part contains an AFI ID (all digits)
            if part.isdigit():
                # This is an AFI ID for the previous name
                if results and results[-1][1] is None:
                    results[-1] = (results[-1][0], part)
            else:
                # This is a name
                # Check if name has embedded ID like "John Doe|12345"
                name_match = re.match(r'^(.+?)\|(\d+)$', part)
                if name_match:
                    results.append((name_match.group(1).strip(), name_match.group(2)))
                else:
                    results.append((part, None))
        
        return results
    
    # 5. Helper function to normalize names for matching
    def normalize_name(name: str) -> str:
        """Normalize name for better matching"""
        # Remove common variations
        name = name.lower()
        name = re.sub(r'\s+', ' ', name)  # Multiple spaces to single
        name = re.sub(r'[^\w\s]', '', name)  # Remove punctuation
        return name.strip()
    
    # 6. Process each film and populate people table
    cursor.execute("SELECT id, director, writer, producer FROM films")
    films = cursor.fetchall()
    
    people_added = 0
    
    for film_id, director, writer, producer in films:
        # Process directors
        if director:
            directors = parse_names(director)
            for position, (name, afi_id) in enumerate(directors, 1):
                # Add person if not exists
                norm_name = normalize_name(name)
                cursor.execute("""
                    INSERT OR IGNORE INTO people (name, name_normalized, afi_id) 
                    VALUES (?, ?, ?)
                """, (name, norm_name, afi_id))
                
                # Get person_id
                cursor.execute("SELECT person_id FROM people WHERE name = ?", (name,))
                person_id = cursor.fetchone()[0]
                
                # Link to film
                cursor.execute("""
                    INSERT OR IGNORE INTO film_directors (film_id, person_id, position)
                    VALUES (?, ?, ?)
                """, (film_id, person_id, position))
        
        # Process writers (similar logic)
        if writer:
            writers = parse_names(writer)
            for position, (name, afi_id) in enumerate(writers, 1):
                norm_name = normalize_name(name)
                cursor.execute("""
                    INSERT OR IGNORE INTO people (name, name_normalized, afi_id) 
                    VALUES (?, ?, ?)
                """, (name, norm_name, afi_id))
                
                cursor.execute("SELECT person_id FROM people WHERE name = ?", (name,))
                person_id = cursor.fetchone()[0]
                
                cursor.execute("""
                    INSERT OR IGNORE INTO film_writers (film_id, person_id, position)
                    VALUES (?, ?, ?)
                """, (film_id, person_id, position))
        
        # Process producers (similar logic)
        if producer:
            producers = parse_names(producer)
            for position, (name, afi_id) in enumerate(producers, 1):
                norm_name = normalize_name(name)
                cursor.execute("""
                    INSERT OR IGNORE INTO people (name, name_normalized, afi_id) 
                    VALUES (?, ?, ?)
                """, (name, norm_name, afi_id))
                
                cursor.execute("SELECT person_id FROM people WHERE name = ?", (name,))
                person_id = cursor.fetchone()[0]
                
                cursor.execute("""
                    INSERT OR IGNORE INTO film_producers (film_id, person_id, position)
                    VALUES (?, ?, ?)
                """, (film_id, person_id, position))
    
    # 7. Create helpful views for easy querying
    print("Creating views for easy access...")
    
    # View for directors
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_film_directors AS
        SELECT 
            f.id as film_id,
            f.title,
            f.release_year,
            GROUP_CONCAT(p.name, ' | ') as directors,
            COUNT(p.person_id) as director_count
        FROM films f
        LEFT JOIN film_directors fd ON f.id = fd.film_id
        LEFT JOIN people p ON fd.person_id = p.person_id
        GROUP BY f.id
        ORDER BY f.release_year, f.title
    """)
    
    # View for complete credits
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_film_credits AS
        SELECT 
            f.id,
            f.title,
            f.release_year,
            f.literary_credits,
            (SELECT GROUP_CONCAT(p.name, ' | ') 
             FROM film_directors fd 
             JOIN people p ON fd.person_id = p.person_id 
             WHERE fd.film_id = f.id 
             ORDER BY fd.position) as directors,
            (SELECT GROUP_CONCAT(p.name, ' | ') 
             FROM film_writers fw 
             JOIN people p ON fw.person_id = p.person_id 
             WHERE fw.film_id = f.id 
             ORDER BY fw.position) as writers,
            (SELECT GROUP_CONCAT(p.name, ' | ') 
             FROM film_producers fp 
             JOIN people p ON fp.person_id = p.person_id 
             WHERE fp.film_id = f.id 
             ORDER BY fp.position) as producers
        FROM films f
    """)
    
    # View for person filmography
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_person_filmography AS
        SELECT 
            p.person_id,
            p.name,
            'Director' as role,
            f.title,
            f.release_year,
            fd.position
        FROM people p
        JOIN film_directors fd ON p.person_id = fd.person_id
        JOIN films f ON fd.film_id = f.id
        
        UNION ALL
        
        SELECT 
            p.person_id,
            p.name,
            'Writer' as role,
            f.title,
            f.release_year,
            fw.position
        FROM people p
        JOIN film_writers fw ON p.person_id = fw.person_id
        JOIN films f ON fw.film_id = f.id
        
        UNION ALL
        
        SELECT 
            p.person_id,
            p.name,
            'Producer' as role,
            f.title,
            f.release_year,
            fp.position
        FROM people p
        JOIN film_producers fp ON p.person_id = fp.person_id
        JOIN films f ON fp.film_id = f.id
        
        ORDER BY person_id, release_year
    """)
    
    # 8. Add some useful queries as prepared statements
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_multi_director_films AS
        SELECT 
            f.id,
            f.title,
            f.release_year,
            COUNT(fd.person_id) as director_count,
            GROUP_CONCAT(p.name, ' & ') as directors
        FROM films f
        JOIN film_directors fd ON f.id = fd.film_id
        JOIN people p ON fd.person_id = p.person_id
        GROUP BY f.id
        HAVING director_count > 1
        ORDER BY director_count DESC, f.release_year
    """)
    
    conn.commit()
    
    # Print statistics
    cursor.execute("SELECT COUNT(*) FROM people")
    total_people = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM v_multi_director_films")
    multi_director_films = cursor.fetchone()[0]
    
    print(f"\n✅ Normalization complete!")
    print(f"Statistics:")
    print(f"  - Total unique people: {total_people}")
    print(f"  - Films with multiple directors: {multi_director_films}")
    
    # Show some examples
    print("\nExample: Films with multiple directors:")
    cursor.execute("SELECT title, release_year, directors FROM v_multi_director_films LIMIT 5")
    for row in cursor.fetchall():
        print(f"  - {row[0]} ({row[1]}): {row[2]}")
    
    conn.close()


def query_examples(db_path='data/databases/holreg_research.db'):
    """Show example queries using the normalized structure"""
    
    print("\n" + "="*50)
    print("EXAMPLE QUERIES WITH NORMALIZED DATA")
    print("="*50)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Example 1: Find all films by a specific director
    print("\n1. All films directed by James Leo Meehan:")
    cursor.execute("""
        SELECT f.title, f.release_year
        FROM films f
        JOIN film_directors fd ON f.id = fd.film_id
        JOIN people p ON fd.person_id = p.person_id
        WHERE p.name = 'James Leo Meehan'
        ORDER BY f.release_year
    """)
    for row in cursor.fetchall():
        print(f"   - {row[0]} ({row[1]})")
    
    # Example 2: Find people who worked in multiple roles
    print("\n2. People who worked in multiple roles:")
    cursor.execute("""
        SELECT 
            p.name,
            GROUP_CONCAT(DISTINCT role) as roles,
            COUNT(DISTINCT f.id) as film_count
        FROM v_person_filmography f
        JOIN people p ON f.person_id = p.person_id
        GROUP BY p.person_id
        HAVING COUNT(DISTINCT role) > 1
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"   - {row[0]}: {row[1]} ({row[2]} films)")
    
    # Example 3: Most frequent collaborations
    print("\n3. Most frequent director-writer collaborations:")
    cursor.execute("""
        SELECT 
            d.name as director,
            w.name as writer,
            COUNT(*) as collaborations
        FROM film_directors fd
        JOIN film_writers fw ON fd.film_id = fw.film_id
        JOIN people d ON fd.person_id = d.person_id
        JOIN people w ON fw.person_id = w.person_id
        WHERE d.person_id != w.person_id
        GROUP BY d.person_id, w.person_id
        HAVING collaborations > 1
        ORDER BY collaborations DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"   - {row[0]} & {row[1]}: {row[2]} films")
    
    conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Normalize database for multiple people per role')
    parser.add_argument('--db', default='data/databases/holreg_research.db',
                        help='Path to database file')
    parser.add_argument('--examples', action='store_true',
                        help='Show example queries after normalization')
    
    args = parser.parse_args()
    
    normalize_multiple_people(args.db)
    
    if args.examples:
        query_examples(args.db)