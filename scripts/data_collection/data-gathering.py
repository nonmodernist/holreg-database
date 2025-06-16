import sqlite3
import csv
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class SourceDataGatherer:
    def __init__(self, db_path: str = "data/databases/holreg_research.db"):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # Common patterns for parsing literary credits
        self.source_patterns = {
            'novel': r'novel|book',
            'short story': r'short story|story',
            'play': r'play|drama|theatrical',
            'poem': r'poem|verse',
            'essay': r'essay',
            'memoir': r'memoir|autobiography'
        }
        
    def extract_authors_from_credits(self):
        """Extract unique authors from literary_credits field"""
        self.cursor.execute("""
            SELECT DISTINCT literary_credits 
            FROM films 
            WHERE literary_credits IS NOT NULL
        """)
        
        all_authors = set()
        for (credits,) in self.cursor.fetchall():
            # Split by | and clean each author name
            authors = [a.strip() for a in credits.split('|') if a.strip()]
            all_authors.update(authors)
        
        print(f"Found {len(all_authors)} unique author names")
        return sorted(all_authors)
    
    def create_author_lookup_csv(self):
        """Create CSV for manual author data entry"""
        authors = self.extract_authors_from_credits()
        
        with open('author_lookup_sheet.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['author_name', 'birth_year', 'death_year', 'notes'])
            
            for author in authors:
                writer.writerow([author, '', '', ''])
        
        print("Created author_lookup_sheet.csv for manual completion")
    
    def create_source_lookup_csv(self):
        """Create CSV for manual source text research"""
        self.cursor.execute("""
            SELECT id, title, release_year, literary_credits 
            FROM films 
            WHERE literary_credits IS NOT NULL 
            ORDER BY literary_credits, release_year
        """)
        
        with open('source_text_lookup.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'film_id', 'film_title', 'film_year', 'literary_credits',
                'source_title', 'source_type', 'source_year', 'source_publisher', 
                'adaptation_notes'
            ])
            
            for row in self.cursor.fetchall():
                writer.writerow(list(row) + ['', '', '', '', ''])
        
        print("Created source_text_lookup.csv for manual completion")
    
    def create_survival_status_csv(self):
        """Create CSV for tracking survival status"""
        self.cursor.execute("""
            SELECT 
                f.id, 
                f.title, 
                f.release_year,
                GROUP_CONCAT(
                    CASE 
                        WHEN pc.company_type = 'production' 
                        THEN pc.company_name 
                    END, ' | '
                ) as production_companies
            FROM films f
            LEFT JOIN production_companies pc ON f.id = pc.film_id
            GROUP BY f.id, f.title, f.release_year
            ORDER BY f.release_year, f.title
        """)
        
        with open('survival_status_lookup.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'film_id', 'film_title', 'film_year', 'production_companies',
                'survival_status', 'archive_holdings', 'viewing_format', 
                'survival_notes', 'last_verified'
            ])
            
            for row in self.cursor.fetchall():
                # Pre-fill some educated guesses based on year
                film_id, title, year, companies = row
                
                # Default survival status based on era
                if year and year < 1930:
                    default_status = 'unknown'  # Many silent films are lost
                else:
                    default_status = 'likely extant'
                
                writer.writerow([film_id, title, year, companies or '', 
                               default_status, '', '', '', ''])
        
        print("Created survival_status_lookup.csv for research")
    
    def import_author_data(self, csv_path: str):
        """Import completed author data from CSV"""
        imported = 0
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                if row['author_name'].strip():
                    try:
                        self.cursor.execute("""
                            INSERT OR REPLACE INTO authors 
                            (author_name, birth_year, death_year, author_notes)
                            VALUES (?, ?, ?, ?)
                        """, (
                            row['author_name'].strip(),
                            int(row['birth_year']) if row['birth_year'] else None,
                            int(row['death_year']) if row['death_year'] else None,
                            row['notes']
                        ))
                        imported += 1
                    except Exception as e:
                        print(f"Error importing {row['author_name']}: {e}")
        
        self.conn.commit()
        print(f"Imported {imported} author records")
    
    def import_source_data(self, csv_path: str):
        """Import completed source text data from CSV"""
        imported = 0
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                if row['source_title'].strip():
                    # First, get or create the author
                    author_name = row['literary_credits'].split('|')[0].strip()
                    self.cursor.execute(
                        "SELECT author_id FROM authors WHERE author_name = ?",
                        (author_name,)
                    )
                    result = self.cursor.fetchone()
                    
                    if result:
                        author_id = result[0]
                    else:
                        # Create author if not exists
                        self.cursor.execute(
                            "INSERT INTO authors (author_name) VALUES (?)",
                            (author_name,)
                        )
                        author_id = self.cursor.lastrowid
                    
                    # Insert source text
                    self.cursor.execute("""
                        INSERT OR IGNORE INTO source_texts 
                        (title, author_id, publication_year, source_type, publisher, notes)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        row['source_title'],
                        author_id,
                        int(row['source_year']) if row['source_year'] else None,
                        row['source_type'],
                        row['source_publisher'],
                        row['adaptation_notes']
                    ))
                    
                    source_id = self.cursor.lastrowid
                    
                    # Link film to source
                    self.cursor.execute("""
                        INSERT OR IGNORE INTO film_sources 
                        (film_id, source_id, adaptation_notes)
                        VALUES (?, ?, ?)
                    """, (
                        int(row['film_id']),
                        source_id,
                        row['adaptation_notes']
                    ))
                    
                    imported += 1
        
        self.conn.commit()
        print(f"Imported {imported} source text records")
    
    def import_survival_data(self, csv_path: str):
        """Import survival status data from CSV"""
        imported = 0
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                if row['survival_status']:
                    self.cursor.execute("""
                        UPDATE films 
                        SET survival_status = ?,
                            archive_holdings = ?,
                            viewing_format = ?,
                            survival_notes = ?,
                            last_verified = ?
                        WHERE id = ?
                    """, (
                        row['survival_status'],
                        row['archive_holdings'],
                        row['viewing_format'],
                        row['survival_notes'],
                        row['last_verified'] if row['last_verified'] else datetime.now().date(),
                        int(row['film_id'])
                    ))
                    imported += 1
        
        self.conn.commit()
        print(f"Updated survival status for {imported} films")
    
    def generate_research_report(self):
        """Generate a report of data completeness"""
        print("\n=== DATA COMPLETENESS REPORT ===")
        
        # Films with/without source data
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_films,
                SUM(CASE WHEN source_title IS NOT NULL THEN 1 ELSE 0 END) as with_source,
                SUM(CASE WHEN survival_status IS NOT NULL THEN 1 ELSE 0 END) as with_survival
            FROM films
        """)
        
        total, with_source, with_survival = self.cursor.fetchone()
        
        print(f"\nTotal films: {total}")
        print(f"Films with source data: {with_source} ({with_source/total*100:.1f}%)")
        print(f"Films with survival status: {with_survival} ({with_survival/total*100:.1f}%)")
        
        # Survival status breakdown
        print("\nSurvival Status Breakdown:")
        self.cursor.execute("""
            SELECT survival_status, COUNT(*) as count
            FROM films
            WHERE survival_status IS NOT NULL
            GROUP BY survival_status
            ORDER BY count DESC
        """)
        
        for status, count in self.cursor.fetchall():
            print(f"  {status}: {count}")
        
        # Source type breakdown
        print("\nSource Type Breakdown:")
        self.cursor.execute("""
            SELECT source_type, COUNT(*) as count
            FROM films
            WHERE source_type IS NOT NULL
            GROUP BY source_type
            ORDER BY count DESC
        """)
        
        for source_type, count in self.cursor.fetchall():
            print(f"  {source_type}: {count}")
    
    def analyze_production_companies(self):
        """Analyze production companies to help with survival research"""
        print("\n=== PRODUCTION COMPANY ANALYSIS ===")
        
        # Major studios by era
        major_studios = {
            'silent_era': ['Biograph', 'Vitagraph', 'Thanhouser', 'Famous Players'],
            'studio_system': ['MGM', 'Paramount', 'Warner Bros', 'RKO', 'Fox', 
                            'Universal', 'Columbia'],
            'independent': ['Monogram', 'Republic', 'Selznick']
        }
        
        # Get production company statistics
        self.cursor.execute("""
            SELECT 
                pc.company_name,
                COUNT(DISTINCT f.id) as film_count,
                MIN(f.release_year) as earliest,
                MAX(f.release_year) as latest,
                GROUP_CONCAT(DISTINCT 
                    CASE 
                        WHEN f.release_year < 1930 THEN 'silent'
                        WHEN f.release_year < 1950 THEN 'classical'
                        ELSE 'modern'
                    END
                ) as eras
            FROM production_companies pc
            JOIN films f ON pc.film_id = f.id
            WHERE pc.company_type = 'production'
            GROUP BY pc.company_name
            HAVING film_count > 1
            ORDER BY film_count DESC
        """)
        
        print("\nMost prolific production companies:")
        for company, count, earliest, latest, eras in self.cursor.fetchall()[:20]:
            print(f"  {company}: {count} films ({earliest}-{latest}) [{eras}]")
        
        # Survival likelihood by company
        print("\n\nSurvival likelihood notes:")
        print("  Major studios (MGM, Paramount, etc.): HIGH - corporate archives")
        print("  Independent studios: MEDIUM - varies by company")
        print("  Silent era companies: LOW - many defunct before preservation")
        print("  Monogram/Republic: MEDIUM-HIGH - TV syndication saved many")
    
    def generate_studio_research_batches(self):
        """Group films by studio for efficient archive research"""
        print("\n=== STUDIO-BASED RESEARCH BATCHES ===")
        
        # Define studio-to-archive mappings
        studio_archives = {
            'Universal': ['Universal Studios Archives', 'UCLA'],
            'Paramount': ['UCLA Film & Television Archive'],
            'MGM': ['George Eastman Museum', 'Academy Archive'],
            'Warner Bros': ['USC Warner Bros. Archives', 'Wisconsin Center for Film'],
            'RKO': ['UCLA', 'Library of Congress'],
            'Fox': ['Academy Film Archive', 'UCLA'],
            'Columbia': ['Academy Film Archive'],
            'Selznick': ['Harry Ransom Center', 'George Eastman Museum'],
            'Monogram': ['Library of Congress', 'various private collections'],
            'Republic': ['UCLA', 'Library of Congress']
        }
        
        # Create research batches
        with open('studio_research_batches.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['studio', 'suggested_archives', 'film_count', 'films'])
            
            for studio, archives in studio_archives.items():
                self.cursor.execute("""
                    SELECT 
                        f.id,
                        f.title || ' (' || f.release_year || ')' as film_info
                    FROM films f
                    JOIN production_companies pc ON f.id = pc.film_id
                    WHERE pc.company_type = 'production'
                      AND pc.company_name LIKE ?
                      AND f.survival_status IS NULL
                    ORDER BY f.release_year
                """, (f'%{studio}%',))
                
                films = self.cursor.fetchall()
                if films:
                    film_list = '; '.join([f[1] for f in films])
                    writer.writerow([
                        studio,
                        ' / '.join(archives),
                        len(films),
                        film_list
                    ])
        
        print("Created studio_research_batches.csv for targeted archive research")
    
    def close(self):
        self.conn.close()


# Research workflow
if __name__ == "__main__":
    gatherer = SourceDataGatherer()
    
    # Step 1: Generate lookup CSVs
    print("=== STEP 1: Generating lookup CSVs ===")
    gatherer.create_author_lookup_csv()
    gatherer.create_source_lookup_csv()
    gatherer.create_survival_status_csv()
    
    # Step 2: Analyze production companies
    gatherer.analyze_production_companies()
    
    # Step 3: Generate studio research batches
    gatherer.generate_studio_research_batches()
    
    print("\n📋 Next steps:")
    print("1. Fill out author_lookup_sheet.csv with birth/death years")
    print("2. Research and fill out source_text_lookup.csv")
    print("3. Check archives and fill out survival_status_lookup.csv")
    print("4. Use studio_research_batches.csv to contact archives efficiently")
    print("5. Run this script again with --import flag to import the data")
    
    # Uncomment these when ready to import:
    # gatherer.import_author_data('author_lookup_sheet_completed.csv')
    # gatherer.import_source_data('source_text_lookup_completed.csv')
    # gatherer.import_survival_data('survival_status_lookup_completed.csv')
    # gatherer.generate_research_report()
    
    gatherer.close()