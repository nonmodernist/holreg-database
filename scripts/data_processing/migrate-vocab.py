import sqlite3
import pandas as pd
from pathlib import Path
import json
from datetime import datetime

class ControlledVocabularyMigration:
    def __init__(self, db_path):
        """Initialize connection to your SQLite database"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
    def explore_database(self):
        """First, let's see what tables you have"""
        print("=== Current Tables in Database ===")
        tables = self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
        
        for table in tables:
            print(f"\n📊 Table: {table[0]}")
            # Get column info
            columns = self.cursor.execute(f"PRAGMA table_info({table[0]})").fetchall()
            for col in columns:
                print(f"   - {col[1]} ({col[2]})")
        
        return [t[0] for t in tables]
    
    def create_controlled_vocabulary_tables(self):
        """Create the tables for controlled vocabulary"""
        print("\n=== Creating Controlled Vocabulary Tables ===")
        
        # Main controlled terms table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS controlled_terms (
            term_id INTEGER PRIMARY KEY AUTOINCREMENT,
            term TEXT NOT NULL UNIQUE,
            facet TEXT NOT NULL,
            scope_note TEXT,
            historical_note TEXT,
            modern_equivalent TEXT,
            afi_frequency INTEGER DEFAULT 0,
            created_date DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        print("✅ Created controlled_terms table")
        
        # Term relationships (hierarchical)
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS term_relationships (
            relationship_id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_term_id INTEGER,
            child_term_id INTEGER,
            relationship_type TEXT DEFAULT 'broader/narrower',
            FOREIGN KEY (parent_term_id) REFERENCES controlled_terms(term_id),
            FOREIGN KEY (child_term_id) REFERENCES controlled_terms(term_id)
        )
        """)
        print("✅ Created term_relationships table")
        
        # Film-term mapping table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS film_subjects_controlled (
            film_id INTEGER NOT NULL,
            term_id INTEGER NOT NULL,
            relevance_weight INTEGER DEFAULT 2 CHECK(relevance_weight BETWEEN 1 AND 3),
            assignment_type TEXT DEFAULT 'auto_mapped',
            assigned_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (film_id, term_id),
            FOREIGN KEY (term_id) REFERENCES controlled_terms(term_id)
        )
        """)
        print("✅ Created film_subjects_controlled table")
        
        # Mapping from original AFI subjects
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS afi_to_controlled_mapping (
            mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
            afi_subject TEXT NOT NULL UNIQUE,
            controlled_term_id INTEGER,
            confidence_score REAL DEFAULT 1.0,
            mapping_notes TEXT,
            FOREIGN KEY (controlled_term_id) REFERENCES controlled_terms(term_id)
        )
        """)
        print("✅ Created afi_to_controlled_mapping table")
        
        self.conn.commit()
    
    def analyze_current_subjects(self, table_name='films'):
        """Analyze the current subjects in your database"""
        print(f"\n=== Analyzing Subjects in '{table_name}' Table ===")
        
        # Get all subjects
        query = f"SELECT subjects FROM {table_name} WHERE subjects IS NOT NULL"
        df = pd.read_sql_query(query, self.conn)
        
        # Split and count subjects
        all_subjects = []
        for subjects_str in df['subjects']:
            if subjects_str:
                subjects = [s.strip() for s in subjects_str.split('|') if s.strip()]
                all_subjects.extend(subjects)
        
        # Count frequency
        subject_counts = pd.Series(all_subjects).value_counts()
        
        print(f"Total unique subjects: {len(subject_counts)}")
        print(f"Total subject assignments: {len(all_subjects)}")
        print("\nTop 20 subjects:")
        for subject, count in subject_counts.head(20).items():
            print(f"  {subject}: {count}")
        
        return subject_counts
    
    def populate_controlled_vocabulary(self, subject_counts):
        """Populate the controlled vocabulary with initial terms"""
        print("\n=== Populating Controlled Vocabulary ===")
        
        # Define the controlled vocabulary structure
        vocabulary = {
            'Family Relations': {
                'terms': [
                    ('Mothers and daughters', 'Primary plot relationships between mothers and daughters'),
                    ('Mothers and sons', 'Primary plot relationships between mothers and sons'),
                    ('Fathers and daughters', 'Primary plot relationships between fathers and daughters'),
                    ('Fathers and sons', 'Primary plot relationships between fathers and sons'),
                    ('Motherhood', 'Maternal themes, becoming a mother, maternal identity'),
                    ('Fatherhood', 'Paternal themes, becoming a father, paternal identity'),
                    ('Brothers and sisters', 'Sibling relationships'),
                    ('Sisters', 'Relationships between sisters'),
                    ('Brothers', 'Relationships between brothers'),
                    ('Orphans', 'Characters without living parents, central to plot'),
                    ('Adoption', 'Legal or informal adoption as plot element'),
                    ('Family relationships', 'General family dynamics'),
                    ('Family life', 'Domestic family situations'),
                    ('Aunts', 'Aunt relationships'),
                    ('Uncles', 'Uncle relationships'),
                    ('Cousins', 'Cousin relationships'),
                    ('Grandmothers', 'Grandmother relationships'),
                    ('Grandfathers', 'Grandfather relationships'),
                    ('Stepmothers', 'Stepmother relationships'),
                    ('Stepfathers', 'Stepfather relationships'),
                ]
            },
            'Marital Relations': {
                'terms': [
                    ('Marriage', 'Marriage as institution or event'),
                    ('Engagements', 'Betrothal, engagement period'),
                    ('Weddings', 'Wedding ceremonies'),
                    ('Divorce', 'Marital dissolution'),
                    ('Widows', 'Women whose husbands have died'),
                    ('Widowers', 'Men whose wives have died'),
                    ('Infidelity', 'Marital unfaithfulness'),
                    ('Bigamy', 'Being married to multiple people'),
                    ('Romance', 'Romantic relationships'),
                    ('Courtship', 'Romantic pursuit'),
                ]
            },
            'Professions': {
                'terms': [
                    ('Physicians', 'Medical doctors as significant characters'),
                    ('Lawyers', 'Legal professionals as significant characters'),
                    ('Schoolteachers', 'Teachers as significant characters'),
                    ('Nurses', 'Medical nurses'),
                    ('Ministers', 'Religious clergy'),
                    ('Writers', 'Authors, journalists'),
                    ('Artists', 'Painters, sculptors, etc.'),
                    ('Farmers', 'Agricultural workers'),
                    ('Servants', 'Domestic workers'),
                ]
            },
            'Social Issues': {
                'terms': [
                    ('African Americans', 'African American characters or themes'),
                    ('Racism', 'Racial prejudice as theme'),
                    ('Class distinction', 'Class differences as plot element'),
                    ('Poverty', 'Economic hardship as theme'),
                    ('Slavery', 'Enslavement of people'),
                    ('Prejudice', 'Bias and discrimination'),
                ]
            },
            'Women-Specific': {
                'terms': [
                    ('Spinsters', 'Unmarried women (period term)', 'Single women'),
                    ('Working women', 'Women in workforce'),
                    ('Women in business', 'Female entrepreneurs/business owners'),
                    ('Chorus girls', 'Female stage performers'),
                    ('Childbirth', 'Giving birth, labor'),
                ]
            },
            'Settings': {
                'terms': [
                    ('Small town life', 'Rural or small town settings'),
                    ('New York City', 'NYC as setting'),
                    ('Plantations', 'Southern plantation settings'),
                    ('Farms', 'Agricultural settings'),
                ]
            },
            'Plot Elements': {
                'terms': [
                    ('Murder', 'Homicide as plot element'),
                    ('Trials', 'Legal proceedings'),
                    ('False accusations', 'Wrongful blame'),
                    ('Rescues', 'Rescue scenarios'),
                    ('Fires', 'Fire as plot element'),
                    ('Automobile accidents', 'Car crashes'),
                    ('Kidnapping', 'Abduction'),
                    ('Death and dying', 'Death as theme'),
                    ('Self-sacrifice', 'Sacrificing for others'),
                ]
            }
        }
        
        # Insert terms into database
        term_count = 0
        for facet, facet_data in vocabulary.items():
            for term_data in facet_data['terms']:
                term = term_data[0]
                scope_note = term_data[1] if len(term_data) > 1 else None
                modern_equiv = term_data[2] if len(term_data) > 2 else None
                
                # Get frequency from original data
                frequency = subject_counts.get(term, 0)
                
                try:
                    self.cursor.execute("""
                    INSERT INTO controlled_terms 
                    (term, facet, scope_note, modern_equivalent, afi_frequency)
                    VALUES (?, ?, ?, ?, ?)
                    """, (term, facet, scope_note, modern_equiv, frequency))
                    term_count += 1
                except sqlite3.IntegrityError:
                    print(f"  ⚠️  Term already exists: {term}")
        
        self.conn.commit()
        print(f"✅ Added {term_count} controlled terms")
    
    def create_afi_mappings(self, subject_counts):
        """Create mappings from AFI subjects to controlled terms"""
        print("\n=== Creating AFI to Controlled Term Mappings ===")
        
        # Define mapping rules
        mappings = [
            # Exact matches (confidence = 1.0)
            ('Mothers and daughters', 'Mothers and daughters', 1.0),
            ('Mothers and sons', 'Mothers and sons', 1.0),
            ('Fathers and daughters', 'Fathers and daughters', 1.0),
            ('Orphans', 'Orphans', 1.0),
            ('Marriage', 'Marriage', 1.0),
            ('Physicians', 'Physicians', 1.0),
            ('Lawyers', 'Lawyers', 1.0),
            ('African Americans', 'African Americans', 1.0),
            ('Racism', 'Racism', 1.0),
            ('Murder', 'Murder', 1.0),
            
            # Close matches (confidence = 0.9)
            ('Family honor', 'Family relationships', 0.9),
            ('Mothers-in-law', 'Family relationships', 0.9),
            ('Fathers-in-law', 'Family relationships', 0.9),
            ('Schoolmasters', 'Schoolteachers', 0.9),
            ('Tutors', 'Schoolteachers', 0.9),
            
            # Broader mappings (confidence = 0.8)
            ('Bigamy', 'Marriage', 0.8),
            ('Desertion (Marital)', 'Marriage', 0.8),
            ('Marriage--Arranged', 'Marriage', 0.8),
            ('Marriage--Secret', 'Marriage', 0.8),
            
            # Historical term updates (confidence = 0.95)
            ('Indians of North America', 'Native Americans', 0.95),
            ('Spinsters', 'Spinsters', 1.0),  # Keep but note the modern equivalent
            ('Handicapped', 'Persons with disabilities', 0.95),
        ]
        
        # First, get controlled term IDs
        controlled_terms = {}
        for row in self.cursor.execute("SELECT term_id, term FROM controlled_terms").fetchall():
            controlled_terms[row[1]] = row[0]
        
        # Insert mappings
        mapping_count = 0
        for afi_subject, controlled_term, confidence in mappings:
            if controlled_term in controlled_terms:
                try:
                    self.cursor.execute("""
                    INSERT INTO afi_to_controlled_mapping 
                    (afi_subject, controlled_term_id, confidence_score)
                    VALUES (?, ?, ?)
                    """, (afi_subject, controlled_terms[controlled_term], confidence))
                    mapping_count += 1
                except sqlite3.IntegrityError:
                    pass
        
        self.conn.commit()
        print(f"✅ Created {mapping_count} AFI to controlled term mappings")
    
    def generate_mapping_report(self):
        """Generate a report of unmapped AFI subjects"""
        print("\n=== Unmapped AFI Subjects Report ===")
        
        # Get all unique AFI subjects from the films table
        all_afi_subjects = set()
        for row in self.cursor.execute("SELECT subjects FROM films WHERE subjects IS NOT NULL"):
            subjects = [s.strip() for s in row[0].split('|') if s.strip()]
            all_afi_subjects.update(subjects)
        
        # Get mapped subjects
        mapped_subjects = set()
        for row in self.cursor.execute("SELECT afi_subject FROM afi_to_controlled_mapping"):
            mapped_subjects.add(row[0])
        
        # Find unmapped
        unmapped = all_afi_subjects - mapped_subjects
        
        print(f"Total AFI subjects: {len(all_afi_subjects)}")
        print(f"Mapped subjects: {len(mapped_subjects)}")
        print(f"Unmapped subjects: {len(unmapped)}")
        
        # Save unmapped to file for review
        with open('unmapped_subjects.txt', 'w') as f:
            f.write("UNMAPPED AFI SUBJECTS\n")
            f.write("=" * 50 + "\n\n")
            for subject in sorted(unmapped):
                f.write(f"{subject}\n")
        
        print("\n📄 Saved unmapped subjects to 'unmapped_subjects.txt' for review")
        
        return unmapped
    
    def close(self):
        """Close database connection"""
        self.conn.close()
        print("\n✅ Database connection closed")

# USAGE INSTRUCTIONS
if __name__ == "__main__":
    print("""
    🎬 CONTROLLED VOCABULARY MIGRATION TOOL 🎬
    
    This script will:
    1. Explore your current database
    2. Create controlled vocabulary tables
    3. Populate with standardized terms
    4. Create mappings from AFI subjects
    5. Generate a report of unmapped terms
    
    """)
    
    # UPDATE THIS PATH to your database file
    db_path = "film_research.db"  # <-- CHANGE THIS!
    
    # Create migration object
    migration = ControlledVocabularyMigration(db_path)
    
    try:
        # Step 1: Explore
        tables = migration.explore_database()
        
        # Step 2: Create tables
        migration.create_controlled_vocabulary_tables()
        
        # Step 3: Analyze current subjects (update table name if needed)
        subject_counts = migration.analyze_current_subjects('films')
        
        # Step 4: Populate controlled vocabulary
        migration.populate_controlled_vocabulary(subject_counts)
        
        # Step 5: Create mappings
        migration.create_afi_mappings(subject_counts)
        
        # Step 6: Generate report
        migration.generate_mapping_report()
        
    finally:
        migration.close()
    
    print("\n🎉 Migration complete! Next steps:")
    print("1. Review 'unmapped_subjects.txt'")
    print("2. Add more mappings as needed")
    print("3. Start tagging films with controlled terms")