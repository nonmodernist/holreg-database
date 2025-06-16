import sqlite3
import re
from collections import defaultdict

class ExpandedMappingCreator:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # Get existing controlled terms
        self.controlled_terms = {}
        for row in self.cursor.execute("SELECT term_id, term FROM controlled_terms").fetchall():
            self.controlled_terms[row[1]] = row[0]
    
    def create_expanded_mappings(self):
        """Create comprehensive mappings based on patterns"""
        
        # Read unmapped subjects
        with open('unmapped_subjects.txt', 'r') as f:
            unmapped = [line.strip() for line in f if line.strip() and not line.startswith('=')]
        
        print(f"Processing {len(unmapped)} unmapped subjects...")
        
        # Define mapping strategies
        mappings = []
        
        # Strategy 1: Direct term matches (just different case or slight variations)
        direct_mappings = {
            'Children': 'Children',
            'Mothers': 'Motherhood',
            'Fathers': 'Fatherhood',
            'Brothers': 'Brothers',
            'Sisters': 'Sisters',
            'Weddings': 'Weddings',
            'Nurses': 'Nurses',
            'Ministers': 'Ministers',
            'Artists': 'Artists',
            'Farmers': 'Farmers',
            'Servants': 'Servants',
            'Trials': 'Trials',
            'Kidnapping': 'Kidnapping',
            'Death and dying': 'Death and dying',
            'Childbirth': 'Childbirth',
            'Divorce': 'Divorce',
            'Courtship': 'Courtship',
        }
        
        # Strategy 2: Pattern-based mappings
        pattern_mappings = [
            # Family patterns
            (r'.*\b(mother|maternal)\b.*', 'Motherhood', 0.8),
            (r'.*\b(father|paternal)\b.*', 'Fatherhood', 0.8),
            (r'.*\bbrother.*sister\b.*', 'Brothers and sisters', 0.9),
            (r'.*\b(grandparent|grandmother|grandfather)\b.*', 'Family relationships', 0.8),
            (r'.*\b(aunt|uncle|cousin|nephew|niece)\b.*', 'Family relationships', 0.8),
            (r'.*\bfamily\b.*', 'Family relationships', 0.8),
            
            # Marriage patterns
            (r'.*\b(wedding|marriage|marital|matrimony)\b.*', 'Marriage', 0.8),
            (r'.*\b(engagement|betrothal|fiancé)\b.*', 'Engagements', 0.8),
            (r'.*\b(widow|widower)\b.*', 'Widows', 0.8),
            (r'.*\b(divorce|separation)\b.*', 'Divorce', 0.8),
            
            # Profession patterns
            (r'.*\b(doctor|physician|medical)\b.*', 'Physicians', 0.8),
            (r'.*\b(lawyer|attorney|legal)\b.*', 'Lawyers', 0.8),
            (r'.*\b(teacher|professor|tutor)\b.*', 'Schoolteachers', 0.8),
            (r'.*\b(nurse|nursing)\b.*', 'Nurses', 0.8),
            (r'.*\b(minister|priest|clergy|reverend)\b.*', 'Ministers', 0.8),
            (r'.*\b(writer|author|journalist)\b.*', 'Writers', 0.8),
            
            # Social issues patterns
            (r'.*\b(racism|racial|prejudice)\b.*', 'Racism', 0.9),
            (r'.*\b(poverty|poor|destitute)\b.*', 'Poverty', 0.9),
            (r'.*\b(class|social status)\b.*', 'Class distinction', 0.8),
            (r'.*\bslave.*', 'Slavery', 0.9),
            
            # Crime/legal patterns
            (r'.*\b(murder|homicide|killing)\b.*', 'Murder', 0.9),
            (r'.*\b(trial|court|judge)\b.*', 'Trials', 0.8),
            (r'.*\b(accusation|accused|blame)\b.*', 'False accusations', 0.8),
            (r'.*\b(kidnap|abduct)\b.*', 'Kidnapping', 0.9),
            
            # Death/dying patterns
            (r'.*\b(death|dying|dead|funeral)\b.*', 'Death and dying', 0.8),
            (r'.*\b(suicide)\b.*', 'Death and dying', 0.7),
            
            # Women-specific patterns
            (r'.*\b(spinster|old maid)\b.*', 'Spinsters', 0.9),
            (r'.*\b(working women|career women)\b.*', 'Working women', 0.9),
            (r'.*\b(chorus girl|dance.*girl|cigarette girl)\b.*', 'Chorus girls', 0.8),
        ]
        
        # Process each unmapped subject
        for subject in unmapped:
            subject_lower = subject.lower()
            
            # Check direct mappings first
            if subject in direct_mappings and direct_mappings[subject] in self.controlled_terms:
                mappings.append((subject, direct_mappings[subject], 1.0))
                continue
            
            # Check pattern mappings
            mapped = False
            for pattern, controlled_term, confidence in pattern_mappings:
                if re.match(pattern, subject_lower) and controlled_term in self.controlled_terms:
                    mappings.append((subject, controlled_term, confidence))
                    mapped = True
                    break
            
            # Special cases for compound subjects
            if not mapped and '--' in subject:
                # Handle subcategories like "Marriage--Secret"
                base_term = subject.split('--')[0]
                if base_term in self.controlled_terms:
                    mappings.append((subject, base_term, 0.9))
                elif base_term in direct_mappings and direct_mappings[base_term] in self.controlled_terms:
                    mappings.append((subject, direct_mappings[base_term], 0.85))
        
        return mappings
    
    def insert_mappings(self, mappings):
        """Insert the new mappings into the database"""
        inserted_count = 0
        skipped_count = 0
        
        for afi_subject, controlled_term, confidence in mappings:
            if controlled_term in self.controlled_terms:
                try:
                    self.cursor.execute("""
                    INSERT INTO afi_to_controlled_mapping 
                    (afi_subject, controlled_term_id, confidence_score, mapping_notes)
                    VALUES (?, ?, ?, ?)
                    """, (
                        afi_subject, 
                        self.controlled_terms[controlled_term], 
                        confidence,
                        f"Auto-mapped with confidence {confidence}"
                    ))
                    inserted_count += 1
                except sqlite3.IntegrityError:
                    skipped_count += 1
        
        self.conn.commit()
        print(f"✅ Inserted {inserted_count} new mappings")
        print(f"⏭️  Skipped {skipped_count} duplicate mappings")
        
        return inserted_count
    
    def add_missing_controlled_terms(self):
        """Add important controlled terms that are missing"""
        new_terms = [
            # Add Children since it's so common
            ('Children', 'Family Relations', 'Young people as characters or themes'),
            
            # Add more women-specific terms based on the unmapped
            ('Women physicians', 'Women-Specific', 'Female doctors'),
            ('Women lawyers', 'Women-Specific', 'Female attorneys'),
            ('Actresses', 'Women-Specific', 'Female stage/film performers'),
            
            # Add setting terms
            ('Rural life', 'Settings', 'Rural/country settings'),
            ('Urban life', 'Settings', 'City settings'),
            ('Boarding houses', 'Settings', 'Boarding house settings'),
            
            # Add more plot elements
            ('Deception', 'Plot Elements', 'Lies, tricks, false identities'),
            ('Secrets', 'Plot Elements', 'Hidden information driving plot'),
            ('Inheritance', 'Plot Elements', 'Inheritance disputes or windfalls'),
            ('Blackmail', 'Plot Elements', 'Extortion through threats'),
            ('Escapes', 'Plot Elements', 'Escape scenarios'),
            
            # Add emotional/psychological terms
            ('Jealousy', 'Emotions/States', 'Jealousy as motivation'),
            ('Revenge', 'Emotions/States', 'Vengeance as motivation'),
            ('Ambition', 'Emotions/States', 'Drive for success/power'),
            ('Mental illness', 'Health/Disability', 'Psychiatric conditions'),
            ('Alcoholism', 'Health/Disability', 'Alcohol addiction'),
            
            # Historical/temporal
            ('World War I', 'Historical Periods', '1914-1918 and aftermath'),
            ('Roaring Twenties', 'Historical Periods', '1920s era'),
            ('Great Depression', 'Historical Periods', '1929-1939 economic crisis'),
            ('World War II', 'Historical Periods', '1939-1945 and home front'),
        ]
        
        added_count = 0
        for term, facet, scope_note in new_terms:
            try:
                self.cursor.execute("""
                INSERT INTO controlled_terms (term, facet, scope_note, afi_frequency)
                VALUES (?, ?, ?, 0)
                """, (term, facet, scope_note))
                added_count += 1
            except sqlite3.IntegrityError:
                pass
        
        self.conn.commit()
        print(f"✅ Added {added_count} new controlled terms")
        
        # Refresh controlled terms dict
        self.controlled_terms = {}
        for row in self.cursor.execute("SELECT term_id, term FROM controlled_terms").fetchall():
            self.controlled_terms[row[1]] = row[0]
    
    def generate_mapping_stats(self):
        """Show statistics about the mapping coverage"""
        # Total AFI subjects
        total_afi = self.cursor.execute(
            "SELECT COUNT(DISTINCT subjects) FROM films WHERE subjects IS NOT NULL"
        ).fetchone()[0]
        
        # Mapped subjects
        mapped_count = self.cursor.execute(
            "SELECT COUNT(DISTINCT afi_subject) FROM afi_to_controlled_mapping"
        ).fetchone()[0]
        
        # High confidence mappings
        high_conf = self.cursor.execute(
            "SELECT COUNT(*) FROM afi_to_controlled_mapping WHERE confidence_score >= 0.9"
        ).fetchone()[0]
        
        print("\n=== Mapping Statistics ===")
        print(f"Total mapped subjects: {mapped_count}")
        print(f"High confidence mappings (≥0.9): {high_conf}")
        print(f"Coverage: {mapped_count/753*100:.1f}% of unique subjects")
        
        # Show mapping distribution by facet
        print("\nMappings by facet:")
        facet_stats = self.cursor.execute("""
            SELECT ct.facet, COUNT(DISTINCT m.afi_subject) as mapped_count
            FROM afi_to_controlled_mapping m
            JOIN controlled_terms ct ON m.controlled_term_id = ct.term_id
            GROUP BY ct.facet
            ORDER BY mapped_count DESC
        """).fetchall()
        
        for facet, count in facet_stats:
            print(f"  {facet}: {count}")
    
    def close(self):
        self.conn.close()


# Run the expanded mapping
if __name__ == "__main__":
    db_path = "film_research.db"  # Update this!
    
    mapper = ExpandedMappingCreator(db_path)
    
    # Add missing controlled terms first
    print("=== Adding Missing Controlled Terms ===")
    mapper.add_missing_controlled_terms()
    
    # Create expanded mappings
    print("\n=== Creating Expanded Mappings ===")
    mappings = mapper.create_expanded_mappings()
    
    # Insert them
    mapper.insert_mappings(mappings)
    
    # Show statistics
    mapper.generate_mapping_stats()
    
    mapper.close()
    
    print("\n🎉 Expanded mapping complete!")
    print("\nNext: Run 'apply_controlled_terms.py' to tag your films")