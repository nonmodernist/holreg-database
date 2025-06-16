import sqlite3
import csv
from datetime import datetime
from typing import Dict, List

class PrePopulatedDataImporter:
    def __init__(self, db_path: str = "data/databases/holreg_research.db"):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # Ensure we have the new columns
        self.ensure_columns_exist()
    
    def ensure_columns_exist(self):
        """Make sure all necessary columns exist in the database"""
        # Get existing columns
        self.cursor.execute("PRAGMA table_info(films)")
        existing_columns = [col[1] for col in self.cursor.fetchall()]
        
        # Add missing columns
        new_columns = [
            ('source_title', 'TEXT'),
            ('source_type', 'TEXT'),
            ('source_year', 'INTEGER'),
            ('source_publisher', 'TEXT'),
            ('source_notes', 'TEXT'),
            ('survival_status', 'TEXT'),
            ('survival_notes', 'TEXT'),
            ('archive_holdings', 'TEXT'),
            ('viewing_format', 'TEXT'),
            ('last_verified', 'DATE'),
            ('data_source', 'TEXT'),  # Track where data came from
            ('confidence_level', 'TEXT')  # Track confidence in the data
        ]
        
        for col_name, col_type in new_columns:
            if col_name not in existing_columns:
                try:
                    self.cursor.execute(f"ALTER TABLE films ADD COLUMN {col_name} {col_type}")
                    print(f"Added column: {col_name}")
                except sqlite3.OperationalError:
                    pass  # Column already exists
        
        self.conn.commit()
    
    def import_from_csv(self, csv_path: str, dry_run: bool = True):
        """Import pre-populated data from CSV file"""
        imported_count = 0
        skipped_count = 0
        updates = []
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                film_id = int(row['film_id'])
                confidence = row.get('confidence', 'low')
                
                # Prepare update data
                update_data = {}
                
                # Source information
                if row.get('source_title'):
                    update_data['source_title'] = row['source_title']
                    update_data['source_type'] = row.get('source_type')
                    
                    # Parse source year
                    if row.get('source_year'):
                        try:
                            update_data['source_year'] = int(row['source_year'])
                        except ValueError:
                            pass
                    
                    # Add notes about the source
                    if row.get('based_on_raw'):
                        update_data['source_notes'] = f"Wikipedia: {row['based_on_raw']}"
                
                # Survival information
                if row.get('survival_status'):
                    update_data['survival_status'] = row['survival_status']
                    
                    if row.get('archive_holdings'):
                        update_data['archive_holdings'] = row['archive_holdings']
                    
                    if row.get('viewing_format'):
                        update_data['viewing_format'] = row['viewing_format']
                    
                    if row.get('archive_url'):
                        survival_notes = f"Archive URL: {row['archive_url']}"
                        update_data['survival_notes'] = survival_notes
                
                # Metadata
                if update_data:
                    update_data['data_source'] = row.get('sources', 'wikipedia')
                    update_data['confidence_level'] = confidence
                    update_data['last_verified'] = datetime.now().date()
                    
                    updates.append((film_id, update_data))
                else:
                    skipped_count += 1
        
        # Show what would be updated
        if dry_run:
            print("\n=== DRY RUN - Would update: ===")
            for film_id, data in updates[:10]:  # Show first 10
                # Get film title for display
                self.cursor.execute("SELECT title, release_year FROM films WHERE id = ?", (film_id,))
                title, year = self.cursor.fetchone()
                
                print(f"\n{title} ({year}):")
                for key, value in data.items():
                    if key not in ['last_verified', 'data_source', 'confidence_level']:
                        print(f"  {key}: {value}")
            
            if len(updates) > 10:
                print(f"\n... and {len(updates) - 10} more films")
            
            print(f"\nTotal updates: {len(updates)}")
            print(f"Skipped (no data): {skipped_count}")
            
            return updates
        
        # Actually perform updates
        for film_id, data in updates:
            # Build UPDATE query dynamically
            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            values = list(data.values()) + [film_id]
            
            query = f"UPDATE films SET {set_clause} WHERE id = ?"
            
            try:
                self.cursor.execute(query, values)
                imported_count += 1
            except Exception as e:
                print(f"Error updating film {film_id}: {e}")
        
        self.conn.commit()
        
        print(f"\n✅ Imported data for {imported_count} films")
        print(f"⏭️  Skipped {skipped_count} films (no data found)")
        
        return updates
    
    def verify_imports(self):
        """Show statistics about imported data"""
        print("\n=== IMPORT VERIFICATION ===")
        
        # Overall statistics
        stats_query = """
        SELECT 
            COUNT(*) as total_films,
            SUM(CASE WHEN source_title IS NOT NULL THEN 1 ELSE 0 END) as with_source,
            SUM(CASE WHEN survival_status IS NOT NULL THEN 1 ELSE 0 END) as with_survival,
            SUM(CASE WHEN data_source = 'wikipedia' THEN 1 ELSE 0 END) as from_wikipedia,
            SUM(CASE WHEN data_source = 'wikidata' THEN 1 ELSE 0 END) as from_wikidata,
            SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) as high_confidence,
            SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) as low_confidence
        FROM films
        """
        
        result = self.cursor.execute(stats_query).fetchone()
        
        total, with_source, with_survival, from_wiki, from_wikidata, high_conf, low_conf = result
        
        print(f"\nTotal films: {total}")
        print(f"With source data: {with_source} ({with_source/total*100:.1f}%)")
        print(f"With survival status: {with_survival} ({with_survival/total*100:.1f}%)")
        print(f"\nData sources:")
        print(f"  From Wikipedia: {from_wiki}")
        print(f"  From Wikidata: {from_wikidata}")
        print(f"\nConfidence levels:")
        print(f"  High confidence: {high_conf}")
        print(f"  Low confidence: {low_conf}")
        
        # Sample of imported data
        print("\n=== SAMPLE IMPORTED DATA ===")
        sample_query = """
        SELECT title, release_year, source_title, source_type, 
               survival_status, confidence_level
        FROM films
        WHERE data_source IS NOT NULL
        ORDER BY confidence_level DESC, release_year DESC
        LIMIT 10
        """
        
        for row in self.cursor.execute(sample_query):
            title, year, source, s_type, survival, confidence = row
            print(f"\n{title} ({year}) [{confidence}]:")
            if source:
                print(f"  Source: {source} ({s_type})")
            if survival:
                print(f"  Status: {survival}")
    
    def export_gaps_report(self):
        """Export report of films still needing data"""
        gaps_query = """
        SELECT id, title, release_year, literary_credits
        FROM films
        WHERE literary_credits IS NOT NULL
          AND (source_title IS NULL OR survival_status IS NULL)
        ORDER BY release_year DESC
        """
        
        with open('data_gaps_report.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['film_id', 'title', 'year', 'literary_credits', 
                           'needs_source', 'needs_survival'])
            
            for film_id, title, year, credits in self.cursor.execute(gaps_query):
                # Check what's missing
                self.cursor.execute(
                    "SELECT source_title, survival_status FROM films WHERE id = ?",
                    (film_id,)
                )
                source, survival = self.cursor.fetchone()
                
                needs_source = 'YES' if not source else 'NO'
                needs_survival = 'YES' if not survival else 'NO'
                
                writer.writerow([film_id, title, year, credits, needs_source, needs_survival])
        
        print("\n📄 Exported 'data_gaps_report.csv' for remaining research")
    
    def close(self):
        self.conn.close()


# Usage
if __name__ == "__main__":
    print("=== Pre-populated Data Import Tool ===\n")
    
    importer = PrePopulatedDataImporter()
    
    # First, do a dry run
    print("Running dry run to preview changes...")
    csv_file = input("Enter CSV filename (default: wikipedia_prepopulated_data.csv): ").strip()
    if not csv_file:
        csv_file = "wikipedia_prepopulated_data.csv"
    
    try:
        updates = importer.import_from_csv(csv_file, dry_run=True)
        
        if updates:
            proceed = input("\nProceed with import? (y/n): ")
            
            if proceed.lower() == 'y':
                # Do actual import
                importer.import_from_csv(csv_file, dry_run=False)
                
                # Verify imports
                importer.verify_imports()
                
                # Export gaps report
                importer.export_gaps_report()
        else:
            print("No data to import found in CSV")
    
    except FileNotFoundError:
        print(f"Error: Could not find file '{csv_file}'")
        print("Make sure you've run the Wikipedia pre-population script first!")
    
    finally:
        importer.close()