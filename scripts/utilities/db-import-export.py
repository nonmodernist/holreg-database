#!/usr/bin/env python3
"""
Database export/import utilities for version control
Exports database tables to CSV for git tracking
Imports CSV files to rebuild database
"""

import sqlite3
import pandas as pd
import os
from pathlib import Path

class DatabaseManager:
    def __init__(self, db_path='data/databases/adaptation_research.db'):
        self.db_path = db_path
        self.export_dir = Path('data/csv_exports')
        self.export_dir.mkdir(parents=True, exist_ok=True)
    
    def export_to_csv(self):
        """Export all tables to CSV files"""
        conn = sqlite3.connect(self.db_path)
        
        # Get all table names
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            print(f"Exporting {table_name}...")
            
            # Read table into pandas
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            
            # Export to CSV
            csv_path = self.export_dir / f"{table_name}.csv"
            df.to_csv(csv_path, index=False)
            print(f"  Exported to {csv_path}")
        
        conn.close()
        print(f"\nAll tables exported to {self.export_dir}")
    
    def import_from_csv(self, rebuild=False):
        """Import CSV files to database"""
        if rebuild and os.path.exists(self.db_path):
            print(f"Removing existing database: {self.db_path}")
            os.remove(self.db_path)
        
        conn = sqlite3.connect(self.db_path)
        
        # Find all CSV files
        csv_files = list(self.export_dir.glob('*.csv'))
        
        for csv_file in csv_files:
            table_name = csv_file.stem
            print(f"Importing {table_name} from {csv_file}...")
            
            # Read CSV
            df = pd.read_csv(csv_file)
            
            # Import to database
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"  Imported {len(df)} rows")
        
        conn.close()
        print(f"\nDatabase rebuilt at {self.db_path}")
    
    def get_db_info(self):
        """Get information about the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get file size
        file_size = os.path.getsize(self.db_path) / 1024 / 1024  # MB
        print(f"Database size: {file_size:.2f} MB")
        
        # Get table info
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = cursor.fetchall()
        
        print("\nTables and row counts:")
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  {table_name}: {count} rows")
        
        conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Database export/import utility')
    parser.add_argument('action', choices=['export', 'import', 'info'], 
                        help='Action to perform')
    parser.add_argument('--rebuild', action='store_true', 
                        help='Rebuild database from scratch (import only)')
    parser.add_argument('--db', default='data/databases/adaptation_research.db',
                        help='Path to database file')
    
    args = parser.parse_args()
    
    manager = DatabaseManager(args.db)
    
    if args.action == 'export':
        manager.export_to_csv()
    elif args.action == 'import':
        manager.import_from_csv(rebuild=args.rebuild)
    elif args.action == 'info':
        manager.get_db_info()