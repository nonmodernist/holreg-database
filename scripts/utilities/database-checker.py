import sqlite3
import os

def check_database_contents(db_path="film_research.db"):
    """Check what's actually in the database"""
    
    # Check if database file exists
    if not os.path.exists(db_path):
        print(f"Database file '{db_path}' does not exist yet.")
        print("The database will be created when you run the collector script.")
        return
    
    print(f"Database file '{db_path}' exists!")
    print(f"File size: {os.path.getsize(db_path)} bytes")
    
    # Connect and check contents
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check what tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"\nTables in database: {[table[0] for table in tables]}")
    
    # Check films table
    try:
        cursor.execute("SELECT COUNT(*) FROM films")
        film_count = cursor.fetchone()[0]
        print(f"\nFilms in database: {film_count}")
        
        if film_count > 0:
            cursor.execute("SELECT title, release_year, literary_credits FROM films LIMIT 5")
            sample_films = cursor.fetchall()
            print("\nSample films:")
            for title, year, credits in sample_films:
                print(f"  - {title} ({year}) - Credits: {credits}")
    except sqlite3.OperationalError as e:
        print(f"Error checking films table: {e}")
    
    # Check production companies table
    try:
        cursor.execute("SELECT COUNT(*) FROM production_companies")
        company_count = cursor.fetchone()[0]
        print(f"\nProduction companies: {company_count}")
        
        if company_count > 0:
            cursor.execute("SELECT company_name, company_type FROM production_companies LIMIT 5")
            sample_companies = cursor.fetchall()
            print("\nSample companies:")
            for name, type in sample_companies:
                print(f"  - {name} ({type})")
    except sqlite3.OperationalError as e:
        print(f"Error checking production companies table: {e}")
    
    conn.close()

def test_single_search():
    """Test a single search to see if data saving works"""
    from afi_collector import AFICatalogCollector
    
    print("\n=== TESTING SINGLE SEARCH AND SAVE ===")
    collector = AFICatalogCollector()
    
    # Test search
    result = collector.search_film("Ramona", 1928)
    if result:
        films = collector.extract_film_data(result, 1928)
        print(f"Found {len(films)} films for Ramona (1928)")
        
        # Save the data
        for film in films:
            collector.save_film_data(film)
            print(f"Saved: {film['title']} ({film['release_year']})")
            print(f"  Literary credits: {film['literary_credits']}")
            print(f"  Production companies: {film['production_companies']}")
    
    # Check database again
    print("\n=== CHECKING DATABASE AFTER SAVE ===")
    check_database_contents()

if __name__ == "__main__":
    print("=== INITIAL DATABASE CHECK ===")
    check_database_contents()
    
    # Test saving a few films first
    print("\n=== TESTING DATA COLLECTION ===")
    test_films = [
        ("Ramona", 1928),
        ("Gone with the Wind", 1939),
        ("Uncle Tom's Cabin", 1927)
    ]
    
    from afi_collector import AFICatalogCollector
    collector = AFICatalogCollector()
    
    print("Testing collection with 3 films...")
    collector.collect_films_from_list(test_films, delay=0.5)
    
    print("\n=== FINAL DATABASE CHECK ===")
    check_database_contents()