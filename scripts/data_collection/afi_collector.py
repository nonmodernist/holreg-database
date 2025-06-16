import requests
import json
import sqlite3
import time
from typing import List, Dict, Any, Tuple, Optional

class AFICatalogCollector:
    def __init__(self, db_path: str = "film_research.db"):
        self.base_url = "https://catalog.afi.com"
        self.search_endpoint = "/Search/Search"
        self.db_path = db_path
        self.session = requests.Session()
        
        # Set up headers to mimic browser request
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://catalog.afi.com/Search'
        })
        
        self.init_database()
    
    def init_database(self):
        """Create database tables for storing film research data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main films table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS films (
                id INTEGER PRIMARY KEY,
                afi_movie_id TEXT UNIQUE,
                title TEXT,
                release_year INTEGER,
                release_date TEXT,
                director TEXT,
                director_id TEXT,
                writer TEXT,
                producer TEXT,
                genre TEXT,
                sub_genre TEXT,
                film_type TEXT,
                subjects TEXT,
                literary_credits TEXT,
                source_citations TEXT,
                filming_location TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Production companies table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS production_companies (
                id INTEGER PRIMARY KEY,
                film_id INTEGER,
                company_name TEXT,
                company_type TEXT, -- 'production' or 'distribution'
                FOREIGN KEY (film_id) REFERENCES films (id)
            )
        ''')
        
        # Cast table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cast_crew (
                id INTEGER PRIMARY KEY,
                film_id INTEGER,
                person_name TEXT,
                person_id TEXT,
                role TEXT, -- 'cast1', 'cast2', 'director', etc.
                FOREIGN KEY (film_id) REFERENCES films (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def search_film(self, movie_title: str, target_year: int = None) -> Dict[str, Any]:
        """
        Search for a specific film in the AFI catalog
        """
        # Exact POST data format from browser network tab
        search_data = {
            'searchText': movie_title,
            'searchField': 'MovieName',
            'directorFacet': '',
            'producerFacet': '',
            'releaseYearFacet': '',
            'productionCompanyFacet': '',
            'genreFacet': '',
            'filmTypeFacet': '',
            'moviesOnly': 'true',
            'peopleOnly': 'false',
            'sortType': 'sortByRelevance',
            'currentPage': '1',
            'searchId': '',
            'logSearch': 'false',
            'isCompact': 'false'
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}{self.search_endpoint}",
                data=search_data
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error searching for {movie_title}: {e}")
            return {}
    
    def extract_film_data(self, search_result: Dict[str, Any], target_title: str, target_year: int) -> Optional[Dict[str, Any]]:
        """
        Extract film data ONLY if it exactly matches the target title and year
        Returns None if no exact match is found
        """
        if 'MovieSearch' not in search_result or 'Results' not in search_result['MovieSearch']:
            return None
        
        # Normalize the target title for comparison
        target_title_normalized = target_title.strip().lower()
        
        for result in search_result['MovieSearch']['Results']:
            doc = result.get('Document', {})
            
            # Get film title and year
            film_title = doc.get('MovieName', '')
            film_year = doc.get('ReleaseYear', '')
            
            # Normalize film title for comparison
            film_title_normalized = film_title.strip().lower()
            
            # Check for exact title match (case-insensitive)
            if film_title_normalized != target_title_normalized:
                continue
            
            # Check for exact year match
            try:
                if int(film_year) != target_year:
                    continue
            except (ValueError, TypeError):
                # If year can't be parsed or is missing, skip this result
                continue
            
            # If we get here, we have an exact match!
            print(f"  Found exact match: {film_title} ({film_year})")
            
            film_data = {
                'afi_movie_id': doc.get('MovieId'),
                'title': film_title,
                'release_year': int(film_year),
                'release_date': doc.get('ReleaseDate'),
                'director': doc.get('Director'),
                'director_id': doc.get('DirectorId'),
                'writer': doc.get('Writer'),
                'producer': doc.get('Producer'),
                'genre': '|'.join(doc.get('Genre', [])),
                'sub_genre': doc.get('SubGenre'),
                'film_type': doc.get('FilmType'),
                'subjects': doc.get('Subjects'),
                'literary_credits': doc.get('LiteraryNoteCredits'),
                'source_citations': doc.get('SourceCitations'),
                'filming_location': doc.get('NoteGeo'),
                'production_companies': doc.get('ProductionCompany', []),
                'distribution_companies': doc.get('DistributionCompany', []),
                'cast_data': doc.get('Casts', '')
            }
            
            return film_data
        
        # No exact match found
        print(f"  No exact match found for {target_title} ({target_year})")
        return None
    
    def save_film_data(self, film_data: Dict[str, Any]):
        """Save film data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Insert main film record
        cursor.execute('''
            INSERT OR REPLACE INTO films 
            (afi_movie_id, title, release_year, release_date, director, director_id,
             writer, producer, genre, sub_genre, film_type, subjects, 
             literary_credits, source_citations, filming_location)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            film_data['afi_movie_id'], film_data['title'], film_data['release_year'],
            film_data['release_date'], film_data['director'], film_data['director_id'],
            film_data['writer'], film_data['producer'], film_data['genre'],
            film_data['sub_genre'], film_data['film_type'], film_data['subjects'],
            film_data['literary_credits'], film_data['source_citations'],
            film_data['filming_location']
        ))
        
        film_id = cursor.lastrowid
        
        # Insert production companies
        for company in film_data.get('production_companies', []):
            cursor.execute('''
                INSERT INTO production_companies (film_id, company_name, company_type)
                VALUES (?, ?, ?)
            ''', (film_id, company, 'production'))
        
        for company in film_data.get('distribution_companies', []):
            cursor.execute('''
                INSERT INTO production_companies (film_id, company_name, company_type)
                VALUES (?, ?, ?)
            ''', (film_id, company, 'distribution'))
        
        conn.commit()
        conn.close()
    
    def collect_films_from_list(self, movie_list: List[Tuple[str, int]], delay: float = 1.0):
        """
        Collect data for a list of movies with exact title/year matching
        
        Args:
            movie_list: List of tuples (title, year)
            delay: Delay between requests in seconds
        """
        print(f"Starting collection for {len(movie_list)} films...")
        
        successful_matches = 0
        failed_matches = []
        
        for i, (title, year) in enumerate(movie_list):
            print(f"Processing {i+1}/{len(movie_list)}: {title} ({year})")
            
            search_result = self.search_film(title)
            if search_result:
                film_data = self.extract_film_data(search_result, title, year)
                
                if film_data:
                    self.save_film_data(film_data)
                    print(f"  ✓ Saved: {film_data['title']} ({film_data['release_year']})")
                    successful_matches += 1
                else:
                    print(f"  ✗ No exact match found")
                    failed_matches.append((title, year))
            else:
                print(f"  ✗ Search failed")
                failed_matches.append((title, year))
            
            # Rate limiting - be respectful to their servers
            time.sleep(delay)
        
        # Summary
        print(f"\n=== COLLECTION SUMMARY ===")
        print(f"Total films processed: {len(movie_list)}")
        print(f"Successful matches: {successful_matches}")
        print(f"Failed matches: {len(failed_matches)}")
        
        if failed_matches:
            print("\nFailed to find exact matches for:")
            for title, year in failed_matches:
                print(f"  - {title} ({year})")
    
    def analyze_adaptations(self):
        """Analyze adaptation patterns in collected data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Find films with literary credits (adaptations)
        cursor.execute('''
            SELECT title, release_year, literary_credits, 
                   GROUP_CONCAT(company_name) as companies
            FROM films 
            LEFT JOIN production_companies ON films.id = production_companies.film_id
            WHERE literary_credits IS NOT NULL AND literary_credits != ''
            GROUP BY films.id
            ORDER BY literary_credits, release_year
        ''')
        
        adaptations = cursor.fetchall()
        conn.close()
        
        print("\n=== ADAPTATION ANALYSIS ===")
        
        # Group by source author/work
        from collections import defaultdict
        by_source = defaultdict(list)
        
        for title, year, credits, companies in adaptations:
            # Extract primary author (first name in credits)
            primary_author = credits.split('|')[0].strip() if credits else "Unknown"
            by_source[primary_author].append((title, year, companies))
        
        # Show multiple adaptations
        print("\nSOURCES WITH MULTIPLE ADAPTATIONS:")
        for author, films in by_source.items():
            if len(films) > 1:
                print(f"\n{author}:")
                for title, year, companies in sorted(films, key=lambda x: int(x[1]) if x[1] else 0):
                    print(f"  {year}: {title} ({companies})")
                
                # Calculate adaptation gaps
                years = [year for title, year, companies in films if year is not None]
                if len(years) > 1:
                    years.sort()
                    gaps = [years[i+1] - years[i] for i in range(len(years)-1)]
                    print(f"  Adaptation gaps: {gaps} years")
        
        # Studio adaptation preferences
        print("\n\nSTUDIO ADAPTATION ACTIVITY:")
        studio_counts = defaultdict(int)
        for title, year, credits, companies in adaptations:
            if companies:
                for company in companies.split(','):
                    studio_counts[company.strip()] += 1
        
        for studio, count in sorted(studio_counts.items(), key=lambda x: x[1], reverse=True):
            if count > 1:
                print(f"{studio}: {count} adaptations")
    
    def export_research_data(self, filename: str = "adaptation_research.csv"):
        """Export data for further analysis"""
        import csv
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT films.title, films.release_year, films.director, 
                   films.literary_credits, films.subjects, films.filming_location,
                   GROUP_CONCAT(CASE WHEN company_type='production' THEN company_name END) as production_cos,
                   GROUP_CONCAT(CASE WHEN company_type='distribution' THEN company_name END) as distribution_cos
            FROM films 
            LEFT JOIN production_companies ON films.id = production_companies.film_id
            WHERE films.literary_credits IS NOT NULL AND films.literary_credits != ''
            GROUP BY films.id
            ORDER BY films.release_year
        ''')
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Title', 'Year', 'Director', 'Literary_Credits', 'Subjects', 
                           'Filming_Location', 'Production_Companies', 'Distribution_Companies'])
            writer.writerows(cursor.fetchall())
        
        conn.close()
        print(f"\nResearch data exported to {filename}")


# Example usage
if __name__ == "__main__":
    # Your complete list of 125 films
    your_film_list = [
        ("Fieldwork Footage", 1928),
        ("Ethnographic Films", 1929),
        ("Commandment Keeper Church, Beaufort South Carolina, May 1940", 1940),
        ("Mrs. Wiggs of the Cabbage Patch", 1914),
        ("A Romance of Billy Goat Hill", 1916),
        ("Mr. Opp", 1917),
        ("Sandy", 1918),
        ("Sunshine Nan", 1918),
        ("Mrs. Wiggs of the Cabbage Patch", 1919),
        ("Lovey Mary", 1926),
        ("Mrs. Wiggs of the Cabbage Patch", 1934),
        ("Mrs. Wiggs of the Cabbage Patch", 1942),
        ("Judith of the Cumberlands", 1916),
        ("Hungry Hearts", 1922),
        ("Salome of the Tenements", 1925),
        ("Amarilly of Clothes-Line Alley", 1918),
        ("The Woman Who Was Forgotten", 1930),
        ("Cheers for Miss Bishop", 1941),
        ("The Egg and I", 1947),
        ("A Tree Grows in Brooklyn", 1945),
        ("The Member of the Wedding", 1953),
        ("Pinky", 1949),
        ("Tammy and the Bachelor", 1957),
        ("The Hanging Tree", 1959),
        ("That Hagen Girl", 1947),
        ("So Big", 1924),
        ("Cimarron", 1931),
        ("So Big", 1932),
        ("Come and Get It", 1936),
        ("So Big", 1953),
        ("Giant", 1956),
        ("Cimarron", 1960),
        ("Ice Palace", 1960),
        ("Underworld", 1937),
        ("Dawn", 1919),
        ("Pollyanna", 1920),
        ("Has Anybody Seen My Gal", 1952),
        ("Pollyanna", 1960),
        ("Kildare of Storm", 1918),
        ("In This Our Life", 1942),
        ("Humoresque", 1920),
        ("Just Around the Corner", 1921),
        ("The Good Provider", 1922),
        ("Back Street", 1932),
        ("Imitation of Life", 1934),
        ("Back Street", 1941),
        ("Humoresque", 1947),
        ("Back Street", 1948),
        ("Imitation of Life", 1959),
        ("Back Street", 1961),
        ("Gene of the Northland", 1915),
        ("Freckles", 1917),
        ("Michael O'Halloran", 1923),
        ("A Girl of the Limberlost", 1924),
        ("The Keeper of the Bees", 1925),
        ("Laddie", 1926),
        ("The Harvester", 1927),
        ("The Magic Garden", 1927),
        ("Freckles", 1928),
        ("A Girl of the Limberlost", 1934),
        ("Freckles", 1935),
        ("Laddie", 1935),
        ("The Keeper of the Bees", 1935),
        ("The Harvester", 1936),
        ("Michael O'Halloran", 1937),
        ("Romance of the Limberlost", 1938),
        ("Her First Romance", 1940),
        ("Laddie", 1940),
        ("The Girl of the Limberlost", 1945),
        ("Keeper of the Bees", 1947),
        ("Michael O'Halloran", 1948),
        ("Freckles", 1960),
        ("Freckles Comes Home", 1942),
        ("The Panther Woman", 1918),
        ("Tess of the Storm Country", 1914),
        ("The Secret of the Storm Country", 1917),
        ("Judy of Rogue's Harbor", 1920),
        ("Polly of the Storm Country", 1920),
        ("Tess of the Storm Country", 1922),
        ("Tess of the Storm Country", 1932),
        ("Tess of the Storm Country", 1960),
        ("Uncle Tom's Cabin", 1910),
        ("Uncle Tom's Cabin", 1914),
        ("Uncle Tom's Cabin", 1918),
        ("The Pearl of Love", 1925),
        ("Topsy and Eva", 1927),
        ("Uncle Tom's Cabin", 1927),
        ("Janet of the Dunes", 1913),
        ("Joyce of the North Woods", 1913),
        ("The Place Beyond the Winds", 1916),
        ("A Son of the Hills", 1917),
        ("Silent Years", 1921),
        ("Mother's Cry", 1930),
        ("Ramona", 1910),
        ("Ramona", 1916),
        ("Ramona", 1928),
        ("Ramona", 1936),
        ("Rebecca of Sunnybrook Farm", 1917),
        ("Rebecca of Sunnybrook Farm", 1932),
        ("Rebecca of Sunnybrook Farm", 1938),
        ("Nobody's Kid", 1921),
        ("I Remember Mama", 1948),
        ("The Silent Woman", 1918),
        ("Lucy Gallant", 1955),
        ("The Awakening of Helena Richie", 1916),
        ("The Iron Woman", 1916),
        ("Gone with the Wind", 1939),
        ("Linda", 1929),
        ("The Curse of Quon Gwon", 1917),
        ("The Yearling", 1947),
        ("The Sun Comes Up", 1949),
        ("Bright Road", 1953),
        ("My Friend Flicka", 1943),
        ("Thunderhead Son of Flicka", 1945),
        ("Green Grass of Wyoming", 1948),
        ("The Mountain Rat", 1914),
        ("Sunshine Alley", 1917),
        ("An Alabaster Box", 1917),
        ("False Evidence", 1919),
        ("Little Miss Smiles", 1922),
        ("The Vanishing Virginian", 1942),
        ("The Primrose Ring", 1917),
        ("Sparrows", 1926),
        ("Miss Lulu Bett", 1922)
    ]
    
    # Create collector instance
    collector = AFICatalogCollector()
    
    # Test with a small subset first
    test_movies = [
        ("Ramona", 1928),
        ("Uncle Tom's Cabin", 1927),
        ("Back Street", 1932)
    ]
    
    # print("Testing with small subset...")
    # collector.collect_films_from_list(test_movies, delay=1.0)
    
    # Uncomment to run full collection
    print("\nRunning full collection...")
    collector.collect_films_from_list(your_film_list, delay=1.5)
    
    # Analyze patterns
    collector.analyze_adaptations()
    
    # Export research data
    collector.export_research_data("adaptation_research_complete.csv")