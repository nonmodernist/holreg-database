import requests
import sqlite3
import time
import re
from typing import Dict, Optional, List, Tuple
from datetime import datetime
import json

class WikipediaWikidataPrePopulator:
    def __init__(self, db_path: str = "data/databases/holreg_research.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # API endpoints
        self.wikipedia_api = "https://en.wikipedia.org/w/api.php"
        self.wikidata_api = "https://www.wikidata.org/w/api.php"
        self.wikidata_sparql = "https://query.wikidata.org/sparql"
        
        # Common headers
        self.headers = {
            'User-Agent': 'FilmResearchBot/1.0 (https://example.com/contact) python-requests/2.28.0'
        }
        
        # Cache to avoid repeated API calls
        self.cache = {}
        
    def search_wikipedia(self, film_title: str, year: int) -> Optional[Dict]:
        """Search Wikipedia for film article"""
        cache_key = f"wiki_{film_title}_{year}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Try with year first
        search_terms = [
            f"{film_title} {year} film",
            f"{film_title} ({year} film)",
            f"{film_title} film"
        ]
        
        for search_term in search_terms:
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': search_term,
                'srlimit': 5
            }
            
            try:
                response = requests.get(self.wikipedia_api, params=params, headers=self.headers)
                data = response.json()
                
                # Look for best match
                for result in data.get('query', {}).get('search', []):
                    title = result['title']
                    # Check if it's likely a film article
                    if 'film' in title.lower() or str(year) in title:
                        page_data = self.get_wikipedia_page(title)
                        if page_data:
                            self.cache[cache_key] = page_data
                            return page_data
                
            except Exception as e:
                print(f"Error searching Wikipedia for {film_title}: {e}")
        
        self.cache[cache_key] = None
        return None
    
    def get_wikipedia_page(self, page_title: str) -> Optional[Dict]:
        """Get Wikipedia page content and extract film data"""
        params = {
            'action': 'query',
            'format': 'json',
            'titles': page_title,
            'prop': 'revisions|pageprops',
            'rvprop': 'content',
            'rvslots': 'main'
        }
        
        try:
            response = requests.get(self.wikipedia_api, params=params, headers=self.headers)
            data = response.json()
            pages = data.get('query', {}).get('pages', {})
            
            for page_id, page_data in pages.items():
                if page_id != '-1':  # Page exists
                    # Get wikidata ID if available
                    wikidata_id = page_data.get('pageprops', {}).get('wikibase_item')
                    
                    # Get page content
                    try:
                        content = page_data['revisions'][0]['slots']['main']['*']
                        return {
                            'title': page_data['title'],
                            'wikidata_id': wikidata_id,
                            'content': content
                        }
                    except:
                        pass
        
        except Exception as e:
            print(f"Error getting Wikipedia page {page_title}: {e}")
        
        return None
    
    def extract_from_wikipedia_content(self, content: str) -> Dict:
        """Extract film information from Wikipedia article content"""
        data = {}
        
        # Extract from infobox using regex
        infobox_match = re.search(r'\{\{Infobox film(.*?)\}\}', content, re.DOTALL | re.IGNORECASE)
        if infobox_match:
            infobox = infobox_match.group(1)
            
            # Based on - source material
            based_on_match = re.search(r'\|\s*based[_ ]on\s*=\s*(.*?)(?=\n\||\}\})', infobox, re.IGNORECASE)
            if based_on_match:
                based_on = based_on_match.group(1).strip()
                # Clean up wiki markup
                based_on = re.sub(r'\[\[(.*?)\]\]', r'\1', based_on)  # Remove wiki links
                based_on = re.sub(r'\{\{.*?\}\}', '', based_on)  # Remove templates
                based_on = re.sub(r'<.*?>', '', based_on)  # Remove HTML
                data['based_on_raw'] = based_on.strip()
                
                # Try to parse source type
                if 'novel' in based_on.lower():
                    data['source_type'] = 'novel'
                elif 'short story' in based_on.lower():
                    data['source_type'] = 'short story'
                elif 'play' in based_on.lower():
                    data['source_type'] = 'play'
                elif 'story' in based_on.lower():
                    data['source_type'] = 'short story'
        
        # Look for preservation/availability info in article
        if 'lost film' in content.lower():
            data['survival_status'] = 'lost'
        elif 'surviving' in content.lower() or 'preserved' in content.lower():
            data['survival_status'] = 'extant'
        
        # Extract archive information
        archive_patterns = [
            r'preserved at ([^\.]+)',
            r'held by ([^\.]+)',
            r'archived at ([^\.]+)',
            r'copy at ([^\.]+)',
            r'Library of Congress',
            r'Academy Film Archive',
            r'UCLA Film [&and]+ Television Archive',
            r'George Eastman Museum',
            r'Museum of Modern Art'
        ]
        
        archives = []
        for pattern in archive_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            archives.extend(matches)
        
        if archives:
            data['archive_holdings'] = ', '.join(set(archives))
        
        return data
    
    def query_wikidata(self, wikidata_id: str) -> Optional[Dict]:
        """Query Wikidata for structured film information"""
        if not wikidata_id:
            return None
        
        # SPARQL query for film data
        query = f"""
        SELECT ?item ?itemLabel ?basedOn ?basedOnLabel ?publicationDate 
               ?genre ?genreLabel ?director ?directorLabel
               ?archiveURL ?distributionFormat ?distributionFormatLabel
        WHERE {{
          VALUES ?item {{ wd:{wikidata_id} }}
          OPTIONAL {{ ?item wdt:P144 ?basedOn. }}  # based on
          OPTIONAL {{ ?basedOn wdt:P577 ?publicationDate. }}  # publication date
          OPTIONAL {{ ?item wdt:P136 ?genre. }}  # genre
          OPTIONAL {{ ?item wdt:P57 ?director. }}  # director
          OPTIONAL {{ ?item wdt:P1651 ?archiveURL. }}  # archive URL
          OPTIONAL {{ ?item wdt:P437 ?distributionFormat. }}  # distribution format
          
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        """
        
        try:
            response = requests.get(
                self.wikidata_sparql,
                params={'query': query, 'format': 'json'},
                headers=self.headers
            )
            data = response.json()
            
            if data.get('results', {}).get('bindings'):
                result = data['results']['bindings'][0]
                
                wikidata_info = {}
                
                # Based on work
                if 'basedOnLabel' in result:
                    wikidata_info['source_title'] = result['basedOnLabel']['value']
                    
                # Publication date of source
                if 'publicationDate' in result:
                    pub_date = result['publicationDate']['value']
                    year_match = re.search(r'(\d{4})', pub_date)
                    if year_match:
                        wikidata_info['source_year'] = int(year_match.group(1))
                
                # Archive URL
                if 'archiveURL' in result:
                    wikidata_info['archive_url'] = result['archiveURL']['value']
                
                # Distribution format (can indicate survival)
                if 'distributionFormatLabel' in result:
                    format_label = result['distributionFormatLabel']['value']
                    wikidata_info['viewing_format'] = format_label
                    if format_label.lower() in ['dvd', 'blu-ray', 'streaming media']:
                        wikidata_info['survival_status'] = 'extant'
                
                return wikidata_info
        
        except Exception as e:
            print(f"Error querying Wikidata for {wikidata_id}: {e}")
        
        return None
    
    def process_film(self, film_id: int, title: str, year: int, literary_credits: str) -> Dict:
        """Process a single film to gather Wikipedia/Wikidata information"""
        print(f"Processing: {title} ({year})")
        
        results = {
            'film_id': film_id,
            'sources': [],
            'confidence': 'low'
        }
        
        # Search Wikipedia
        wiki_data = self.search_wikipedia(title, year)
        
        if wiki_data:
            # Extract from Wikipedia content
            extracted = self.extract_from_wikipedia_content(wiki_data['content'])
            results.update(extracted)
            
            # Query Wikidata if we have ID
            if wiki_data.get('wikidata_id'):
                wikidata_info = self.query_wikidata(wiki_data['wikidata_id'])
                if wikidata_info:
                    results.update(wikidata_info)
                    results['confidence'] = 'high'
                    results['sources'].append('wikidata')
            
            results['sources'].append('wikipedia')
            
            # Try to match with literary credits
            if 'based_on_raw' in results and literary_credits:
                primary_author = literary_credits.split('|')[0].strip()
                if primary_author.lower() in results['based_on_raw'].lower():
                    results['confidence'] = 'high'
        
        return results
    
    def batch_process_films(self, limit: Optional[int] = None) -> List[Dict]:
        """Process all films needing source/survival data"""
        # Get films that need data
        query = """
            SELECT id, title, release_year, literary_credits 
            FROM films 
            WHERE literary_credits IS NOT NULL 
                AND (source_title IS NULL OR survival_status IS NULL)
            ORDER BY release_year DESC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        films = self.cursor.execute(query).fetchall()
        print(f"Processing {len(films)} films...")
        
        results = []
        
        for i, (film_id, title, year, credits) in enumerate(films):
            if i > 0 and i % 10 == 0:
                print(f"Progress: {i}/{len(films)}")
                time.sleep(1)  # Rate limiting
            
            try:
                film_results = self.process_film(film_id, title, year, credits)
                results.append(film_results)
                
                # Small delay to be respectful to APIs
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error processing {title}: {e}")
                continue
        
        return results
    
    def save_results_to_csv(self, results: List[Dict], filename: str = "wikipedia_prepopulated_data.csv"):
        """Save results to CSV for review before importing"""
        import csv
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'film_id', 'confidence', 'sources', 'source_title', 'source_type', 
                'source_year', 'based_on_raw', 'survival_status', 'archive_holdings',
                'viewing_format', 'archive_url'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                # Convert sources list to string
                result['sources'] = ', '.join(result.get('sources', []))
                # Only write fields that exist
                row = {k: v for k, v in result.items() if k in fieldnames}
                writer.writerow(row)
        
        print(f"\nResults saved to {filename}")
        print("Review the data before importing to ensure accuracy!")
    
    def generate_summary_report(self, results: List[Dict]):
        """Generate summary of what was found"""
        total = len(results)
        with_source = sum(1 for r in results if r.get('source_title'))
        with_survival = sum(1 for r in results if r.get('survival_status'))
        high_confidence = sum(1 for r in results if r.get('confidence') == 'high')
        
        print("\n=== PRE-POPULATION SUMMARY ===")
        print(f"Films processed: {total}")
        print(f"Found source information: {with_source} ({with_source/total*100:.1f}%)")
        print(f"Found survival status: {with_survival} ({with_survival/total*100:.1f}%)")
        print(f"High confidence results: {high_confidence} ({high_confidence/total*100:.1f}%)")
        
        # Survival status breakdown
        survival_counts = {}
        for r in results:
            status = r.get('survival_status', 'unknown')
            survival_counts[status] = survival_counts.get(status, 0) + 1
        
        print("\nSurvival status found:")
        for status, count in survival_counts.items():
            print(f"  {status}: {count}")
    
    def close(self):
        self.conn.close()


# Example usage
if __name__ == "__main__":
    print("=== Wikipedia/Wikidata Pre-Population Tool ===")
    print("This tool will search for your films on Wikipedia/Wikidata")
    print("and extract source text and survival information.\n")
    
    populator = WikipediaWikidataPrePopulator()
    
    # Test with a few films first
    print("Testing with a sample of films...")
    test_results = populator.batch_process_films(limit=5)
    
    print("\n=== TEST RESULTS ===")
    for result in test_results:
        if result.get('source_title') or result.get('survival_status'):
            print(f"\nFilm ID {result['film_id']}:")
            print(f"  Source: {result.get('source_title', 'Not found')}")
            print(f"  Type: {result.get('source_type', 'Not found')}")
            print(f"  Status: {result.get('survival_status', 'Not found')}")
            print(f"  Confidence: {result.get('confidence')}")
    
    proceed = input("\nProceed with full batch? (y/n): ")
    
    if proceed.lower() == 'y':
        # Process all films
        all_results = populator.batch_process_films()
        
        # Save to CSV
        populator.save_results_to_csv(all_results)
        
        # Generate summary
        populator.generate_summary_report(all_results)
        
        print("\n✅ Pre-population complete!")
        print("📋 Review 'wikipedia_prepopulated_data.csv' before importing")
        print("🔍 Manually verify high-value entries and 'low confidence' results")
    
    populator.close()