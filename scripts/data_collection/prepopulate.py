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
                based_on = re.sub(r'\[\[(.*?)\|(.*?)\]\]', r'\2', based_on)  # Handle piped links
                based_on = re.sub(r'\[\[(.*?)\]\]', r'\1', based_on)  # Remove wiki links
                based_on = re.sub(r"''(.*?)''", r'\1', based_on)  # Remove italics
                based_on = re.sub(r'\{\{.*?\}\}', '', based_on)  # Remove templates
                based_on = re.sub(r'<.*?>', '', based_on)  # Remove HTML
                based_on = re.sub(r'\n', ' ', based_on)  # Replace newlines
                data['based_on_raw'] = based_on.strip()
                
                # Try to extract title and author
                # Common patterns: "novel by Author", "''Title'' by Author"
                title_author_match = re.search(r'([^by]+?)\s+by\s+(.+)', based_on, re.IGNORECASE)
                if title_author_match:
                    potential_title = title_author_match.group(1).strip()
                    potential_author = title_author_match.group(2).strip()
                    
                    # Clean up title
                    potential_title = re.sub(r'^(novel|story|play|book)\s+', '', potential_title, flags=re.IGNORECASE)
                    potential_title = potential_title.strip(' "\'')
                    
                    if potential_title:
                        data['source_title'] = potential_title
                
                # Try to parse source type
                if re.search(r'\bnovel\b', based_on, re.IGNORECASE):
                    data['source_type'] = 'novel'
                elif re.search(r'\bshort story\b', based_on, re.IGNORECASE):
                    data['source_type'] = 'short story'
                elif re.search(r'\bplay\b', based_on, re.IGNORECASE):
                    data['source_type'] = 'play'
                elif re.search(r'\bstory\b', based_on, re.IGNORECASE):
                    data['source_type'] = 'short story'
                elif re.search(r'\bbook\b', based_on, re.IGNORECASE):
                    data['source_type'] = 'novel'
        
        # Look for preservation/availability info in article
        preservation_section = re.search(r'==\s*(?:Preservation|Availability|Home media)(.*?)(?===|$)', 
                                       content, re.DOTALL | re.IGNORECASE)
        
        if preservation_section:
            pres_content = preservation_section.group(1)
            if 'lost film' in pres_content.lower():
                data['survival_status'] = 'lost'
            elif any(term in pres_content.lower() for term in ['dvd', 'blu-ray', 'streaming', 'restored']):
                data['survival_status'] = 'extant'
        else:
            # General article search
            if 'lost film' in content.lower():
                data['survival_status'] = 'lost'
            elif 'surviving' in content.lower() or 'preserved' in content.lower():
                data['survival_status'] = 'extant'
            # For films after 1950, assume extant unless stated otherwise
            elif not data.get('survival_status'):
                year_match = re.search(r'Release date.*?(\d{4})', content)
                if year_match:
                    year = int(year_match.group(1))
                    if year >= 1950:
                        data['survival_status'] = 'likely extant'
        
        # Extract archive information
        archive_patterns = [
            r'preserved at ([^\.]+)',
            r'held by ([^\.]+)',
            r'archived at ([^\.]+)',
            r'copy at ([^\.]+)',
            r'restored by ([^\.]+)',
            r'Library of Congress',
            r'Academy Film Archive',
            r'UCLA Film [&and]+ Television Archive',
            r'George Eastman Museum',
            r'Museum of Modern Art',
            r'BFI National Archive',
            r'EYE Film Institute'
        ]
        
        archives = []
        for pattern in archive_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            archives.extend(matches)
        
        if archives:
            # Clean up archive names
            cleaned_archives = []
            for archive in archives:
                # Remove trailing punctuation and clean up
                archive = re.sub(r'[,;]+$', '', archive).strip()
                if len(archive) > 3:  # Avoid very short matches
                    cleaned_archives.append(archive)
            
            if cleaned_archives:
                data['archive_holdings'] = ', '.join(list(dict.fromkeys(cleaned_archives)))  # Remove duplicates
        
        return data
    
    def query_wikidata_simple(self, wikidata_id: str) -> Optional[Dict]:
        """Simpler Wikidata query using the API instead of SPARQL"""
        if not wikidata_id:
            return None
        
        params = {
            'action': 'wbgetentities',
            'ids': wikidata_id,
            'format': 'json',
            'languages': 'en',
            'props': 'claims|labels'
        }
        
        try:
            response = requests.get(self.wikidata_api, params=params, headers=self.headers)
            data = response.json()
            
            if 'entities' in data and wikidata_id in data['entities']:
                entity = data['entities'][wikidata_id]
                claims = entity.get('claims', {})
                
                wikidata_info = {}
                
                # P144 - based on
                if 'P144' in claims:
                    based_on_claim = claims['P144'][0]
                    if 'datavalue' in based_on_claim.get('mainsnak', {}):
                        based_on_id = based_on_claim['mainsnak']['datavalue']['value']['id']
                        # Get the label for this item
                        based_on_info = self.get_wikidata_label(based_on_id)
                        if based_on_info:
                            wikidata_info['source_title'] = based_on_info
                
                # P437 - distribution format
                if 'P437' in claims:
                    formats = []
                    for format_claim in claims['P437']:
                        if 'datavalue' in format_claim.get('mainsnak', {}):
                            format_id = format_claim['mainsnak']['datavalue']['value']['id']
                            format_label = self.get_wikidata_label(format_id)
                            if format_label:
                                formats.append(format_label)
                    
                    if formats:
                        wikidata_info['viewing_format'] = ', '.join(formats)
                        # If has modern formats, likely extant
                        if any(f.lower() in ['dvd', 'blu-ray', 'streaming'] for f in formats):
                            wikidata_info['survival_status'] = 'extant'
                
                # P1651 - video URL (indicates availability)
                if 'P1651' in claims:
                    wikidata_info['archive_url'] = claims['P1651'][0]['mainsnak']['datavalue']['value']
                    wikidata_info['survival_status'] = 'extant'
                
                return wikidata_info
        
        except Exception as e:
            print(f"Error querying Wikidata for {wikidata_id}: {e}")
        
        return None
    
    def get_wikidata_label(self, item_id: str) -> Optional[str]:
        """Get the English label for a Wikidata item"""
        params = {
            'action': 'wbgetentities',
            'ids': item_id,
            'format': 'json',
            'languages': 'en',
            'props': 'labels'
        }
        
        try:
            response = requests.get(self.wikidata_api, params=params, headers=self.headers)
            data = response.json()
            
            if 'entities' in data and item_id in data['entities']:
                entity = data['entities'][item_id]
                if 'labels' in entity and 'en' in entity['labels']:
                    return entity['labels']['en']['value']
        except:
            pass
        
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
                wikidata_info = self.query_wikidata_simple(wiki_data['wikidata_id'])
                if wikidata_info:
                    # Only update if we don't already have the info
                    for key, value in wikidata_info.items():
                        if key not in results or not results[key]:
                            results[key] = value
                    results['confidence'] = 'high'
                    results['sources'].append('wikidata')
            
            results['sources'].append('wikipedia')
            
            # Try to match with literary credits
            if literary_credits:
                primary_author = literary_credits.split('|')[0].strip()
                # Check if author is mentioned in the based_on text
                if 'based_on_raw' in results and primary_author.lower() in results['based_on_raw'].lower():
                    results['confidence'] = 'high'
                # Also check if we found a source title
                if results.get('source_title'):
                    results['confidence'] = 'high'
        
        # For films from 1950s-1960s, default to likely extant if no status found
        if not results.get('survival_status') and year >= 1950:
            results['survival_status'] = 'likely extant'
            results['survival_notes'] = 'Post-1950 film, preservation likely'
        
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
                'viewing_format', 'archive_url', 'survival_notes'
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
            print(f"  Archives: {result.get('archive_holdings', 'Not found')}")
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