#!/usr/bin/env python3
"""
Export Hollywood Adaptations database to JSON for static site generation
Updated to handle normalized database structure with junction tables
"""

import sqlite3
import json
import os
from pathlib import Path
from datetime import datetime
from collections import defaultdict

class DatabaseToJsonExporter:
    def __init__(self, db_path='data/databases/holreg_research.db', output_dir='site/data'):
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def export_all(self):
        """Export all data needed for the static site"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Access columns by name
        
        print("Starting export process...")
        
        # Check if we have normalized tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='people'")
        has_normalized = cursor.fetchone() is not None
        
        if has_normalized:
            print("Detected normalized database structure")
            self.export_films_normalized(conn)
            self.export_people(conn)
        else:
            print("Using original database structure")
            self.export_films(conn)
        
        # These remain the same
        self.export_authors(conn)
        self.export_controlled_vocabulary(conn)
        self.export_themes_analysis(conn)
        self.export_search_index(conn)
        self.export_site_metadata(conn)
        
        conn.close()
        print(f"\nExport complete! Files saved to {self.output_dir}")
    
    def export_films_normalized(self, conn):
        """Export films with normalized crew data"""
        cursor = conn.cursor()
        
        # Get all films with their normalized credits
        cursor.execute("""
            SELECT 
                f.*,
                -- Get directors as JSON array
                (SELECT json_group_array(
                    json_object('name', p.name, 'position', fd.position)
                )
                FROM film_directors fd
                JOIN people p ON fd.person_id = p.person_id
                WHERE fd.film_id = f.id
                ORDER BY fd.position) as directors_json,
                
                -- Get writers as JSON array
                (SELECT json_group_array(
                    json_object('name', p.name, 'position', fw.position)
                )
                FROM film_writers fw
                JOIN people p ON fw.person_id = p.person_id
                WHERE fw.film_id = f.id
                ORDER BY fw.position) as writers_json,
                
                -- Get producers as JSON array
                (SELECT json_group_array(
                    json_object('name', p.name, 'position', fp.position)
                )
                FROM film_producers fp
                JOIN people p ON fp.person_id = p.person_id
                WHERE fp.film_id = f.id
                ORDER BY fp.position) as producers_json
            FROM films f
            ORDER BY f.release_year, f.title
        """)
        
        films = []
        for row in cursor:
            film = dict(row)
            
            # Parse JSON fields
            if film['directors_json']:
                directors_data = json.loads(film['directors_json'])
                # Provide both formats for flexibility
                film['directors'] = directors_data
                film['director'] = ' & '.join([d['name'] for d in directors_data])
            else:
                film['directors'] = []
                film['director'] = film.get('director')  # Fallback to original field
            
            if film['writers_json']:
                writers_data = json.loads(film['writers_json'])
                film['writers'] = writers_data
                film['writer'] = ' & '.join([w['name'] for w in writers_data])
            else:
                film['writers'] = []
                film['writer'] = film.get('writer')
            
            if film['producers_json']:
                producers_data = json.loads(film['producers_json'])
                film['producers'] = producers_data
                film['producer'] = ' & '.join([p['name'] for p in producers_data])
            else:
                film['producers'] = []
                film['producer'] = film.get('producer')
            
            # Remove the JSON fields
            film.pop('directors_json', None)
            film.pop('writers_json', None)
            film.pop('producers_json', None)
            
            # Get controlled subjects
            subject_cursor = conn.cursor()
            subject_cursor.execute("""
                SELECT 
                    ct.term,
                    ct.facet,
                    fsc.relevance_weight as weight
                FROM film_subjects_controlled fsc
                JOIN controlled_terms ct ON fsc.term_id = ct.term_id
                WHERE fsc.film_id = ?
                ORDER BY fsc.relevance_weight DESC, ct.term
            """, (film['id'],))
            
            film['controlled_subjects'] = [
                {'term': s['term'], 'facet': s['facet'], 'weight': s['weight']}
                for s in subject_cursor
            ]
            
            # Handle multiple authors if they exist
            if film.get('literary_credits') and '|' in film['literary_credits']:
                film['authors'] = [a.strip() for a in film['literary_credits'].split('|')]
            else:
                film['authors'] = [film['literary_credits']] if film.get('literary_credits') else []
            
            # Clean up None values
            film = {k: v for k, v in film.items() if v is not None}
            
            films.append(film)
        
        # Save full dataset
        self._save_json(films, 'films.json')
        
        # Also create smaller files by decade
        by_decade = defaultdict(list)
        for film in films:
            decade = (film['release_year'] // 10) * 10
            by_decade[decade].append(film)
        
        for decade, decade_films in by_decade.items():
            self._save_json(decade_films, f'films_{decade}s.json')
        
        print(f"Exported {len(films)} films (normalized structure)")
    
    def export_films(self, conn):
        """Export films with original structure (fallback)"""
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM films
            ORDER BY release_year, title
        """)
        
        films = []
        for row in cursor:
            film = dict(row)
            
            # Get controlled subjects
            subject_cursor = conn.cursor()
            subject_cursor.execute("""
                SELECT 
                    ct.term,
                    ct.facet,
                    fsc.relevance_weight as weight
                FROM film_subjects_controlled fsc
                JOIN controlled_terms ct ON fsc.term_id = ct.term_id
                WHERE fsc.film_id = ?
                ORDER BY fsc.relevance_weight DESC, ct.term
            """, (film['id'],))
            
            film['controlled_subjects'] = [
                {'term': s['term'], 'facet': s['facet'], 'weight': s['weight']}
                for s in subject_cursor
            ]
            
            # Clean up None values
            film = {k: v for k, v in film.items() if v is not None}
            
            films.append(film)
        
        self._save_json(films, 'films.json')
        
        by_decade = defaultdict(list)
        for film in films:
            decade = (film['release_year'] // 10) * 10
            by_decade[decade].append(film)
        
        for decade, decade_films in by_decade.items():
            self._save_json(decade_films, f'films_{decade}s.json')
        
        print(f"Exported {len(films)} films")
    
    def export_people(self, conn):
        """Export people data (directors, writers, producers)"""
        cursor = conn.cursor()
        
        # Check if people table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='people'")
        if not cursor.fetchone():
            print("Skipping people export (table not found)")
            return
        
        cursor.execute("""
            SELECT 
                p.person_id,
                p.name,
                COUNT(DISTINCT fd.film_id) as films_directed,
                COUNT(DISTINCT fw.film_id) as films_written,
                COUNT(DISTINCT fp.film_id) as films_produced,
                MIN(COALESCE(fd_films.min_year, COALESCE(fw_films.min_year, fp_films.min_year))) as first_credit,
                MAX(COALESCE(fd_films.max_year, COALESCE(fw_films.max_year, fp_films.max_year))) as last_credit
            FROM people p
            LEFT JOIN film_directors fd ON p.person_id = fd.person_id
            LEFT JOIN film_writers fw ON p.person_id = fw.person_id
            LEFT JOIN film_producers fp ON p.person_id = fp.person_id
            LEFT JOIN (
                SELECT fd.person_id, MIN(f.release_year) as min_year, MAX(f.release_year) as max_year
                FROM film_directors fd
                JOIN films f ON fd.film_id = f.id
                GROUP BY fd.person_id
            ) fd_films ON p.person_id = fd_films.person_id
            LEFT JOIN (
                SELECT fw.person_id, MIN(f.release_year) as min_year, MAX(f.release_year) as max_year
                FROM film_writers fw
                JOIN films f ON fw.film_id = f.id
                GROUP BY fw.person_id
            ) fw_films ON p.person_id = fw_films.person_id
            LEFT JOIN (
                SELECT fp.person_id, MIN(f.release_year) as min_year, MAX(f.release_year) as max_year
                FROM film_producers fp
                JOIN films f ON fp.film_id = f.id
                GROUP BY fp.person_id
            ) fp_films ON p.person_id = fp_films.person_id
            GROUP BY p.person_id
            HAVING (films_directed + films_written + films_produced) > 0
            ORDER BY (films_directed + films_written + films_produced) DESC
        """)
        
        people = []
        for row in cursor:
            person = dict(row)
            
            # Get filmography
            film_cursor = conn.cursor()
            film_cursor.execute("""
                SELECT 
                    f.id,
                    f.title,
                    f.release_year,
                    'Director' as role,
                    fd.position
                FROM film_directors fd
                JOIN films f ON fd.film_id = f.id
                WHERE fd.person_id = ?
                
                UNION ALL
                
                SELECT 
                    f.id,
                    f.title,
                    f.release_year,
                    'Writer' as role,
                    fw.position
                FROM film_writers fw
                JOIN films f ON fw.film_id = f.id
                WHERE fw.person_id = ?
                
                UNION ALL
                
                SELECT 
                    f.id,
                    f.title,
                    f.release_year,
                    'Producer' as role,
                    fp.position
                FROM film_producers fp
                JOIN films f ON fp.film_id = f.id
                WHERE fp.person_id = ?
                
                ORDER BY release_year, title
            """, (person['person_id'], person['person_id'], person['person_id']))
            
            filmography = defaultdict(list)
            for film in film_cursor:
                filmography[film['role']].append({
                    'id': film['id'],
                    'title': film['title'],
                    'year': film['release_year'],
                    'position': film['position']
                })
            
            person['filmography'] = dict(filmography)
            person['total_films'] = len(set(
                f['id'] for role_films in filmography.values() 
                for f in role_films
            ))
            
            people.append(person)
        
        self._save_json(people, 'people.json')
        print(f"Exported {len(people)} people")
    
    def export_authors(self, conn):
        """Export author data with their adaptations"""
        cursor = conn.cursor()
        
        # Check if we have normalized authors table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='authors'")
        has_authors_table = cursor.fetchone() is not None
        
        if has_authors_table:
            # Use normalized structure
            cursor.execute("""
                SELECT 
                    a.author_id,
                    a.name as author,
                    COUNT(DISTINCT fa.film_id) as adaptation_count,
                    MIN(f.release_year) as first_adaptation,
                    MAX(f.release_year) as last_adaptation
                FROM authors a
                JOIN film_authors fa ON a.author_id = fa.author_id
                JOIN films f ON fa.film_id = f.id
                GROUP BY a.author_id
                ORDER BY adaptation_count DESC, a.name
            """)
        else:
            # Use original structure
            cursor.execute("""
                SELECT 
                    literary_credits as author,
                    COUNT(*) as adaptation_count,
                    MIN(release_year) as first_adaptation,
                    MAX(release_year) as last_adaptation
                FROM films
                WHERE literary_credits IS NOT NULL AND literary_credits != ''
                GROUP BY literary_credits
                ORDER BY adaptation_count DESC, author
            """)
        
        authors = []
        for row in cursor:
            author = row['author']
            
            # Get all films for this author
            if has_authors_table:
                films_cursor = conn.cursor()
                films_cursor.execute("""
                    SELECT f.id, f.title, f.release_year, f.survival_status
                    FROM films f
                    JOIN film_authors fa ON f.id = fa.film_id
                    WHERE fa.author_id = ?
                    ORDER BY f.release_year
                """, (row['author_id'],))
            else:
                films_cursor = conn.cursor()
                films_cursor.execute("""
                    SELECT id, title, release_year, survival_status
                    FROM films
                    WHERE literary_credits = ?
                    ORDER BY release_year
                """, (author,))
            
            films = [
                {
                    'id': f['id'],
                    'title': f['title'],
                    'year': f['release_year'],
                    'survival_status': f['survival_status']
                }
                for f in films_cursor
            ]
            
            author_data = {
                'name': author,
                'adaptation_count': row['adaptation_count'],
                'first_adaptation': row['first_adaptation'],
                'last_adaptation': row['last_adaptation'],
                'year_span': row['last_adaptation'] - row['first_adaptation'] if row['last_adaptation'] and row['first_adaptation'] else 0,
                'films': films
            }
            authors.append(author_data)
        
        self._save_json(authors, 'authors.json')
        print(f"Exported {len(authors)} authors")
    
    def export_controlled_vocabulary(self, conn):
        """Export controlled vocabulary structure"""
        cursor = conn.cursor()
        
        # Get vocabulary organized by facet
        cursor.execute("""
            SELECT 
                ct.facet,
                ct.term,
                COUNT(DISTINCT fsc.film_id) as usage_count
            FROM controlled_terms ct
            LEFT JOIN film_subjects_controlled fsc ON ct.term_id = fsc.term_id
            GROUP BY ct.term_id
            ORDER BY ct.facet, ct.term
        """)
        
        vocabulary = defaultdict(list)
        for row in cursor:
            row_dict = dict(row)
            term_data = {
                'term': row_dict['term'],
                'usage_count': row_dict['usage_count']
            }
            vocabulary[row_dict['facet']].append(term_data)
        
        vocab_list = []
        for facet, terms in vocabulary.items():
            vocab_list.append({
                'facet': facet,
                'terms': terms,
                'term_count': len(terms),
                'total_usage': sum(t['usage_count'] for t in terms)
            })
        
        self._save_json(vocab_list, 'controlled_vocabulary.json')
        print(f"Exported {len(vocab_list)} facets with {sum(f['term_count'] for f in vocab_list)} terms")
    
    def export_themes_analysis(self, conn):
        """Export pre-computed theme analysis"""
        cursor = conn.cursor()
        
        # Theme frequency by decade
        cursor.execute("""
            SELECT 
                ct.facet,
                ct.term,
                SUM(CASE WHEN f.release_year BETWEEN 1910 AND 1919 THEN 1 ELSE 0 END) as decade_1910s,
                SUM(CASE WHEN f.release_year BETWEEN 1920 AND 1929 THEN 1 ELSE 0 END) as decade_1920s,
                SUM(CASE WHEN f.release_year BETWEEN 1930 AND 1939 THEN 1 ELSE 0 END) as decade_1930s,
                SUM(CASE WHEN f.release_year BETWEEN 1940 AND 1949 THEN 1 ELSE 0 END) as decade_1940s,
                SUM(CASE WHEN f.release_year BETWEEN 1950 AND 1960 THEN 1 ELSE 0 END) as decade_1950s,
                COUNT(DISTINCT f.id) as total
            FROM films f
            JOIN film_subjects_controlled fsc ON f.id = fsc.film_id
            JOIN controlled_terms ct ON fsc.term_id = ct.term_id
            WHERE fsc.relevance_weight >= 2
            GROUP BY ct.facet, ct.term
            HAVING total > 2
            ORDER BY ct.facet, total DESC
        """)
        
        themes_by_decade = []
        for row in cursor:
            themes_by_decade.append({
                'facet': row['facet'],
                'term': row['term'],
                'decades': {
                    '1910s': row['decade_1910s'],
                    '1920s': row['decade_1920s'],
                    '1930s': row['decade_1930s'],
                    '1940s': row['decade_1940s'],
                    '1950s': row['decade_1950s']
                },
                'total': row['total']
            })
        
        # Co-occurring themes
        cursor.execute("""
            SELECT 
                ct1.term as theme1,
                ct2.term as theme2,
                COUNT(DISTINCT fsc1.film_id) as co_occurrences
            FROM film_subjects_controlled fsc1
            JOIN film_subjects_controlled fsc2 
                ON fsc1.film_id = fsc2.film_id 
                AND fsc1.term_id < fsc2.term_id
            JOIN controlled_terms ct1 ON fsc1.term_id = ct1.term_id
            JOIN controlled_terms ct2 ON fsc2.term_id = ct2.term_id
            WHERE ct1.facet != ct2.facet
                AND fsc1.relevance_weight >= 2
                AND fsc2.relevance_weight >= 2
            GROUP BY ct1.term, ct2.term
            HAVING co_occurrences > 3
            ORDER BY co_occurrences DESC
            LIMIT 50
        """)
        
        co_occurring = []
        for row in cursor:
            co_occurring.append({
                'theme1': row['theme1'],
                'theme2': row['theme2'],
                'count': row['co_occurrences']
            })
        
        analysis = {
            'themes_by_decade': themes_by_decade,
            'co_occurring_themes': co_occurring
        }
        
        self._save_json(analysis, 'themes_analysis.json')
        print(f"Exported analysis with {len(themes_by_decade)} themes")
    
    def export_search_index(self, conn):
        """Create a search index for client-side searching"""
        cursor = conn.cursor()
        
        # Check if we have normalized structure
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='people'")
        has_normalized = cursor.fetchone() is not None
        
        if has_normalized:
            cursor.execute("""
                SELECT 
                    f.id,
                    f.title,
                    f.release_year,
                    f.literary_credits,
                    (SELECT GROUP_CONCAT(p.name, ' ')
                     FROM film_directors fd
                     JOIN people p ON fd.person_id = p.person_id
                     WHERE fd.film_id = f.id) as directors,
                    (SELECT GROUP_CONCAT(p.name, ' ')
                     FROM film_writers fw
                     JOIN people p ON fw.person_id = p.person_id
                     WHERE fw.film_id = f.id) as writers,
                    f.subjects,
                    GROUP_CONCAT(ct.term, ' ') as controlled_terms
                FROM films f
                LEFT JOIN film_subjects_controlled fsc ON f.id = fsc.film_id
                LEFT JOIN controlled_terms ct ON fsc.term_id = ct.term_id
                GROUP BY f.id
            """)
        else:
            cursor.execute("""
                SELECT 
                    f.id,
                    f.title,
                    f.release_year,
                    f.literary_credits,
                    f.director,
                    f.writer,
                    f.subjects,
                    GROUP_CONCAT(ct.term, ' ') as controlled_terms
                FROM films f
                LEFT JOIN film_subjects_controlled fsc ON f.id = fsc.film_id
                LEFT JOIN controlled_terms ct ON fsc.term_id = ct.term_id
                GROUP BY f.id
            """)
        
        search_index = []
        for row in cursor:
            # Convert Row to dict to use .get()
            row_dict = dict(row)
            
            # Create searchable text
            searchable_parts = [
                row_dict['title'],
                str(row_dict['release_year']),
                row_dict['literary_credits'],
                row_dict.get('directors') or row_dict.get('director'),
                row_dict.get('writers') or row_dict.get('writer'),
                row_dict['subjects'],
                row_dict['controlled_terms']
            ]
            
            searchable = ' '.join(filter(None, searchable_parts)).lower()
            
            search_index.append({
                'id': row_dict['id'],
                'title': row_dict['title'],
                'year': row_dict['release_year'],
                'author': row_dict['literary_credits'],
                'searchable': searchable
            })
        
        self._save_json(search_index, 'search_index.json')
        print(f"Created search index with {len(search_index)} entries")
    
    def export_site_metadata(self, conn):
        """Export metadata about the dataset"""
        cursor = conn.cursor()
        
        # Get statistics
        cursor.execute("SELECT COUNT(*) FROM films")
        total_films = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT literary_credits) FROM films WHERE literary_credits IS NOT NULL")
        total_authors = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(release_year), MAX(release_year) FROM films")
        year_range = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*) FROM controlled_terms")
        total_terms = cursor.fetchone()[0]
        
        # Check for people table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='people'")
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM people")
            total_people = cursor.fetchone()[0]
        else:
            total_people = None
        
        metadata = {
            'title': 'Hollywood Adaptations of American Women Writers',
            'subtitle': 'Film Adaptations Database (1910-1960)',
            'generated': datetime.now().isoformat(),
            'statistics': {
                'total_films': total_films,
                'total_authors': total_authors,
                'year_range': {
                    'start': year_range[0],
                    'end': year_range[1]
                },
                'total_controlled_terms': total_terms
            },
            'last_updated': datetime.now().strftime('%B %d, %Y')
        }
        
        if total_people:
            metadata['statistics']['total_people'] = total_people
        
        self._save_json(metadata, 'metadata.json')
        print("Exported site metadata")
    
    def _save_json(self, data, filename):
        """Save data as JSON with pretty printing"""
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Export database to JSON for static site')
    parser.add_argument('--db', default='data/databases/holreg_research.db',
                        help='Path to database file')
    parser.add_argument('--output', default='site/data',
                        help='Output directory for JSON files')
    
    args = parser.parse_args()
    
    exporter = DatabaseToJsonExporter(args.db, args.output)
    exporter.export_all()