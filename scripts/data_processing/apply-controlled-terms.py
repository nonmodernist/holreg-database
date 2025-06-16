import sqlite3
import pandas as pd
from datetime import datetime

class FilmTagger:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
    def apply_controlled_terms(self):
        """Apply controlled terms to films based on AFI subject mappings"""
        print("=== Applying Controlled Terms to Films ===")
        
        # Get all films with subjects
        films_query = """
        SELECT id, title, release_year, subjects 
        FROM films 
        WHERE subjects IS NOT NULL
        """
        films_df = pd.read_sql_query(films_query, self.conn)
        
        print(f"Processing {len(films_df)} films...")
        
        # Get all mappings
        mappings_query = """
        SELECT afi_subject, controlled_term_id, confidence_score 
        FROM afi_to_controlled_mapping
        """
        mappings = {}
        for row in self.cursor.execute(mappings_query):
            mappings[row[0]] = (row[1], row[2])  # term_id, confidence
        
        # Process each film
        tagged_count = 0
        total_assignments = 0
        
        for idx, film in films_df.iterrows():
            film_id = film['id']
            subjects = [s.strip() for s in film['subjects'].split('|') if s.strip()]
            
            # Track term assignments for this film
            term_assignments = {}
            
            for subject in subjects:
                if subject in mappings:
                    term_id, confidence = mappings[subject]
                    
                    # Determine relevance weight based on frequency in this film
                    # and confidence of mapping
                    if confidence >= 0.9:
                        relevance = 3 if subjects.count(subject) > 1 else 2
                    else:
                        relevance = 2 if subjects.count(subject) > 1 else 1
                    
                    # Keep highest relevance if term already assigned
                    if term_id not in term_assignments or term_assignments[term_id] < relevance:
                        term_assignments[term_id] = relevance
            
            # Insert term assignments for this film
            if term_assignments:
                for term_id, relevance in term_assignments.items():
                    try:
                        self.cursor.execute("""
                        INSERT INTO film_subjects_controlled 
                        (film_id, term_id, relevance_weight, assignment_type)
                        VALUES (?, ?, ?, 'auto_mapped')
                        """, (film_id, term_id, relevance))
                        total_assignments += 1
                    except sqlite3.IntegrityError:
                        # Already exists, update relevance if higher
                        self.cursor.execute("""
                        UPDATE film_subjects_controlled 
                        SET relevance_weight = MAX(relevance_weight, ?)
                        WHERE film_id = ? AND term_id = ?
                        """, (relevance, film_id, term_id))
                
                tagged_count += 1
            
            if (idx + 1) % 20 == 0:
                print(f"  Processed {idx + 1} films...")
        
        self.conn.commit()
        print(f"\n✅ Tagged {tagged_count} films with {total_assignments} term assignments")
        
        return tagged_count, total_assignments
    
    def generate_analysis_queries(self):
        """Create useful analysis queries"""
        queries = {
            "films_by_theme.sql": """
-- Films grouped by controlled vocabulary themes
SELECT 
    ct.facet,
    ct.term,
    COUNT(DISTINCT fsc.film_id) as film_count,
    GROUP_CONCAT(DISTINCT f.release_year || ': ' || f.title, ', ') as films
FROM film_subjects_controlled fsc
JOIN controlled_terms ct ON fsc.term_id = ct.term_id
JOIN films f ON fsc.film_id = f.id
GROUP BY ct.facet, ct.term
HAVING film_count > 2
ORDER BY ct.facet, film_count DESC;
""",
            
            "decade_analysis.sql": """
-- How themes evolved over decades
SELECT 
    ct.facet,
    ct.term,
    SUM(CASE WHEN f.release_year BETWEEN 1910 AND 1919 THEN 1 ELSE 0 END) as '1910s',
    SUM(CASE WHEN f.release_year BETWEEN 1920 AND 1929 THEN 1 ELSE 0 END) as '1920s',
    SUM(CASE WHEN f.release_year BETWEEN 1930 AND 1939 THEN 1 ELSE 0 END) as '1930s',
    SUM(CASE WHEN f.release_year BETWEEN 1940 AND 1949 THEN 1 ELSE 0 END) as '1940s',
    SUM(CASE WHEN f.release_year BETWEEN 1950 AND 1960 THEN 1 ELSE 0 END) as '1950s',
    COUNT(DISTINCT f.id) as total
FROM films f
JOIN film_subjects_controlled fsc ON f.id = fsc.film_id
JOIN controlled_terms ct ON fsc.term_id = ct.term_id
WHERE ct.facet IN ('Family Relations', 'Social Issues', 'Women-Specific')
GROUP BY ct.facet, ct.term
HAVING total > 3
ORDER BY ct.facet, total DESC;
""",
            
            "author_themes.sql": """
-- Which authors' works were adapted with which themes
SELECT 
    f.literary_credits as author,
    ct.term as theme,
    COUNT(DISTINCT f.id) as film_count,
    GROUP_CONCAT(f.release_year || ': ' || f.title, ' | ') as films
FROM films f
JOIN film_subjects_controlled fsc ON f.id = fsc.film_id
JOIN controlled_terms ct ON fsc.term_id = ct.term_id
WHERE f.literary_credits IS NOT NULL
  AND fsc.relevance_weight >= 2  -- Only significant themes
GROUP BY f.literary_credits, ct.term
HAVING film_count > 1
ORDER BY author, film_count DESC;
""",
            
            "genre_themes.sql": """
-- Themes by genre
SELECT 
    f.genre,
    ct.facet,
    ct.term,
    COUNT(DISTINCT f.id) as film_count
FROM films f
JOIN film_subjects_controlled fsc ON f.id = fsc.film_id
JOIN controlled_terms ct ON fsc.term_id = ct.term_id
WHERE f.genre IS NOT NULL
GROUP BY f.genre, ct.facet, ct.term
HAVING film_count > 2
ORDER BY f.genre, ct.facet, film_count DESC;
""",
            
            "unmapped_subjects.sql": """
-- Films with subjects that couldn't be mapped
SELECT 
    f.id,
    f.title,
    f.release_year,
    f.subjects
FROM films f
WHERE f.subjects IS NOT NULL
  AND f.id NOT IN (
    SELECT DISTINCT film_id 
    FROM film_subjects_controlled
  )
ORDER BY f.release_year;
""",
            
            "co_occurring_themes.sql": """
-- Which themes appear together most often
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
WHERE ct1.facet != ct2.facet  -- Cross-facet relationships
  AND fsc1.relevance_weight >= 2
  AND fsc2.relevance_weight >= 2
GROUP BY ct1.term, ct2.term
HAVING co_occurrences > 3
ORDER BY co_occurrences DESC;
"""
        }
        
        print("\n=== Saving Analysis Queries ===")
        for filename, query in queries.items():
            with open(filename, 'w') as f:
                f.write(query)
            print(f"✅ Saved {filename}")
        
        return queries
    
    def show_sample_results(self):
        """Show a sample of the tagged films"""
        print("\n=== Sample Tagged Films ===")
        
        sample_query = """
        SELECT 
            f.title,
            f.release_year,
            GROUP_CONCAT(ct.facet || ': ' || ct.term, ' | ') as controlled_subjects
        FROM films f
        JOIN film_subjects_controlled fsc ON f.id = fsc.film_id
        JOIN controlled_terms ct ON fsc.term_id = ct.term_id
        WHERE fsc.relevance_weight >= 2
        GROUP BY f.id
        ORDER BY f.release_year DESC
        LIMIT 10
        """
        
        results = pd.read_sql_query(sample_query, self.conn)
        for idx, row in results.iterrows():
            print(f"\n{row['title']} ({row['release_year']})")
            print(f"  {row['controlled_subjects']}")
    
    def close(self):
        self.conn.close()


if __name__ == "__main__":
    db_path = "film_research.db"  # Update this!
    
    tagger = FilmTagger(db_path)
    
    # Apply controlled terms
    tagged_count, total_assignments = tagger.apply_controlled_terms()
    
    # Generate analysis queries
    tagger.generate_analysis_queries()
    
    # Show sample results
    tagger.show_sample_results()
    
    tagger.close()
    
    print("\n🎉 Film tagging complete!")
    print("\n📊 You can now:")
    print("1. Run the generated SQL queries to analyze your data")
    print("2. Open your database in VS Code to explore")
    print("3. Create visualizations of theme patterns")
    print("4. Write about trends in women's adaptations!")