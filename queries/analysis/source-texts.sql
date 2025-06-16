-- ============================================
-- QUERIES TO FIND FILMS FROM SOURCE TEXTS
-- ============================================

-- 1. Find all films adapted from a specific source text
SELECT 
    st.title AS source_title,
    st.author,
    st.publication_year,
    f.title AS film_title,
    f.release_year,
    f.director,
    f.survival_status
FROM source_texts st
JOIN film_sources fs ON st.source_id = fs.source_id
JOIN films f ON fs.film_id = f.id
WHERE st.title = 'Uncle Tom''s Cabin'
ORDER BY f.release_year;

-- 2. Show all source texts with their film adaptations count
SELECT 
    st.title,
    st.author,
    st.publication_year,
    st.type,
    COUNT(fs.film_id) AS num_adaptations,
    GROUP_CONCAT(f.release_year || ': ' || f.title, ' | ') AS films
FROM source_texts st
LEFT JOIN film_sources fs ON st.source_id = fs.source_id
LEFT JOIN films f ON fs.film_id = f.id
GROUP BY st.source_id
ORDER BY num_adaptations DESC, st.author, st.title;

-- 3. Find source texts with multiple adaptations
SELECT 
    st.title AS source_title,
    st.author,
    st.publication_year,
    COUNT(fs.film_id) AS adaptation_count,
    MIN(f.release_year) AS first_adaptation,
    MAX(f.release_year) AS last_adaptation,
    (MAX(f.release_year) - MIN(f.release_year)) AS year_span
FROM source_texts st
JOIN film_sources fs ON st.source_id = fs.source_id
JOIN films f ON fs.film_id = f.id
GROUP BY st.source_id
HAVING adaptation_count > 1
ORDER BY adaptation_count DESC;

-- 4. Timeline of adaptations for a specific source
SELECT 
    st.title AS source_title,
    f.title AS film_title,
    f.release_year,
    f.director,
    f.survival_status,
    f.viewing_format,
    LAG(f.release_year) OVER (ORDER BY f.release_year) AS prev_adaptation_year,
    f.release_year - LAG(f.release_year) OVER (ORDER BY f.release_year) AS years_since_last
FROM source_texts st
JOIN film_sources fs ON st.source_id = fs.source_id
JOIN films f ON fs.film_id = f.id
WHERE st.title = 'Mrs. Wiggs of the Cabbage Patch'
ORDER BY f.release_year;

-- 5. Source texts by author with adaptation details
SELECT 
    st.author,
    COUNT(DISTINCT st.source_id) AS num_source_texts,
    COUNT(DISTINCT fs.film_id) AS total_adaptations,
    GROUP_CONCAT(DISTINCT st.title) AS source_titles
FROM source_texts st
LEFT JOIN film_sources fs ON st.source_id = fs.source_id
GROUP BY st.author
HAVING total_adaptations > 0
ORDER BY total_adaptations DESC;

-- 6. Create a view for easy access to film-source relationships
CREATE VIEW IF NOT EXISTS v_film_adaptations AS
SELECT 
    f.id AS film_id,
    f.title AS film_title,
    f.release_year,
    f.director,
    f.survival_status,
    f.viewing_format,
    st.source_id,
    st.title AS source_title,
    st.author AS source_author,
    st.publication_year AS source_year,
    st.type AS source_type,
    fs.relationship_type
FROM films f
JOIN film_sources fs ON f.id = fs.film_id
JOIN source_texts st ON fs.source_id = st.source_id;

-- Then you can use the view simply:
-- SELECT * FROM v_film_adaptations WHERE source_title = 'Freckles';

-- 7. Find "adaptation families" - source texts adapted multiple times
WITH adaptation_families AS (
    SELECT 
        st.source_id,
        st.title,
        st.author,
        COUNT(fs.film_id) AS adaptation_count,
        GROUP_CONCAT(f.release_year || ': ' || f.title, '\n') AS all_films
    FROM source_texts st
    JOIN film_sources fs ON st.source_id = fs.source_id
    JOIN films f ON fs.film_id = f.id
    GROUP BY st.source_id
    HAVING adaptation_count >= 3
)
SELECT * FROM adaptation_families
ORDER BY adaptation_count DESC;

-- 8. Check which source texts have NO film adaptations (orphaned sources)
SELECT 
    st.title,
    st.author,
    st.publication_year,
    st.notes
FROM source_texts st
LEFT JOIN film_sources fs ON st.source_id = fs.source_id
WHERE fs.film_id IS NULL
ORDER BY st.author, st.title;