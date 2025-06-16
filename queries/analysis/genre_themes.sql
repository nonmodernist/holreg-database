
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
