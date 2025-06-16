
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
