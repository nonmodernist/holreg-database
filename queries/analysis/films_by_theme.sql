
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
