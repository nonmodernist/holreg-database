
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
