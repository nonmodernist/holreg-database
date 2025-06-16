
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
