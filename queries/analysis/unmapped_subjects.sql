
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
