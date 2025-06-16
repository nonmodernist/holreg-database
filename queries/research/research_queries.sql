-- ========================================
-- RESEARCH QUERIES FOR FILM ADAPTATIONS
-- ========================================

SELECT * FROM films;

-- 1. Films by decade with author info
SELECT 
    (release_year / 10) * 10 as decade,
    COUNT(*) as films,
    GROUP_CONCAT(DISTINCT literary_credits) as authors
FROM films
GROUP BY decade;

-- 2. Most adapted authors
SELECT 
    literary_credits,
    COUNT(*) as num_adaptations
FROM films
WHERE literary_credits IS NOT NULL
GROUP BY literary_credits
ORDER BY num_adaptations DESC;

-- 3. Find films with specific themes
SELECT title, release_year, subjects
FROM films
WHERE subjects LIKE '%mother%' 
   OR subjects LIKE '%daughter%';

-- 4. Geographic distribution
SELECT 
    filming_location,
    COUNT(*) as num_films
FROM films
WHERE filming_location IS NOT NULL
GROUP BY filming_location
ORDER BY num_films DESC;


