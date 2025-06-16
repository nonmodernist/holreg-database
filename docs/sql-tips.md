# SQL & SQLite Guide for VS Code

## Setting Up VS Code for SQLite

### 1. Install the SQLite Extension
1. Open VS Code
2. Go to Extensions (Ctrl+Shift+X or Cmd+Shift+X)
3. Search for **"SQLite"** by alexcvzz
4. Install it

### 2. Open Your Database
1. Open your project folder in VS Code
2. Find your `.db` or `.sqlite` file in the file explorer
3. Right-click on it and select "Open Database"
4. You'll see a new "SQLITE EXPLORER" panel in the sidebar

## SQL Basics - Think of it Like Asking Questions

SQL is just a way to ask questions about your data. Here are the main commands:

### SELECT - Looking at Data
```sql
-- This is a comment in SQL

-- See everything in a table
SELECT * FROM films;

-- See specific columns
SELECT title, year, director FROM films;

-- Filter with WHERE
SELECT title, year 
FROM films 
WHERE year > 1940;

-- Sort results
SELECT title, year 
FROM films 
ORDER BY year DESC;  -- DESC = newest first

-- Limit results
SELECT * FROM films LIMIT 10;
```

### Understanding Your Database Structure
```sql
-- See all tables in your database
SELECT name FROM sqlite_master WHERE type='table';

-- See columns in a specific table
PRAGMA table_info(films);
```

## Working with Your Film Data

### Basic Queries for Your Research

```sql
-- Count films by decade
SELECT 
    (year / 10) * 10 as decade,
    COUNT(*) as num_films
FROM films
GROUP BY decade
ORDER BY decade;

-- Find all films with a specific subject
SELECT title, year, subjects
FROM films
WHERE subjects LIKE '%African Americans%';

-- Find films by a specific author
SELECT title, year, literary_credits
FROM films
WHERE literary_credits LIKE '%Harriet Beecher Stowe%';
```

### Creating New Tables
```sql
-- Basic table creation pattern
CREATE TABLE table_name (
    column_name DATA_TYPE,
    another_column DATA_TYPE
);

-- Real example for your project
CREATE TABLE authors (
    author_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    birth_year INTEGER,
    death_year INTEGER,
    notes TEXT
);
```

### Adding Data
```sql
-- Insert a single row
INSERT INTO authors (name, birth_year, death_year) 
VALUES ('Edith Wharton', 1862, 1937);

-- Insert multiple rows
INSERT INTO authors (name, birth_year, death_year) VALUES
    ('Willa Cather', 1873, 1947),
    ('Zora Neale Hurston', 1891, 1960),
    ('Pearl S. Buck', 1892, 1973);
```

### Connecting Tables (JOINs)
```sql
-- If you have films and authors tables, connect them
SELECT 
    f.title,
    f.year,
    a.name as author_name,
    a.birth_year
FROM films f
JOIN authors a ON f.author_id = a.author_id;
```

## Practical Workflow in VS Code

### 1. Exploring Your Database
```sql
-- First, see what tables you have
.tables

-- Or
SELECT name FROM sqlite_master WHERE type='table';

-- Pick a table and explore its structure
PRAGMA table_info(your_table_name);

-- See sample data
SELECT * FROM your_table_name LIMIT 5;
```

### 2. Running Queries
1. Open a new file with `.sql` extension
2. Write your query
3. Highlight the query text
4. Right-click → "Run Selected Query"
5. Results appear in the OUTPUT panel

### 3. Saving Useful Queries
Create a file called `research_queries.sql`:

```sql
-- ========================================
-- RESEARCH QUERIES FOR FILM ADAPTATIONS
-- ========================================

-- 1. Films by decade with author info
SELECT 
    (year / 10) * 10 as decade,
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
SELECT title, year, subjects
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
```

## Python + SQL in VS Code

You can also work with your SQLite database using Python:

```python
import sqlite3
import pandas as pd

# Connect to your database
conn = sqlite3.connect('your_database.db')

# Read data into a DataFrame
df = pd.read_sql_query("SELECT * FROM films", conn)

# Do Python analysis
films_by_year = df.groupby('year').size()

# Write new data back
new_data.to_sql('new_table', conn, if_exists='replace')

# Always close the connection
conn.close()
```

## Common Tasks for Your Project

### 1. Split the Subjects Column
```sql
-- Create a temporary table with split subjects
WITH split_subjects AS (
    SELECT 
        film_id,
        title,
        TRIM(value) as subject
    FROM films
    CROSS JOIN json_each('["' || REPLACE(subjects, '|', '","') || '"]')
    WHERE subjects IS NOT NULL
)
SELECT subject, COUNT(*) as count
FROM split_subjects
GROUP BY subject
ORDER BY count DESC;
```

### 2. Update Data
```sql
-- Fix a typo in an author name
UPDATE films
SET literary_credits = 'Harriet Beecher Stowe'
WHERE literary_credits = 'Harriet Beacher Stowe';

-- Add a new column
ALTER TABLE films ADD COLUMN adaptation_fidelity TEXT;
```

### 3. Create a Backup
```sql
-- In VS Code terminal
cp your_database.db your_database_backup_2024_01.db
```

## Tips for Learning

1. **Start Simple**: Begin with SELECT queries before moving to CREATE/UPDATE
2. **Use LIMIT**: Always add `LIMIT 10` when testing queries
3. **Save Everything**: Keep a file of working queries
4. **Comment Your Code**: Explain what each query does
5. **Practice on Copies**: Work on a backup of your database

## Next Steps

1. Open your database in VS Code
2. Run `SELECT * FROM films LIMIT 5;` to see your data
3. Try modifying the query to show only films from the 1920s
4. Let me know what you discover and what questions you have!

## Keyboard Shortcuts in VS Code
- **Ctrl+Enter**: Run selected query
- **Ctrl+Shift+E**: Focus on Explorer
- **Ctrl+`**: Open terminal
- **F2**: Rename symbol