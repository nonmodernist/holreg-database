-- Add source text information columns
ALTER TABLE films ADD COLUMN source_title TEXT;
ALTER TABLE films ADD COLUMN source_type TEXT; -- 'novel', 'short story', 'play', 'poem', 'essay', 'memoir'
ALTER TABLE films ADD COLUMN source_year INTEGER;
ALTER TABLE films ADD COLUMN source_publisher TEXT;
ALTER TABLE films ADD COLUMN source_notes TEXT; -- for additional context

-- Add survival status information
ALTER TABLE films ADD COLUMN survival_status TEXT; -- 'extant', 'partially lost', 'lost', 'unknown'
ALTER TABLE films ADD COLUMN survival_notes TEXT; -- details about what survives
ALTER TABLE films ADD COLUMN archive_holdings TEXT; -- which archives have copies
ALTER TABLE films ADD COLUMN viewing_format TEXT; -- '35mm', '16mm', 'digital', 'DVD', etc.
ALTER TABLE films ADD COLUMN last_verified DATE; -- when survival status was last checked

-- Create an authors table for normalized data
CREATE TABLE IF NOT EXISTS authors (
    author_id INTEGER PRIMARY KEY AUTOINCREMENT,
    author_name TEXT NOT NULL,
    birth_year INTEGER,
    death_year INTEGER,
    author_notes TEXT,
    UNIQUE(author_name)
);

-- Create source_texts table for normalized source information
CREATE TABLE IF NOT EXISTS source_texts (
    source_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    author_id INTEGER,
    publication_year INTEGER,
    source_type TEXT,
    publisher TEXT,
    notes TEXT,
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);

-- Link films to source texts (many-to-many for anthologies)
CREATE TABLE IF NOT EXISTS film_sources (
    film_id INTEGER,
    source_id INTEGER,
    adaptation_notes TEXT, -- how faithful, what changed, etc.
    PRIMARY KEY (film_id, source_id),
    FOREIGN KEY (film_id) REFERENCES films(id),
    FOREIGN KEY (source_id) REFERENCES source_texts(source_id)
);