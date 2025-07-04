#!/usr/bin/env python3
"""
Generate individual static HTML pages for each film and author
Creates SEO-friendly pages with proper metadata
"""

import json
import os
from pathlib import Path
from datetime import datetime
import re

class StaticPageGenerator:
    def __init__(self, data_dir='site/data', output_dir='site'):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.template_cache = {}
        
    def generate_all_pages(self):
        """Generate all static pages"""
        print("Generating static pages...")
        
        # Create CSS directory
        css_dir = self.output_dir / 'css'
        css_dir.mkdir(parents=True, exist_ok=True)
        
        # Load data
        films = self.load_json('films.json')
        authors = self.load_json('authors.json')
        metadata = self.load_json('metadata.json')
        
        # Create directories
        (self.output_dir / 'films').mkdir(parents=True, exist_ok=True)
        (self.output_dir / 'authors').mkdir(parents=True, exist_ok=True)
        
        # Generate pages
        self.generate_film_pages(films, metadata)
        self.generate_author_pages(authors, films, metadata)
        self.generate_index_pages(films, authors, metadata)
        
        print(f"Generated {len(films)} film pages and {len(authors)} author pages")
    
    def load_json(self, filename):
        """Load JSON data file"""
        with open(self.data_dir / filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def slugify(self, text):
        """Convert text to URL-friendly slug"""
        text = re.sub(r'[^\w\s-]', '', text.lower())
        text = re.sub(r'[-\s]+', '-', text)
        return text.strip('-')
    
    def generate_film_pages(self, films, metadata):
        """Generate individual film pages"""
        for film in films:
            slug = f"{self.slugify(film['title'])}-{film['release_year']}"
            output_path = self.output_dir / 'films' / f'{slug}.html'
            
            html = self.render_film_page(film, metadata)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html)
    
    def generate_author_pages(self, authors, all_films, metadata):
        """Generate individual author pages"""
        for author in authors:
            slug = self.slugify(author['name'])
            output_path = self.output_dir / 'authors' / f'{slug}.html'
            
            # Get full film details for this author
            author_films = []
            for film_ref in author['films']:
                film = next((f for f in all_films if f['id'] == film_ref['id']), None)
                if film:
                    author_films.append(film)
            
            html = self.render_author_page(author, author_films, metadata)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html)
    
    def generate_index_pages(self, films, authors, metadata):
        """Generate index pages for films and authors"""
        # Films index
        films_index_html = self.render_films_index(films, metadata)
        with open(self.output_dir / 'films' / 'index.html', 'w', encoding='utf-8') as f:
            f.write(films_index_html)
        
        # Authors index
        authors_index_html = self.render_authors_index(authors, metadata)
        with open(self.output_dir / 'authors' / 'index.html', 'w', encoding='utf-8') as f:
            f.write(authors_index_html)
    
    def render_film_page(self, film, metadata):
        """Render individual film page"""
        subjects_html = ""
        if film.get('controlled_subjects'):
            subjects_by_facet = {}
            for subject in film['controlled_subjects']:
                facet = subject.get('facet', 'Other')
                if facet not in subjects_by_facet:
                    subjects_by_facet[facet] = []
                subjects_by_facet[facet].append(subject)
            
            subjects_html = '<div class="subjects-section">'
            subjects_html += '<h2>Themes & Subjects</h2>'
            for facet, subjects in subjects_by_facet.items():
                subjects_html += f'<div class="facet-group">'
                subjects_html += f'<h3>{facet}</h3>'
                subjects_html += '<div class="subject-tags">'
                for subject in subjects:
                    weight_class = 'primary' if subject.get('weight', 1) >= 2 else 'secondary'
                    subjects_html += f'<span class="subject-tag {weight_class}">{subject["term"]}</span>'
                subjects_html += '</div></div>'
            subjects_html += '</div>'
        
        # Generate structured data for SEO
        structured_data = {
            "@context": "https://schema.org",
            "@type": "Movie",
            "name": film['title'],
            "datePublished": str(film['release_year']),
        }
        
        if film.get('director'):
            structured_data["director"] = {"@type": "Person", "name": film['director']}
        
        if film.get('literary_credits'):
            structured_data["isBasedOn"] = {
                "@type": "Book",
                "author": {"@type": "Person", "name": film['literary_credits']}
            }
        
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="{film['title']} ({film['release_year']}) - Film adaptation of work by {film.get('literary_credits', 'Unknown')}">
    <title>{film['title']} ({film['release_year']}) - Hollywood Adaptations Database</title>
    
    <link rel="canonical" href="/films/{self.slugify(film['title'])}-{film['release_year']}.html">
    
    <script type="application/ld+json">
    {json.dumps(structured_data, indent=2)}
    </script>
    
{self.get_common_styles()}

</head>
<body>
    <div class="film-header">
        <div class="container">
            <div class="breadcrumb">
                <a href="/">Home</a> / <a href="/films/">Films</a> / {film['title']}
            </div>
            <h1 class="film-title">{film['title']}</h1>
            <div class="film-meta">
                <span>Released: {film['release_year']}</span>
                {f'<span>Director: {film["director"]}</span>' if film.get('director') else ''}
                {f'<span>Author: {film["literary_credits"]}</span>' if film.get('literary_credits') else ''}
            </div>
        </div>
    </div>
    
    <main class="container">
        <div class="detail-grid">
            <div class="detail-card">
                <h2>Film Details</h2>
                <div class="detail-item">
                    <div class="detail-label">Title</div>
                    <div>{film['title']}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Release Year</div>
                    <div>{film['release_year']}</div>
                </div>
                {f'''<div class="detail-item">
                    <div class="detail-label">Director</div>
                    <div>{film['director']}</div>
                </div>''' if film.get('director') else ''}
                {f'''<div class="detail-item">
                    <div class="detail-label">Genre</div>
                    <div>{film['genre']}</div>
                </div>''' if film.get('genre') else ''}
                {f'''<div class="detail-item">
                    <div class="detail-label">Survival Status</div>
                    <div>{film['survival_status']}</div>
                </div>''' if film.get('survival_status') else ''}
            </div>
            
            <div class="detail-card">
                <h2>Source & Credits</h2>
                {f'''<div class="detail-item">
                    <div class="detail-label">Literary Credits</div>
                    <div><a href="/authors/{self.slugify(film['literary_credits'])}.html">{film['literary_credits']}</a></div>
                </div>''' if film.get('literary_credits') else ''}
                {f'''<div class="detail-item">
                    <div class="detail-label">Writer</div>
                    <div>{film['writer']}</div>
                </div>''' if film.get('writer') else ''}
                {f'''<div class="detail-item">
                    <div class="detail-label">Producer</div>
                    <div>{film['producer']}</div>
                </div>''' if film.get('producer') else ''}
            </div>
        </div>
        
        {subjects_html}
        
        <div class="navigation-links">
            <a href="/films/" class="btn">← Back to All Films</a>
            {f'<a href="/authors/{self.slugify(film["literary_credits"])}.html" class="btn">View Author Page →</a>' if film.get('literary_credits') else ''}
        </div>
    </main>
    
    {self.get_footer_html()}
</body>
</html>'''
    
    def render_author_page(self, author, films, metadata):
        """Render individual author page"""
        films_html = ""
        if films:
            films_html = '<div class="films-grid">'
            for film in sorted(films, key=lambda f: f['release_year']):
                film_slug = f"{self.slugify(film['title'])}-{film['release_year']}"
                films_html += f'''
                <div class="film-card">
                    <h3><a href="/films/{film_slug}.html">{film['title']}</a></h3>
                    <div class="film-year">{film['release_year']}</div>
                    {f'<p>Director: {film["director"]}</p>' if film.get('director') else ''}
                    {f'<p class="survival-status {film["survival_status"].lower().replace(" ", "-") if film.get("survival_status") else ""}">Status: {film["survival_status"]}</p>' if film.get('survival_status') else ''}
                </div>'''
            films_html += '</div>'
        
        # Timeline visualization data
        timeline_data = []
        for film in films:
            timeline_data.append({
                'year': film['release_year'],
                'title': film['title'],
                'slug': f"{self.slugify(film['title'])}-{film['release_year']}"
            })
        
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="{author['name']} - {author['adaptation_count']} film adaptations from {author['first_adaptation']} to {author['last_adaptation']}">
    <title>{author['name']} - Hollywood Adaptations Database</title>
    
    <link rel="canonical" href="/authors/{self.slugify(author['name'])}.html">
    
    <script type="application/ld+json">
    {{
        "@context": "https://schema.org",
        "@type": "Person",
        "name": "{author['name']}",
        "jobTitle": "Author"
    }}
    </script>
    
 {self.get_common_styles()}
</head>
<body>
    <div class="author-header">
        <div class="container">
            <div class="breadcrumb">
                <a href="/">Home</a> / <a href="/authors/">Authors</a> / {author['name']}
            </div>
            <h1 class="author-name">{author['name']}</h1>
        </div>
    </div>
    
    <main class="container">
        <div class="author-stats">
            <div class="stat">
                <div class="stat-number">{author['adaptation_count']}</div>
                <div class="stat-label">Film Adaptations</div>
            </div>
            <div class="stat">
                <div class="stat-number">{author['first_adaptation']}</div>
                <div class="stat-label">First Adaptation</div>
            </div>
            <div class="stat">
                <div class="stat-number">{author['last_adaptation']}</div>
                <div class="stat-label">Last Adaptation</div>
            </div>
            <div class="stat">
                <div class="stat-number">{author['year_span']}</div>
                <div class="stat-label">Year Span</div>
            </div>
        </div>
        
        <section>
            <h2>Film Adaptations</h2>
            {films_html}
        </section>
        
        <section class="timeline-section">
            <h2>Adaptation Timeline</h2>
            <div class="timeline">
                {''.join([f'<div class="timeline-item"><strong>{item["year"]}</strong>: <a href="/films/{item["slug"]}.html">{item["title"]}</a></div>' for item in timeline_data])}
            </div>
        </section>
        
        <div class="navigation-links">
            <a href="/authors/" class="btn">← Back to All Authors</a>
        </div>
    </main>
    
    {self.get_footer_html()}
</body>
</html>'''
    
    def render_films_index(self, films, metadata):
        """Render films index page"""
        films_by_decade = {}
        for film in films:
            decade = (film['release_year'] // 10) * 10
            if decade not in films_by_decade:
                films_by_decade[decade] = []
            films_by_decade[decade].append(film)
        
        films_list_html = ""
        for decade in sorted(films_by_decade.keys()):
            films_list_html += f'<h2>{decade}s</h2>'
            films_list_html += '<div class="films-grid">'
            for film in sorted(films_by_decade[decade], key=lambda f: (f['release_year'], f['title'])):
                film_slug = f"{self.slugify(film['title'])}-{film['release_year']}"
                films_list_html += f'''
                <div class="film-card">
                    <h3><a href="/films/{film_slug}.html">{film['title']}</a></h3>
                    <div class="film-year">{film['release_year']}</div>
                    {f'<p>by {film["literary_credits"]}</p>' if film.get('literary_credits') else ''}
                </div>'''
            films_list_html += '</div>'
        
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="Complete list of Hollywood film adaptations of American women writers' works (1910-1960)">
    <title>All Films - Hollywood Adaptations Database</title>
    
{self.get_common_styles()}
</head>
<body>
    <div class="page-header">
        <div class="container">
            <div class="breadcrumb">
                <a href="/">Home</a> / Films
            </div>
            <h1>All Films</h1>
            <p>{len(films)} films from {metadata['statistics']['year_range']['start']} to {metadata['statistics']['year_range']['end']}</p>
        </div>
    </div>
    
    <main class="container">
        {films_list_html}
    </main>
    
    {self.get_footer_html()}
</body>
</html>'''
    
    def render_authors_index(self, authors, metadata):
        """Render authors index page"""
        authors_html = '<div class="authors-grid">'
        for author in sorted(authors, key=lambda a: a['name']):
            author_slug = self.slugify(author['name'])
            authors_html += f'''
            <div class="author-card">
                <h3><a href="/authors/{author_slug}.html">{author['name']}</a></h3>
                <p>{author['adaptation_count']} adaptations ({author['first_adaptation']}-{author['last_adaptation']})</p>
            </div>'''
        authors_html += '</div>'
        
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="American women writers whose works were adapted to film (1910-1960)">
    <title>All Authors - Hollywood Adaptations Database</title>
    
 {self.get_common_styles()}
</head>
<body>
    <div class="page-header">
        <div class="container">
            <div class="breadcrumb">
                <a href="/">Home</a> / Authors
            </div>
            <h1>All Authors</h1>
            <p>{len(authors)} authors adapted to film</p>
        </div>
    </div>
    
    <main class="container">
        {authors_html}
    </main>
    
    {self.get_footer_html()}
</body>
</html>'''
    
    def get_common_styles(self):
        """Get link to common CSS stylesheet"""
        return '<link rel="stylesheet" href="/css/main.css">'
    
    def get_footer_html(self):
        """Get common footer HTML"""
        return f'''
    <footer style="margin-top: 4rem; padding: 2rem 0; background: var(--primary-color); color: white;">
        <div class="container" style="text-align: center;">
            <p>Hollywood Adaptations of American Women Writers (1910-1960)</p>
            <p style="opacity: 0.7; font-size: 0.9rem;">Last updated: {datetime.now().strftime('%B %d, %Y')}</p>
        </div>
    </footer>
    '''


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate static pages for films and authors')
    parser.add_argument('--data', default='site/data',
                        help='Directory containing JSON data files')
    parser.add_argument('--output', default='site',
                        help='Output directory for HTML files')
    
    args = parser.parse_args()
    
    generator = StaticPageGenerator(args.data, args.output)
    generator.generate_all_pages()