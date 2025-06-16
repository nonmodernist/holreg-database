import os
import shutil
from pathlib import Path

def organize_repository():
    """Reorganize repository files into a clean structure"""
    
    # Define the new directory structure
    directories = [
        'data/raw',
        'data/processed',
        'data/databases/backups',
        'scripts/data_collection',
        'scripts/data_processing',
        'scripts/utilities',
        'queries/analysis',
        'queries/research',
        'docs',
        'outputs/logs',
        'outputs/visualizations',
        'web'
    ]
    
    # Create .gitignore
    gitignore_content = """# Database files
*.db
*.sqlite

# Backup files
*.bak
*.backup

# Python
__pycache__/
*.py[cod]
*$py.class
.env
venv/
env/

# Logs
*.log
logs/

# OS files
.DS_Store
Thumbs.db

# IDE files
.vscode/
.idea/
*.swp
*.swo

# Temporary files
*.tmp
~*
"""
    
    with open('.gitignore', 'w') as f:
        f.write(gitignore_content)
    print("\n✅ Created .gitignore")
    
    # Create README.md
    readme_content = """# Hollywood Adaptations of American Women Writers (1910-1960)

## Overview
This repository contains data and analysis tools for researching film adaptations of works by American women writers from 1910-1960.

## Database Structure
- **Films**: Core film data from AFI Catalog
- **Controlled Vocabulary**: Standardized subject terms for analysis
- **Mappings**: Connections between AFI subjects and controlled terms

## Quick Start
1. Check database: `python scripts/utilities/database_checker.py`
2. Run analysis queries in the `queries/analysis/` folder
3. View results in VS Code's SQLite extension

## Data Sources
- AFI Catalog API
- Manual research additions

## Key Files
- `data/databases/adaptation_research.db` - Main database
- `docs/controlled_vocab.md` - Controlled vocabulary documentation
- `queries/analysis/` - Pre-built analysis queries
"""
    
    with open('README.md', 'w') as f:
        f.write(readme_content)
    print("✅ Created README.md")
    
    # Create requirements.txt
    requirements = """pandas>=1.3.0
sqlite3
requests
beautifulsoup4
lxml
"""
    
    with open('requirements.txt', 'w') as f:
        f.write(requirements)
    print("✅ Created requirements.txt")
    
    print("\n🎉 Repository organized successfully!")
    print("\nNext steps:")
    print("1. Review the new structure")
    print("2. Commit changes to git")
    print("3. Update any hardcoded paths in your scripts")

if __name__ == "__main__":
    organize_repository()