# Hollywood Adaptations of American Women Writers (1910-1960)

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
