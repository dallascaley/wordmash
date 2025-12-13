# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WordMash is a FastAPI web application for comparing files between a "broken" and "clean" WordPress installation. It includes a classification workflow to label file differences and a placeholder ML training pipeline (scikit-learn).

## Commands

**Run the development server:**
```bash
uvicorn app.main:app --reload
```

**Install dependencies:**
```bash
pip install -r requirements.txt
```

**Environment variables (required):**
```bash
export DB_HOST=your_host
export DB_USER=your_username
export DB_PASSWORD=your_password
export DB_NAME=wordmash  # optional, defaults to "wordmash"
```

See `.env.example` for reference.

## Architecture

```
app/
├── main.py          # FastAPI app, routes for home (/) and compare (/compare)
├── db.py            # MySQL connection via PyMySQL
├── templates/       # Jinja2 templates (layout.html base, home.html, compare.html)
├── static/          # CSS assets
├── routers/         # Route modules (files, records) - currently empty stubs
├── ml/              # ML training module and model.pkl
└── utils/           # Utility modules (file_loader, diff_utils) - currently empty stubs
```

**Key paths configured in main.py:**
- `BROKEN_ROOT`: Path to the broken WordPress installation
- `CLEAN_ROOT`: Path to the clean WordPress installation

**Database:** MySQL connection via environment variables (see db.py)

## UI Flow

1. Home page (`/`) - Enter a file path to compare
2. Compare page (`/compare?path=...`) - Side-by-side view of broken vs clean file content with Good/Broken classification buttons
