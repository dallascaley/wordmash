from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from app.db import get_conn
import os

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

BROKEN_ROOT = "/home/hakr49/dallascaley.info/dallascaley-old"
CLEAN_ROOT = "/home/hakr49/dallascaley.info/wordpress"

@app.get("/")
def home(request: Request):
    return templates.TemplateResponse(
        "home.html",
        {"request": request}
    )


@app.get("/compare")
def compare(request: Request, path: str):
    broken_file = os.path.join(BROKEN_ROOT, path)
    clean_file = os.path.join(CLEAN_ROOT, path)

    broken_content = open(broken_file).read() if os.path.exists(broken_file) else ""
    clean_content = open(clean_file).read() if os.path.exists(clean_file) else ""

    return templates.TemplateResponse(
        "compare.html",
        {
            "request": request,
            "path": path,
            "broken_content": broken_content,
            "clean_content": clean_content
        }
    )


@app.get("/projects")
def projects(request: Request):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects ORDER BY created_at DESC")
    projects = cursor.fetchall()
    cursor.close()
    conn.close()
    return templates.TemplateResponse(
        "projects.html",
        {"request": request, "projects": projects}
    )


@app.post("/projects")
def create_project(
    request: Request,
    name: str = Form(...),
    description: str = Form(""),
    url: str = Form(""),
    clean_root: str = Form(...),
    dirty_root: str = Form(...)
):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO projects (name, description, url, clean_root, dirty_root) VALUES (%s, %s, %s, %s, %s)",
        (name, description, url, clean_root, dirty_root)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return RedirectResponse(url="/projects", status_code=303)


@app.get("/inventory")
def inventory(request: Request, project_id: int = None):
    conn = get_conn()
    cursor = conn.cursor()

    # Get all projects for dropdown
    cursor.execute("SELECT id, name FROM projects ORDER BY name")
    projects = cursor.fetchall()

    stats = None
    project = None

    if project_id:
        # Get project details
        cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        project = cursor.fetchone()

        if project:
            # Get file stats
            cursor.execute("""
                SELECT
                    COUNT(*) as total_files,
                    SUM(CASE WHEN processed = 1 THEN 1 ELSE 0 END) as processed_files
                FROM files WHERE project_id = %s
            """, (project_id,))
            file_stats = cursor.fetchone()

            # Get file rows stats
            cursor.execute("""
                SELECT
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN fr.processed = 1 THEN 1 ELSE 0 END) as processed_rows
                FROM file_rows fr
                JOIN files f ON fr.file_id = f.id
                WHERE f.project_id = %s
            """, (project_id,))
            row_stats = cursor.fetchone()

            # Get db tables stats
            cursor.execute("""
                SELECT
                    COUNT(*) as total_tables,
                    SUM(CASE WHEN processed = 1 THEN 1 ELSE 0 END) as processed_tables
                FROM db_tables WHERE project_id = %s
            """, (project_id,))
            table_stats = cursor.fetchone()

            # Get db table rows stats
            cursor.execute("""
                SELECT
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN dtr.processed = 1 THEN 1 ELSE 0 END) as processed_rows
                FROM db_table_rows dtr
                JOIN db_tables dt ON dtr.table_id = dt.id
                WHERE dt.project_id = %s
            """, (project_id,))
            db_row_stats = cursor.fetchone()

            stats = {
                "files": file_stats,
                "file_rows": row_stats,
                "db_tables": table_stats,
                "db_table_rows": db_row_stats
            }

    cursor.close()
    conn.close()

    return templates.TemplateResponse(
        "inventory.html",
        {"request": request, "projects": projects, "project": project, "stats": stats}
    )


@app.get("/statistics")
def statistics(request: Request):
    return templates.TemplateResponse(
        "statistics.html",
        {"request": request}
    )


@app.get("/admin")
def admin(request: Request):
    return templates.TemplateResponse(
        "admin.html",
        {"request": request}
    )


@app.get("/project/{project_id}")
def project(request: Request, project_id: int):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()
    cursor.close()
    conn.close()
    if not project:
        return templates.TemplateResponse("404.html", {"request": request}, status_code=404)
    return templates.TemplateResponse(
        "project.html",
        {"request": request, "project": project}
    )


@app.get("/project/{project_id}/compare")
def project_compare(request: Request, project_id: int, path: str):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()
    cursor.close()
    conn.close()
    if not project:
        return templates.TemplateResponse("404.html", {"request": request}, status_code=404)

    dirty_file = os.path.join(project["dirty_root"], path)
    clean_file = os.path.join(project["clean_root"], path)

    dirty_content = open(dirty_file).read() if os.path.exists(dirty_file) else ""
    clean_content = open(clean_file).read() if os.path.exists(clean_file) else ""

    return templates.TemplateResponse(
        "project_compare.html",
        {
            "request": request,
            "project": project,
            "path": path,
            "dirty_content": dirty_content,
            "clean_content": clean_content
        }
    )


@app.post("/project/{project_id}/scan/files")
def scan_files(request: Request, project_id: int):
    from datetime import datetime

    conn = get_conn()
    cursor = conn.cursor()

    # Get project
    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()

    if not project:
        cursor.close()
        conn.close()
        return RedirectResponse(url="/inventory", status_code=303)

    dirty_root = project["dirty_root"]

    # Clear existing files for this project (allows re-scanning)
    cursor.execute("DELETE FROM files WHERE project_id = %s", (project_id,))
    conn.commit()

    # Collect all files with metadata
    files_to_insert = []
    for root, dirs, files in os.walk(dirty_root):
        for file_name in files:
            full_path = os.path.join(root, file_name)

            # Get directory path relative to dirty_root (without filename)
            relative_dir = os.path.relpath(root, dirty_root)
            if relative_dir == ".":
                relative_dir = ""

            # Get file timestamps from filesystem
            try:
                stat = os.stat(full_path)
                created_at = datetime.fromtimestamp(stat.st_ctime)
                updated_at = datetime.fromtimestamp(stat.st_mtime)
            except OSError:
                created_at = datetime.now()
                updated_at = datetime.now()

            # Check if file is binary (contains null bytes)
            is_binary = False
            try:
                with open(full_path, 'rb') as f:
                    chunk = f.read(8192)
                    if b'\x00' in chunk:
                        is_binary = True
            except (IOError, OSError):
                pass

            files_to_insert.append((file_name, relative_dir, created_at, updated_at, is_binary, project_id))

    # Batch insert for performance
    batch_size = 500
    for i in range(0, len(files_to_insert), batch_size):
        batch = files_to_insert[i:i+batch_size]
        cursor.executemany(
            "INSERT INTO files (file_name, path, created_at, updated_at, is_binary, project_id) VALUES (%s, %s, %s, %s, %s, %s)",
            batch
        )
        conn.commit()

    cursor.close()
    conn.close()

    return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)


@app.post("/project/{project_id}/scan/lines")
def scan_lines(request: Request, project_id: int):
    conn = get_conn()
    cursor = conn.cursor()

    # Get project
    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()

    if not project:
        cursor.close()
        conn.close()
        return RedirectResponse(url="/inventory", status_code=303)

    dirty_root = project["dirty_root"]

    # Get all non-binary files for this project
    cursor.execute("SELECT id, file_name, path FROM files WHERE project_id = %s AND is_binary = FALSE", (project_id,))
    files = cursor.fetchall()

    # Clear existing file_rows for this project's files
    cursor.execute("""
        DELETE fr FROM file_rows fr
        JOIN files f ON fr.file_id = f.id
        WHERE f.project_id = %s
    """, (project_id,))
    conn.commit()

    # Process each file
    rows_to_insert = []
    batch_size = 1000

    for file_record in files:
        file_id = file_record["id"]
        file_name = file_record["file_name"]
        path = file_record["path"]

        # Build full path
        if path:
            full_path = os.path.join(dirty_root, path, file_name)
        else:
            full_path = os.path.join(dirty_root, file_name)

        # Read file and get lines
        try:
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    # Clean the line for MySQL
                    clean_line = line.rstrip('\n\r')
                    rows_to_insert.append((clean_line, file_id))

                    # Insert in batches
                    if len(rows_to_insert) >= batch_size:
                        cursor.executemany(
                            "INSERT INTO file_rows (text, file_id) VALUES (%s, %s)",
                            rows_to_insert
                        )
                        conn.commit()
                        rows_to_insert = []
        except (IOError, OSError):
            # Skip files that can't be read
            pass

    # Insert remaining rows
    if rows_to_insert:
        cursor.executemany(
            "INSERT INTO file_rows (text, file_id) VALUES (%s, %s)",
            rows_to_insert
        )
        conn.commit()

    cursor.close()
    conn.close()

    return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)