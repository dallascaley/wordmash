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

    # Walk through all files in dirty_root
    file_count = 0
    for root, dirs, files in os.walk(dirty_root):
        for file_name in files:
            full_path = os.path.join(root, file_name)
            # Store path relative to dirty_root
            relative_path = os.path.relpath(full_path, dirty_root)

            cursor.execute(
                "INSERT INTO files (file_name, path, project_id) VALUES (%s, %s, %s)",
                (file_name, relative_path, project_id)
            )
            file_count += 1

    conn.commit()
    cursor.close()
    conn.close()

    return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)