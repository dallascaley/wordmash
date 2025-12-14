from fastapi import APIRouter, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from app.db import get_conn
from datetime import datetime
import os

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/projects")
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


@router.post("/projects")
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


@router.get("/project/{project_id}")
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


@router.get("/project/{project_id}/compare")
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


@router.post("/project/{project_id}/scan/files")
def scan_files(request: Request, project_id: int):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()

    if not project:
        cursor.close()
        conn.close()
        return RedirectResponse(url="/inventory", status_code=303)

    dirty_root = project["dirty_root"]

    cursor.execute("DELETE FROM files WHERE project_id = %s", (project_id,))
    conn.commit()

    files_to_insert = []
    for root, dirs, files in os.walk(dirty_root):
        for file_name in files:
            full_path = os.path.join(root, file_name)

            relative_dir = os.path.relpath(root, dirty_root)
            if relative_dir == ".":
                relative_dir = ""

            try:
                stat = os.stat(full_path)
                created_at = datetime.fromtimestamp(stat.st_ctime)
                updated_at = datetime.fromtimestamp(stat.st_mtime)
            except OSError:
                created_at = datetime.now()
                updated_at = datetime.now()

            is_binary = False
            try:
                with open(full_path, 'rb') as f:
                    chunk = f.read(8192)
                    if b'\x00' in chunk:
                        is_binary = True
            except (IOError, OSError):
                pass

            files_to_insert.append((file_name, relative_dir, created_at, updated_at, is_binary, project_id))

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


@router.post("/project/{project_id}/scan/lines")
def scan_lines(request: Request, project_id: int):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()

    if not project:
        cursor.close()
        conn.close()
        return RedirectResponse(url="/inventory", status_code=303)

    dirty_root = project["dirty_root"]

    cursor.execute("SELECT id, file_name, path FROM files WHERE project_id = %s AND is_binary = FALSE", (project_id,))
    files = cursor.fetchall()

    cursor.execute("""
        DELETE fr FROM file_rows fr
        JOIN files f ON fr.file_id = f.id
        WHERE f.project_id = %s
    """, (project_id,))
    conn.commit()

    rows_to_insert = []
    batch_size = 1000

    for file_record in files:
        file_id = file_record["id"]
        file_name = file_record["file_name"]
        path = file_record["path"]

        if path:
            full_path = os.path.join(dirty_root, path, file_name)
        else:
            full_path = os.path.join(dirty_root, file_name)

        try:
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    clean_line = line.rstrip('\n\r')
                    rows_to_insert.append((clean_line, file_id))

                    if len(rows_to_insert) >= batch_size:
                        cursor.executemany(
                            "INSERT INTO file_rows (text, file_id) VALUES (%s, %s)",
                            rows_to_insert
                        )
                        conn.commit()
                        rows_to_insert = []
        except (IOError, OSError):
            pass

    if rows_to_insert:
        cursor.executemany(
            "INSERT INTO file_rows (text, file_id) VALUES (%s, %s)",
            rows_to_insert
        )
        conn.commit()

    cursor.close()
    conn.close()

    return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)
