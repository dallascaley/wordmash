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


@app.get("/admin")
def admin(request: Request):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects ORDER BY created_at DESC")
    projects = cursor.fetchall()
    cursor.close()
    conn.close()
    return templates.TemplateResponse(
        "admin.html",
        {"request": request, "projects": projects}
    )


@app.post("/admin/projects")
def create_project(request: Request, name: str = Form(...), clean_root: str = Form(...), dirty_root: str = Form(...)):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO projects (name, clean_root, dirty_root) VALUES (%s, %s, %s)",
        (name, clean_root, dirty_root)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return RedirectResponse(url="/admin", status_code=303)


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