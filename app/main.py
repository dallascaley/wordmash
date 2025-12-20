from fastapi import FastAPI, Request, WebSocket
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from app.routers import projects, inventory, training
from app.db import get_conn
import os

app = FastAPI()


@app.websocket("/ws/ping")
async def websocket_ping(websocket: WebSocket):
    await websocket.accept()
    await websocket.close()


@app.get("/api/projects")
def api_projects():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM projects ORDER BY name")
    projects = cursor.fetchall()
    cursor.close()
    conn.close()
    return JSONResponse([{"id": p["id"], "name": p["name"]} for p in projects])


templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routers
app.include_router(projects.router)
app.include_router(inventory.router)
app.include_router(training.router)

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


@app.get("/statistics")
def statistics(request: Request):
    from app.db import get_conn

    conn = get_conn()
    cursor = conn.cursor()

    stats = {
        "classifier_accuracy": 0.0,
        "total_files": 0,
        "total_lines": 0,
        "total_tables": 0,
        "total_db_rows": 0,
        "files_valid": 0,
        "files_mixed": 0,
        "files_research": 0,
        "files_unclassified": 0,
        "lines_valid": 0,
        "lines_research": 0,
        "lines_unclassified": 0,
        "training_progress": 0.0,
    }

    # Total files (dirty only - what we're classifying)
    cursor.execute("SELECT COUNT(*) as cnt FROM files WHERE is_dirty = 1")
    stats["total_files"] = cursor.fetchone()["cnt"]

    # Total lines (dirty only)
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM file_rows fr
        JOIN files f ON fr.file_id = f.id
        WHERE fr.is_dirty = 1
    """)
    stats["total_lines"] = cursor.fetchone()["cnt"]

    # Total tables (dirty only)
    cursor.execute("SELECT COUNT(*) as cnt FROM db_tables WHERE is_dirty = 1")
    stats["total_tables"] = cursor.fetchone()["cnt"]

    # Total db rows (dirty only)
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM db_table_rows dr
        JOIN db_tables t ON dr.table_id = t.id
        WHERE dr.is_dirty = 1
    """)
    stats["total_db_rows"] = cursor.fetchone()["cnt"]

    # File classification breakdown
    cursor.execute("""
        SELECT status, COUNT(*) as cnt FROM files
        WHERE is_dirty = 1
        GROUP BY status
    """)
    for row in cursor.fetchall():
        if row["status"] == "valid":
            stats["files_valid"] = row["cnt"]
        elif row["status"] == "mixed":
            stats["files_mixed"] = row["cnt"]
        elif row["status"] == "research":
            stats["files_research"] = row["cnt"]
        elif row["status"] is None:
            stats["files_unclassified"] = row["cnt"]

    # Line classification breakdown
    cursor.execute("""
        SELECT fr.status, COUNT(*) as cnt FROM file_rows fr
        JOIN files f ON fr.file_id = f.id
        WHERE fr.is_dirty = 1
        GROUP BY fr.status
    """)
    for row in cursor.fetchall():
        if row["status"] == "valid":
            stats["lines_valid"] = row["cnt"]
        elif row["status"] == "research":
            stats["lines_research"] = row["cnt"]
        elif row["status"] is None:
            stats["lines_unclassified"] = row["cnt"]

    # Training progress (files classified / total files)
    files_classified = stats["files_valid"] + stats["files_mixed"] + stats["files_research"]
    if stats["total_files"] > 0:
        stats["training_progress"] = round((files_classified / stats["total_files"]) * 100, 1)

    # Classifier accuracy (valid lines / total classified lines)
    lines_classified = stats["lines_valid"] + stats["lines_research"]
    if lines_classified > 0:
        stats["classifier_accuracy"] = round((stats["lines_valid"] / lines_classified) * 100, 1)

    cursor.close()
    conn.close()

    return templates.TemplateResponse(
        "statistics.html",
        {"request": request, "stats": stats}
    )


@app.get("/admin")
def admin(request: Request):
    return templates.TemplateResponse(
        "admin.html",
        {"request": request}
    )
