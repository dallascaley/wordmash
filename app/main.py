from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from app.routers import projects, inventory, training
from app.db import get_conn
from app.jobs import cleanup_stale_jobs
import os


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: clean up any stale jobs from previous runs
    cancelled = cleanup_stale_jobs()
    if cancelled > 0:
        print(f"[Startup] Cancelled {cancelled} stale job(s) from previous run")
    yield
    # Shutdown: nothing to clean up


app = FastAPI(lifespan=lifespan)


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


@app.get("/api/branches/{branch_id}/structure")
def branch_structure(branch_id: int):
    conn = get_conn()
    cursor = conn.cursor()

    # Get the branch info
    cursor.execute("SELECT * FROM branches WHERE id = %s", (branch_id,))
    branch = cursor.fetchone()
    if not branch:
        cursor.close()
        conn.close()
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    # Get all sub-branches under this path
    base_path = branch["path"]
    cursor.execute("""
        SELECT path, files FROM branches
        WHERE project_id = %s AND is_dirty = %s
        AND (path = %s OR path LIKE %s)
        ORDER BY path
    """, (branch["project_id"], branch["is_dirty"], base_path, base_path + "/%"))
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    # Build a tree structure
    def build_tree(rows, base_path):
        tree = {"name": base_path.split("/")[-1] or base_path, "files": 0, "children": []}
        path_to_node = {base_path: tree}

        for row in rows:
            path = row["path"]
            files = row["files"]

            if path == base_path:
                tree["files"] = files
                continue

            # Find parent path
            parts = path.split("/")
            for i in range(len(parts), 0, -1):
                parent_path = "/".join(parts[:i-1]) if i > 1 else ""
                if parent_path in path_to_node or parent_path == base_path:
                    parent = path_to_node.get(parent_path, tree)
                    node = {"name": parts[-1], "files": files, "children": []}
                    parent["children"].append(node)
                    path_to_node[path] = node
                    break

        return tree

    tree = build_tree(rows, base_path)
    return JSONResponse({"tree": tree, "path": base_path})


@app.get("/branches")
def branches(request: Request):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM branches
        WHERE is_root = 1 AND homogeneous = 1 AND is_dirty = 1
        ORDER BY files DESC
    """)
    rows = cursor.fetchall()

    # Determine the type for each branch based on which count field is non-zero
    branches_list = []
    for row in rows:
        if row["valids"] > 0:
            branch_type = "valid"
        elif row["bads"] > 0:
            branch_type = "bad"
        elif row["mixeds"] > 0:
            branch_type = "mixed"
        elif row["researchs"] > 0:
            branch_type = "research"
        else:
            branch_type = "none"

        branches_list.append({
            "id": row["id"],
            "path": row["path"],
            "sub_folders": row["sub_folders"],
            "files": row["files"],
            "type": branch_type,
        })

    cursor.close()
    conn.close()

    return templates.TemplateResponse(
        "branches.html",
        {"request": request, "branches": branches_list}
    )
