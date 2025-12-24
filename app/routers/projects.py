from fastapi import APIRouter, Request, Form, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, JSONResponse
from app.db import get_conn
from app.jobs import (
    create_job, update_job, get_job, get_running_job,
    start_job, complete_job, fail_job, run_job_in_background
)
from datetime import datetime
import asyncio
import os

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def update_inventory_counts(cursor, conn, project_id: int, is_dirty: int, count_type: str, count: int):
    """
    Update the cached inventory counts for a project.
    count_type should be one of: 'files', 'file_rows', 'db_tables', 'db_table_rows'
    """
    column = f"{count_type}_count"

    # Ensure inventory row exists for this project/is_dirty combination
    cursor.execute("""
        INSERT INTO inventory (project_id, is_dirty)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE project_id = project_id
    """, (project_id, is_dirty))

    # Update the specific count
    cursor.execute(f"""
        UPDATE inventory SET {column} = %s
        WHERE project_id = %s AND is_dirty = %s
    """, (count, project_id, is_dirty))
    conn.commit()


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
    dirty_root: str = Form(...),
    clean_db: str = Form(""),
    dirty_db: str = Form("")
):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO projects (name, description, url, clean_root, dirty_root, clean_db, dirty_db) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (name, description, url, clean_root, dirty_root, clean_db or None, dirty_db or None)
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


@router.get("/project/{project_id}/edit")
def edit_project_form(request: Request, project_id: int):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()
    cursor.close()
    conn.close()
    if not project:
        return templates.TemplateResponse("404.html", {"request": request}, status_code=404)
    return templates.TemplateResponse(
        "project_edit.html",
        {"request": request, "project": project}
    )


@router.post("/project/{project_id}/edit")
def edit_project(
    request: Request,
    project_id: int,
    name: str = Form(...),
    description: str = Form(""),
    url: str = Form(""),
    clean_root: str = Form(...),
    dirty_root: str = Form(...),
    clean_db: str = Form(""),
    dirty_db: str = Form("")
):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE projects
           SET name = %s, description = %s, url = %s, clean_root = %s, dirty_root = %s, clean_db = %s, dirty_db = %s
           WHERE id = %s""",
        (name, description, url, clean_root, dirty_root, clean_db or None, dirty_db or None, project_id)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return RedirectResponse(url="/projects", status_code=303)


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


def scan_files_generator(project_id: int, root_path: str, is_dirty: int):
    """
    Generator that yields file count updates during scanning.
    Yields progress dicts every 50 files, then a complete dict with all files.
    Skips the 'quarantine' folder for dirty files.
    """
    files_to_insert = []
    count = 0

    for root, dirs, files in os.walk(root_path):
        # Skip quarantine folder for dirty files
        if is_dirty:
            dirs[:] = [d for d in dirs if d != 'quarantine']
            relative_dir = os.path.relpath(root, root_path)
            if relative_dir == 'quarantine' or relative_dir.startswith('quarantine/'):
                continue
        for file_name in files:
            full_path = os.path.join(root, file_name)

            relative_dir = os.path.relpath(root, root_path)
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

            files_to_insert.append((file_name, relative_dir, created_at, updated_at, is_binary, project_id, is_dirty))
            count += 1

            if count % 50 == 0:
                yield {"type": "progress", "count": count}

    yield {"type": "complete", "count": count, "files": files_to_insert}


@router.post("/project/{project_id}/scan/files")
def scan_files(request: Request, project_id: int, is_dirty: int = 1):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()

    if not project:
        cursor.close()
        conn.close()
        return RedirectResponse(url="/inventory", status_code=303)

    root_path = project["dirty_root"] if is_dirty else project["clean_root"]

    # Only delete files for this project with the same is_dirty flag
    cursor.execute("DELETE FROM files WHERE project_id = %s AND is_dirty = %s", (project_id, is_dirty))
    conn.commit()

    files_to_insert = []
    for root, dirs, files in os.walk(root_path):
        # Skip quarantine folder for dirty files
        if is_dirty:
            dirs[:] = [d for d in dirs if d != 'quarantine']
            rel_check = os.path.relpath(root, root_path)
            if rel_check == 'quarantine' or rel_check.startswith('quarantine/'):
                continue

        for file_name in files:
            full_path = os.path.join(root, file_name)

            relative_dir = os.path.relpath(root, root_path)
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

            files_to_insert.append((file_name, relative_dir, created_at, updated_at, is_binary, project_id, is_dirty))

    batch_size = 500
    for i in range(0, len(files_to_insert), batch_size):
        batch = files_to_insert[i:i+batch_size]
        cursor.executemany(
            "INSERT INTO files (file_name, path, created_at, updated_at, is_binary, project_id, is_dirty) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            batch
        )
        conn.commit()

    # Update inventory cache
    update_inventory_counts(cursor, conn, project_id, is_dirty, 'files', len(files_to_insert))

    cursor.close()
    conn.close()

    return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)


async def scan_files_background_task(job_id: int, project_id: int, root_path: str, is_dirty: int):
    """
    Background task that scans files and updates the jobs table with progress.
    """
    conn = None
    cursor = None
    try:
        start_job(job_id)

        conn = get_conn()
        cursor = conn.cursor()

        # Clear existing files for this is_dirty type only
        cursor.execute("DELETE FROM files WHERE project_id = %s AND is_dirty = %s", (project_id, is_dirty))
        conn.commit()

        # Scan files and collect them
        files_to_insert = []
        count = 0

        for update in scan_files_generator(project_id, root_path, is_dirty):
            if update["type"] == "progress":
                count = update["count"]
                update_job(job_id, progress=count, message=f"Scanning: {count} files found")
                await asyncio.sleep(0)  # Yield to event loop
            elif update["type"] == "complete":
                files_to_insert = update["files"]
                count = update["count"]

        # Update job to show we're inserting
        update_job(job_id, progress=count, total=count, message=f"Inserting {count} files into database...")

        # Batch insert
        batch_size = 500
        inserted = 0
        for i in range(0, len(files_to_insert), batch_size):
            batch = files_to_insert[i:i+batch_size]
            cursor.executemany(
                "INSERT INTO files (file_name, path, created_at, updated_at, is_binary, project_id, is_dirty) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                batch
            )
            conn.commit()
            inserted += len(batch)
            update_job(job_id, message=f"Inserted {inserted}/{count} files")
            await asyncio.sleep(0)

        # Update inventory cache
        update_inventory_counts(cursor, conn, project_id, is_dirty, 'files', len(files_to_insert))

        # Mark job complete
        complete_job(job_id, total=len(files_to_insert), message=f"Completed: {len(files_to_insert)} files scanned")

    except asyncio.CancelledError:
        raise  # Let the wrapper handle cancellation
    except Exception as e:
        fail_job(job_id, str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@router.post("/project/{project_id}/scan/files/start")
async def start_scan_files(project_id: int, is_dirty: int = 1):
    """
    Start a file scan job. Returns the job_id for tracking progress.
    If a scan is already running, returns the existing job_id.
    """
    # Check for existing running job
    job_type = f"scan_files_{is_dirty}"
    existing_job = get_running_job(job_type, project_id)
    if existing_job:
        return JSONResponse({
            "job_id": existing_job["id"],
            "status": existing_job["status"],
            "message": "Job already running",
            "existing": True
        })

    # Get project info
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()
    cursor.close()
    conn.close()

    if not project:
        return JSONResponse({"error": "Project not found"}, status_code=404)

    root_path = project["dirty_root"] if is_dirty else project["clean_root"]

    # Create the job
    job_id = create_job(job_type, project_id, message="Starting file scan...")

    # Start background task
    run_job_in_background(
        job_id,
        scan_files_background_task(job_id, project_id, root_path, is_dirty)
    )

    return JSONResponse({
        "job_id": job_id,
        "status": "pending",
        "message": "Job started",
        "existing": False
    })


@router.get("/job/{job_id}")
def get_job_status(job_id: int):
    """Get the current status of a job."""
    job = get_job(job_id)
    if not job:
        return JSONResponse({"error": "Job not found"}, status_code=404)

    # Convert datetime objects to strings for JSON serialization
    result = dict(job)
    for key in ['created_at', 'started_at', 'ended_at']:
        if result.get(key):
            result[key] = result[key].isoformat()

    return JSONResponse(result)


@router.get("/project/{project_id}/job/{job_type}")
def get_project_job(project_id: int, job_type: str):
    """Get a running or recent job for a project and job type."""
    # First check for running job
    job = get_running_job(job_type, project_id)

    if not job:
        # Get most recent completed job
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """SELECT * FROM jobs
               WHERE job_type = %s AND project_id = %s
               ORDER BY created_at DESC LIMIT 1""",
            (job_type, project_id)
        )
        job = cursor.fetchone()
        cursor.close()
        conn.close()

    if not job:
        return JSONResponse({"error": "No job found"}, status_code=404)

    result = dict(job)
    for key in ['created_at', 'started_at', 'ended_at']:
        if result.get(key):
            result[key] = result[key].isoformat()

    return JSONResponse(result)


@router.websocket("/job/{job_id}/ws")
async def job_observer_ws(websocket: WebSocket, job_id: int):
    """
    WebSocket endpoint to observe job progress.
    Polls the jobs table and sends updates to the client.
    Clients can reconnect at any time to get current status.
    """
    await websocket.accept()

    try:
        last_progress = -1
        last_status = None

        while True:
            job = get_job(job_id)

            if not job:
                await websocket.send_json({"type": "error", "message": "Job not found"})
                break

            # Send update if something changed
            if job["progress"] != last_progress or job["status"] != last_status:
                last_progress = job["progress"]
                last_status = job["status"]

                # Convert datetime objects for JSON
                response = {
                    "type": "update",
                    "job_id": job["id"],
                    "status": job["status"],
                    "progress": job["progress"],
                    "total": job["total"],
                    "message": job["message"],
                    "error_details": job["error_details"]
                }

                await websocket.send_json(response)

                # If job is done (completed, failed, or cancelled), send final message and close
                if job["status"] in ('completed', 'failed', 'cancelled'):
                    await websocket.send_json({
                        "type": "done",
                        "status": job["status"],
                        "total": job["total"] or job["progress"],
                        "message": job["message"],
                        "error_details": job["error_details"]
                    })
                    break

            # Poll interval
            await asyncio.sleep(0.5)

    except WebSocketDisconnect:
        pass  # Client disconnected, that's fine
    except Exception as e:
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except:
            pass


def scan_lines_generator(files: list, root_path: str, is_dirty: int, batch_size: int = 1000):
    """
    Generator that yields batches of lines and progress updates.
    Yields batches of (text, file_id, is_dirty) tuples and progress dicts.
    """
    count = 0
    batch = []

    for file_record in files:
        file_id = file_record["id"]
        file_name = file_record["file_name"]
        path = file_record["path"]

        if path:
            full_path = os.path.join(root_path, path, file_name)
        else:
            full_path = os.path.join(root_path, file_name)

        try:
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    clean_line = line.rstrip('\n\r')
                    # Sanitize text
                    clean_line = clean_line.encode('utf-8', errors='ignore').decode('utf-8')
                    count += 1
                    batch.append((clean_line, file_id, is_dirty))

                    if len(batch) >= batch_size:
                        yield {"type": "batch", "rows": batch}
                        batch = []
                        yield {"type": "progress", "count": count}
        except (IOError, OSError):
            pass

    # Yield remaining batch
    if batch:
        yield {"type": "batch", "rows": batch}

    yield {"type": "complete", "count": count}


@router.post("/project/{project_id}/scan/lines")
def scan_lines(request: Request, project_id: int, is_dirty: int = 1):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()

    if not project:
        cursor.close()
        conn.close()
        return RedirectResponse(url="/inventory", status_code=303)

    root_path = project["dirty_root"] if is_dirty else project["clean_root"]

    cursor.execute("SELECT id, file_name, path FROM files WHERE project_id = %s AND is_binary = FALSE AND is_dirty = %s", (project_id, is_dirty))
    files = cursor.fetchall()

    cursor.execute("""
        DELETE fr FROM file_rows fr
        JOIN files f ON fr.file_id = f.id
        WHERE f.project_id = %s AND f.is_dirty = %s
    """, (project_id, is_dirty))
    conn.commit()

    # Use the same generator as WebSocket endpoint
    inserted_lines = 0
    for update in scan_lines_generator(files, root_path, is_dirty):
        if update["type"] == "batch":
            try:
                cursor.executemany(
                    "INSERT INTO file_rows (text, file_id, is_dirty) VALUES (%s, %s, %s)",
                    update["rows"]
                )
                conn.commit()
                inserted_lines += len(update["rows"])
            except Exception:
                conn.rollback()

    # Update inventory cache with actual inserted count
    update_inventory_counts(cursor, conn, project_id, is_dirty, 'file_rows', inserted_lines)

    cursor.close()
    conn.close()

    return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)


@router.websocket("/project/{project_id}/scan/lines/ws")
async def scan_lines_ws(websocket: WebSocket, project_id: int, is_dirty: int = 1):
    import asyncio
    await websocket.accept()

    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        project = cursor.fetchone()

        if not project:
            await websocket.send_json({"type": "error", "message": "Project not found"})
            await websocket.close()
            return

        root_path = project["dirty_root"] if is_dirty else project["clean_root"]

        # Get files to scan
        cursor.execute("SELECT id, file_name, path FROM files WHERE project_id = %s AND is_binary = FALSE AND is_dirty = %s", (project_id, is_dirty))
        files = cursor.fetchall()

        # Clear existing lines for this is_dirty type
        cursor.execute("""
            DELETE fr FROM file_rows fr
            JOIN files f ON fr.file_id = f.id
            WHERE f.project_id = %s AND f.is_dirty = %s
        """, (project_id, is_dirty))
        conn.commit()

        # Send start message
        await websocket.send_json({"type": "started"})

        # Scan lines inline to properly yield control to event loop
        total_count = 0
        inserted_count = 0
        batch = []
        batch_size = 1000

        for file_record in files:
            file_id = file_record["id"]
            file_name = file_record["file_name"]
            path = file_record["path"]

            if path:
                full_path = os.path.join(root_path, path, file_name)
            else:
                full_path = os.path.join(root_path, file_name)

            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        clean_line = line.rstrip('\n\r')
                        clean_line = clean_line.encode('utf-8', errors='ignore').decode('utf-8')
                        total_count += 1
                        batch.append((clean_line, file_id, is_dirty))

                        if len(batch) >= batch_size:
                            try:
                                cursor.executemany(
                                    "INSERT INTO file_rows (text, file_id, is_dirty) VALUES (%s, %s, %s)",
                                    batch
                                )
                                conn.commit()
                                inserted_count += len(batch)
                            except Exception:
                                conn.rollback()
                            batch = []
                            await websocket.send_json({"type": "progress", "count": total_count})
                            await asyncio.sleep(0)  # Yield control to event loop
            except (IOError, OSError):
                pass

        # Insert remaining batch
        if batch:
            try:
                cursor.executemany(
                    "INSERT INTO file_rows (text, file_id, is_dirty) VALUES (%s, %s, %s)",
                    batch
                )
                conn.commit()
                inserted_count += len(batch)
            except Exception:
                conn.rollback()

        # Update inventory cache with actual inserted count
        update_inventory_counts(cursor, conn, project_id, is_dirty, 'file_rows', inserted_count)

        # Send completion with inserted count
        await websocket.send_json({"type": "done", "total": inserted_count})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except:
            pass
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


async def scan_lines_background_task(job_id: int, project_id: int, root_path: str, is_dirty: int):
    """
    Background task that scans file lines and updates the jobs table with progress.
    """
    conn = None
    cursor = None
    try:
        start_job(job_id)

        conn = get_conn()
        cursor = conn.cursor()

        # Get files to scan
        cursor.execute("SELECT id, file_name, path FROM files WHERE project_id = %s AND is_binary = FALSE AND is_dirty = %s", (project_id, is_dirty))
        files = cursor.fetchall()

        if not files:
            complete_job(job_id, total=0, message="No files to scan")
            return

        # Clear existing lines for this is_dirty type
        cursor.execute("""
            DELETE fr FROM file_rows fr
            JOIN files f ON fr.file_id = f.id
            WHERE f.project_id = %s AND f.is_dirty = %s
        """, (project_id, is_dirty))
        conn.commit()

        update_job(job_id, message=f"Scanning {len(files)} files...")

        # Scan lines
        total_count = 0
        inserted_count = 0
        batch = []
        batch_size = 1000
        last_update = 0

        for file_record in files:
            file_id = file_record["id"]
            file_name = file_record["file_name"]
            path = file_record["path"]

            if path:
                full_path = os.path.join(root_path, path, file_name)
            else:
                full_path = os.path.join(root_path, file_name)

            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        clean_line = line.rstrip('\n\r')
                        clean_line = clean_line.encode('utf-8', errors='ignore').decode('utf-8')
                        total_count += 1
                        batch.append((clean_line, file_id, is_dirty))

                        if len(batch) >= batch_size:
                            try:
                                cursor.executemany(
                                    "INSERT INTO file_rows (text, file_id, is_dirty) VALUES (%s, %s, %s)",
                                    batch
                                )
                                conn.commit()
                                inserted_count += len(batch)
                            except Exception:
                                conn.rollback()
                            batch = []

                            # Update job progress every batch
                            if total_count - last_update >= 5000:
                                update_job(job_id, progress=inserted_count, message=f"Scanned {inserted_count} lines...")
                                last_update = total_count
                                await asyncio.sleep(0)
            except (IOError, OSError):
                pass

        # Insert remaining batch
        if batch:
            try:
                cursor.executemany(
                    "INSERT INTO file_rows (text, file_id, is_dirty) VALUES (%s, %s, %s)",
                    batch
                )
                conn.commit()
                inserted_count += len(batch)
            except Exception:
                conn.rollback()

        # Update inventory cache with actual inserted count
        update_inventory_counts(cursor, conn, project_id, is_dirty, 'file_rows', inserted_count)

        # Mark job complete
        complete_job(job_id, total=inserted_count, message=f"Completed: {inserted_count} lines scanned")

    except asyncio.CancelledError:
        raise
    except Exception as e:
        fail_job(job_id, str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@router.post("/project/{project_id}/scan/lines/start")
async def start_scan_lines(project_id: int, is_dirty: int = 1):
    """
    Start a lines scan job. Returns the job_id for tracking progress.
    If a scan is already running, returns the existing job_id.
    """
    # Check for existing running job
    job_type = f"scan_lines_{is_dirty}"
    existing_job = get_running_job(job_type, project_id)
    if existing_job:
        return JSONResponse({
            "job_id": existing_job["id"],
            "status": existing_job["status"],
            "message": "Job already running",
            "existing": True
        })

    # Get project info
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()
    cursor.close()
    conn.close()

    if not project:
        return JSONResponse({"error": "Project not found"}, status_code=404)

    root_path = project["dirty_root"] if is_dirty else project["clean_root"]

    # Create the job
    job_id = create_job(job_type, project_id, message="Starting lines scan...")

    # Start background task
    run_job_in_background(
        job_id,
        scan_lines_background_task(job_id, project_id, root_path, is_dirty)
    )

    return JSONResponse({
        "job_id": job_id,
        "status": "pending",
        "message": "Job started",
        "existing": False
    })


def get_external_db_conn(db_name: str):
    """Get a connection to an external database on the same server."""
    import pymysql
    return pymysql.connect(
        host=os.environ.get("DB_HOST"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )


@router.post("/project/{project_id}/scan/tables")
def scan_tables(request: Request, project_id: int, is_dirty: int = 1):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()

    db_name = project.get("dirty_db") if is_dirty else project.get("clean_db")
    if not project or not db_name:
        cursor.close()
        conn.close()
        return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)

    # Clear existing tables for this project and is_dirty type
    cursor.execute("DELETE FROM db_tables WHERE project_id = %s AND is_dirty = %s", (project_id, is_dirty))
    conn.commit()

    table_count = 0
    try:
        # Connect to database and get tables
        ext_conn = get_external_db_conn(db_name)
        ext_cursor = ext_conn.cursor()
        ext_cursor.execute("SHOW TABLES")
        tables = ext_cursor.fetchall()
        ext_cursor.close()
        ext_conn.close()

        # Insert tables
        for table_row in tables:
            table_name = list(table_row.values())[0]
            cursor.execute(
                "INSERT INTO db_tables (table_name, project_id, is_dirty) VALUES (%s, %s, %s)",
                (table_name, project_id, is_dirty)
            )
            table_count += 1
        conn.commit()
    except Exception as e:
        pass

    # Update inventory cache
    update_inventory_counts(cursor, conn, project_id, is_dirty, 'db_tables', table_count)

    cursor.close()
    conn.close()

    return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)


@router.websocket("/project/{project_id}/scan/tables/ws")
async def scan_tables_ws(websocket: WebSocket, project_id: int, is_dirty: int = 1):
    await websocket.accept()

    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        project = cursor.fetchone()

        db_name = project.get("dirty_db") if is_dirty else project.get("clean_db")
        if not project or not db_name:
            await websocket.send_json({"type": "error", "message": "Project not found or database not set"})
            await websocket.close()
            return

        # Clear existing tables for this is_dirty type
        cursor.execute("DELETE FROM db_tables WHERE project_id = %s AND is_dirty = %s", (project_id, is_dirty))
        conn.commit()

        await websocket.send_json({"type": "started"})

        # Connect to database and get tables
        ext_conn = get_external_db_conn(db_name)
        ext_cursor = ext_conn.cursor()
        ext_cursor.execute("SHOW TABLES")
        tables = ext_cursor.fetchall()
        ext_cursor.close()
        ext_conn.close()

        count = 0
        for table_row in tables:
            table_name = list(table_row.values())[0]
            cursor.execute(
                "INSERT INTO db_tables (table_name, project_id, is_dirty) VALUES (%s, %s, %s)",
                (table_name, project_id, is_dirty)
            )
            count += 1
            await websocket.send_json({"type": "progress", "count": count})

        conn.commit()

        # Update inventory cache
        update_inventory_counts(cursor, conn, project_id, is_dirty, 'db_tables', count)

        await websocket.send_json({"type": "done", "total": count})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except:
            pass
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


async def scan_tables_background_task(job_id: int, project_id: int, db_name: str, is_dirty: int):
    """
    Background task that scans database tables and updates the jobs table with progress.
    """
    conn = None
    cursor = None
    ext_conn = None
    ext_cursor = None
    try:
        start_job(job_id)

        conn = get_conn()
        cursor = conn.cursor()

        # Clear existing tables for this is_dirty type
        cursor.execute("DELETE FROM db_tables WHERE project_id = %s AND is_dirty = %s", (project_id, is_dirty))
        conn.commit()

        update_job(job_id, message="Connecting to database...")

        # Connect to database and get tables
        ext_conn = get_external_db_conn(db_name)
        ext_cursor = ext_conn.cursor()
        ext_cursor.execute("SHOW TABLES")
        tables = ext_cursor.fetchall()

        total_tables = len(tables)
        update_job(job_id, total=total_tables, message=f"Found {total_tables} tables...")

        count = 0
        for table_row in tables:
            table_name = list(table_row.values())[0]
            cursor.execute(
                "INSERT INTO db_tables (table_name, project_id, is_dirty) VALUES (%s, %s, %s)",
                (table_name, project_id, is_dirty)
            )
            count += 1
            if count % 10 == 0:
                update_job(job_id, progress=count, message=f"Inserted {count}/{total_tables} tables...")
                await asyncio.sleep(0)

        conn.commit()

        # Update inventory cache
        update_inventory_counts(cursor, conn, project_id, is_dirty, 'db_tables', count)

        # Mark job complete
        complete_job(job_id, total=count, message=f"Completed: {count} tables scanned")

    except asyncio.CancelledError:
        raise
    except Exception as e:
        fail_job(job_id, str(e))
    finally:
        if ext_cursor:
            ext_cursor.close()
        if ext_conn:
            ext_conn.close()
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@router.post("/project/{project_id}/scan/tables/start")
async def start_scan_tables(project_id: int, is_dirty: int = 1):
    """
    Start a tables scan job. Returns the job_id for tracking progress.
    If a scan is already running, returns the existing job_id.
    """
    # Check for existing running job
    job_type = f"scan_tables_{is_dirty}"
    existing_job = get_running_job(job_type, project_id)
    if existing_job:
        return JSONResponse({
            "job_id": existing_job["id"],
            "status": existing_job["status"],
            "message": "Job already running",
            "existing": True
        })

    # Get project info
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()
    cursor.close()
    conn.close()

    if not project:
        return JSONResponse({"error": "Project not found"}, status_code=404)

    db_name = project.get("dirty_db") if is_dirty else project.get("clean_db")
    if not db_name:
        return JSONResponse({"error": "Database not configured for this project"}, status_code=400)

    # Create the job
    job_id = create_job(job_type, project_id, message="Starting tables scan...")

    # Start background task
    run_job_in_background(
        job_id,
        scan_tables_background_task(job_id, project_id, db_name, is_dirty)
    )

    return JSONResponse({
        "job_id": job_id,
        "status": "pending",
        "message": "Job started",
        "existing": False
    })


@router.post("/project/{project_id}/scan/db-rows")
def scan_db_rows(request: Request, project_id: int, is_dirty: int = 1):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()

    db_name = project.get("dirty_db") if is_dirty else project.get("clean_db")
    if not project or not db_name:
        cursor.close()
        conn.close()
        return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)

    # Get tables for this project and is_dirty type
    cursor.execute("SELECT id, table_name FROM db_tables WHERE project_id = %s AND is_dirty = %s", (project_id, is_dirty))
    tables = cursor.fetchall()

    if not tables:
        cursor.close()
        conn.close()
        return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)

    # Clear existing rows for this project's tables with matching is_dirty
    cursor.execute("""
        DELETE dtr FROM db_table_rows dtr
        JOIN db_tables dt ON dtr.table_id = dt.id
        WHERE dt.project_id = %s AND dt.is_dirty = %s
    """, (project_id, is_dirty))
    conn.commit()

    try:
        ext_conn = get_external_db_conn(db_name)
        ext_cursor = ext_conn.cursor()

        rows_to_insert = []
        for table in tables:
            table_id = table["id"]
            table_name = table["table_name"]

            try:
                ext_cursor.execute(f"SELECT * FROM `{table_name}`")
                rows = ext_cursor.fetchall()

                for row in rows:
                    for field_name, value in row.items():
                        if value is not None:
                            contents = str(value)
                            rows_to_insert.append((field_name, contents, table_id, is_dirty))
            except Exception:
                continue

        ext_cursor.close()
        ext_conn.close()

        # Batch insert
        batch_size = 500
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i+batch_size]
            cursor.executemany(
                "INSERT INTO db_table_rows (field_name, contents, table_id, is_dirty) VALUES (%s, %s, %s, %s)",
                batch
            )
            conn.commit()

        # Update inventory cache
        update_inventory_counts(cursor, conn, project_id, is_dirty, 'db_table_rows', len(rows_to_insert))

    except Exception:
        pass

    cursor.close()
    conn.close()

    return RedirectResponse(url=f"/inventory?project_id={project_id}", status_code=303)


@router.websocket("/project/{project_id}/scan/db-rows/ws")
async def scan_db_rows_ws(websocket: WebSocket, project_id: int, is_dirty: int = 1):
    import asyncio
    await websocket.accept()

    conn = None
    cursor = None
    ext_conn = None
    ext_cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        project = cursor.fetchone()

        db_name = project.get("dirty_db") if is_dirty else project.get("clean_db")
        if not project or not db_name:
            await websocket.send_json({"type": "error", "message": "Project not found or database not set"})
            await websocket.close()
            return

        # Get tables for this project and is_dirty type
        cursor.execute("SELECT id, table_name FROM db_tables WHERE project_id = %s AND is_dirty = %s", (project_id, is_dirty))
        tables = cursor.fetchall()

        if not tables:
            await websocket.send_json({"type": "error", "message": "No tables found. Scan tables first."})
            await websocket.close()
            return

        # Clear existing rows for this is_dirty type
        cursor.execute("""
            DELETE dtr FROM db_table_rows dtr
            JOIN db_tables dt ON dtr.table_id = dt.id
            WHERE dt.project_id = %s AND dt.is_dirty = %s
        """, (project_id, is_dirty))
        conn.commit()

        await websocket.send_json({"type": "started"})

        ext_conn = get_external_db_conn(db_name)
        ext_cursor = ext_conn.cursor()

        total_count = 0
        batch = []
        batch_size = 500

        for table in tables:
            table_id = table["id"]
            table_name = table["table_name"]

            try:
                ext_cursor.execute(f"SELECT * FROM `{table_name}`")
                rows = ext_cursor.fetchall()

                for row in rows:
                    for field_name, value in row.items():
                        if value is not None:
                            contents = str(value)
                            total_count += 1
                            batch.append((field_name, contents, table_id, is_dirty))

                            if len(batch) >= batch_size:
                                cursor.executemany(
                                    "INSERT INTO db_table_rows (field_name, contents, table_id, is_dirty) VALUES (%s, %s, %s, %s)",
                                    batch
                                )
                                conn.commit()
                                batch = []
                                await websocket.send_json({"type": "progress", "count": total_count})
                                await asyncio.sleep(0)
            except Exception:
                continue

        # Insert remaining batch
        if batch:
            cursor.executemany(
                "INSERT INTO db_table_rows (field_name, contents, table_id, is_dirty) VALUES (%s, %s, %s, %s)",
                batch
            )
            conn.commit()

        # Update inventory cache
        update_inventory_counts(cursor, conn, project_id, is_dirty, 'db_table_rows', total_count)

        await websocket.send_json({"type": "done", "total": total_count})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except:
            pass
    finally:
        if ext_cursor:
            ext_cursor.close()
        if ext_conn:
            ext_conn.close()
        if cursor:
            cursor.close()
        if conn:
            conn.close()


async def scan_db_rows_background_task(job_id: int, project_id: int, db_name: str, is_dirty: int):
    """
    Background task that scans database rows and updates the jobs table with progress.
    """
    conn = None
    cursor = None
    ext_conn = None
    ext_cursor = None
    try:
        start_job(job_id)

        conn = get_conn()
        cursor = conn.cursor()

        # Get tables for this project and is_dirty type
        cursor.execute("SELECT id, table_name FROM db_tables WHERE project_id = %s AND is_dirty = %s", (project_id, is_dirty))
        tables = cursor.fetchall()

        if not tables:
            fail_job(job_id, "No tables found. Scan tables first.")
            return

        # Clear existing rows for this is_dirty type
        cursor.execute("""
            DELETE dtr FROM db_table_rows dtr
            JOIN db_tables dt ON dtr.table_id = dt.id
            WHERE dt.project_id = %s AND dt.is_dirty = %s
        """, (project_id, is_dirty))
        conn.commit()

        update_job(job_id, message=f"Scanning {len(tables)} tables...")

        ext_conn = get_external_db_conn(db_name)
        ext_cursor = ext_conn.cursor()

        total_count = 0
        batch = []
        batch_size = 500
        last_update = 0

        for table in tables:
            table_id = table["id"]
            table_name = table["table_name"]

            try:
                ext_cursor.execute(f"SELECT * FROM `{table_name}`")
                rows = ext_cursor.fetchall()

                for row in rows:
                    for field_name, value in row.items():
                        if value is not None:
                            contents = str(value)
                            total_count += 1
                            batch.append((field_name, contents, table_id, is_dirty))

                            if len(batch) >= batch_size:
                                cursor.executemany(
                                    "INSERT INTO db_table_rows (field_name, contents, table_id, is_dirty) VALUES (%s, %s, %s, %s)",
                                    batch
                                )
                                conn.commit()
                                batch = []

                                # Update job progress every 5000 rows
                                if total_count - last_update >= 5000:
                                    update_job(job_id, progress=total_count, message=f"Scanned {total_count} rows...")
                                    last_update = total_count
                                    await asyncio.sleep(0)
            except Exception:
                continue

        # Insert remaining batch
        if batch:
            cursor.executemany(
                "INSERT INTO db_table_rows (field_name, contents, table_id, is_dirty) VALUES (%s, %s, %s, %s)",
                batch
            )
            conn.commit()

        # Update inventory cache
        update_inventory_counts(cursor, conn, project_id, is_dirty, 'db_table_rows', total_count)

        # Mark job complete
        complete_job(job_id, total=total_count, message=f"Completed: {total_count} rows scanned")

    except asyncio.CancelledError:
        raise
    except Exception as e:
        fail_job(job_id, str(e))
    finally:
        if ext_cursor:
            ext_cursor.close()
        if ext_conn:
            ext_conn.close()
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def populate_branches_for_dirty_type(cursor, conn, project_id: int, is_dirty: int):
    """
    Populate branches for a specific is_dirty type (0=clean, 1=dirty).
    Returns the number of branches created.
    Excludes quarantine folder for dirty files.
    """
    # Get aggregated counts for each path in a single query
    # Exclude quarantine paths for dirty files
    if is_dirty:
        cursor.execute("""
            SELECT
                path,
                COUNT(*) as total,
                SUM(CASE WHEN status = 'valid' THEN 1 ELSE 0 END) as valids,
                SUM(CASE WHEN status = 'bad' THEN 1 ELSE 0 END) as bads,
                SUM(CASE WHEN status = 'mixed' THEN 1 ELSE 0 END) as mixeds,
                SUM(CASE WHEN status = 'research' THEN 1 ELSE 0 END) as researchs,
                SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) as nulls
            FROM files
            WHERE project_id = %s AND is_dirty = %s
            AND path != 'quarantine' AND path NOT LIKE 'quarantine/%%'
            GROUP BY path
        """, (project_id, is_dirty))
    else:
        cursor.execute("""
            SELECT
                path,
                COUNT(*) as total,
                SUM(CASE WHEN status = 'valid' THEN 1 ELSE 0 END) as valids,
                SUM(CASE WHEN status = 'bad' THEN 1 ELSE 0 END) as bads,
                SUM(CASE WHEN status = 'mixed' THEN 1 ELSE 0 END) as mixeds,
                SUM(CASE WHEN status = 'research' THEN 1 ELSE 0 END) as researchs,
                SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) as nulls
            FROM files
            WHERE project_id = %s AND is_dirty = %s
            GROUP BY path
        """, (project_id, is_dirty))
    path_counts = {row["path"]: row for row in cursor.fetchall()}

    if not path_counts:
        return 0

    # Build set of all paths including parent paths
    all_paths = set()
    for path in path_counts.keys():
        all_paths.add(path)
        parts = path.split('/') if path else []
        for i in range(len(parts)):
            parent = '/'.join(parts[:i])
            all_paths.add(parent)

    # Build child relationships for sub_folder counting
    # We need to iterate over ALL paths (not just path_counts) to properly build the tree
    direct_children = {}  # parent_path -> set of direct child folder names
    for path in all_paths:
        if path == '':
            continue
        parts = path.split('/')
        # For each level, record the direct child
        for i in range(len(parts)):
            parent = '/'.join(parts[:i]) if i > 0 else ''
            child_name = parts[i]
            if parent not in direct_children:
                direct_children[parent] = set()
            direct_children[parent].add(child_name)

    # Calculate cumulative counts for each path (including subfolders)
    # Sort paths by number of components (deepest first) so we can aggregate up
    # Using len(split('/')) ensures root '' (0 components) is processed last
    sorted_paths = sorted(all_paths, key=lambda p: len(p.split('/')) if p else 0, reverse=True)

    cumulative = {}
    for path in sorted_paths:
        # Start with direct counts for this path
        if path in path_counts:
            row = path_counts[path]
            cumulative[path] = {
                'files': row['total'],
                'valids': row['valids'] or 0,
                'bads': row['bads'] or 0,
                'mixeds': row['mixeds'] or 0,
                'researchs': row['researchs'] or 0,
                'nulls': row['nulls'] or 0
            }
        else:
            cumulative[path] = {
                'files': 0, 'valids': 0, 'bads': 0,
                'mixeds': 0, 'researchs': 0, 'nulls': 0
            }

        # Add counts from direct child paths
        if path in direct_children:
            for child_name in direct_children[path]:
                child_path = f"{path}/{child_name}" if path else child_name
                if child_path in cumulative:
                    for key in cumulative[path]:
                        cumulative[path][key] += cumulative[child_path][key]

    # Build branches data
    branches_data = []
    for path in all_paths:
        sub_folders = len(direct_children.get(path, set()))
        counts = cumulative[path]

        # Calculate is_root: true if none of the path components exist as paths in all_paths
        # e.g., 'gymji' is root if no other row has path='gymji' as a parent
        # 'gymji/ckeditor' is NOT root because 'gymji' exists as a path
        is_root = 1
        if path:
            parts = path.split('/')
            for part in parts:
                if part in all_paths and part != path:
                    is_root = 0
                    break

        # Calculate homogeneous: true if only one category has a positive value
        category_values = [counts['valids'], counts['bads'], counts['mixeds'], counts['researchs'], counts['nulls']]
        positive_categories = sum(1 for v in category_values if v > 0)
        homogeneous = 1 if positive_categories <= 1 else 0

        branches_data.append((
            project_id,
            is_dirty,
            path,
            sub_folders,
            counts['files'],
            counts['valids'],
            counts['bads'],
            counts['mixeds'],
            counts['researchs'],
            counts['nulls'],
            is_root,
            homogeneous
        ))

    # Batch insert branches
    if branches_data:
        cursor.executemany("""
            INSERT INTO branches (project_id, is_dirty, path, sub_folders, files, valids, bads, mixeds, researchs, nulls, is_root, homogeneous)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, branches_data)
        conn.commit()

    return len(branches_data)


def has_multiple_categories(row):
    """Check if a branch has more than one category with positive values."""
    categories = [row['valids'], row['bads'], row['mixeds'], row['researchs'], row['nulls']]
    positive_count = sum(1 for c in categories if c and c > 0)
    return positive_count > 1


def expand_roots(cursor, conn, project_id: int):
    """
    Recursively expand is_root=true deeper into the tree.
    For branches where is_root=true but they have multiple categories with positive values,
    mark their direct children as is_root=true as well.
    Keep going until each is_root branch has only one category with a positive value.
    """
    while True:
        # Find is_root branches with multiple positive categories
        cursor.execute("""
            SELECT id, path, is_dirty, valids, bads, mixeds, researchs, nulls
            FROM branches
            WHERE project_id = %s AND is_root = 1
        """, (project_id,))
        root_branches = cursor.fetchall()

        # Filter to those with multiple categories
        mixed_roots = [r for r in root_branches if has_multiple_categories(r)]

        if not mixed_roots:
            break  # No more to expand

        expanded_any = False
        for root in mixed_roots:
            root_path = root['path']
            is_dirty = root['is_dirty']

            # Find direct children (one level deeper)
            if root_path == '':
                # Root level: children are paths with no '/'
                cursor.execute("""
                    SELECT id, path FROM branches
                    WHERE project_id = %s AND is_dirty = %s
                    AND path != '' AND path NOT LIKE '%%/%%'
                    AND is_root = 0
                """, (project_id, is_dirty))
            else:
                # Find paths that start with root_path/ and have exactly one more component
                prefix = root_path + '/'
                cursor.execute("""
                    SELECT id, path FROM branches
                    WHERE project_id = %s AND is_dirty = %s
                    AND path LIKE %s
                    AND path NOT LIKE %s
                    AND is_root = 0
                """, (project_id, is_dirty, prefix + '%', prefix + '%/%'))

            children = cursor.fetchall()

            if children:
                # Mark children as is_root=true
                child_ids = [c['id'] for c in children]
                cursor.execute(
                    f"UPDATE branches SET is_root = 1 WHERE id IN ({','.join(['%s'] * len(child_ids))})",
                    child_ids
                )
                conn.commit()
                expanded_any = True

        if not expanded_any:
            break  # No children found to expand


def populate_branches(project_id: int):
    """
    Populate the branches table with aggregated counts for each unique path.
    Handles both dirty (is_dirty=1) and clean (is_dirty=0) files.
    For each path, counts:
    - sub_folders: number of direct child folders
    - files: total files in this folder and all subfolders
    - valids, bads, mixeds, researchs, nulls: file counts by status

    Uses an efficient single-query approach to get all file counts,
    then aggregates in Python for parent folders.

    After initial population, recursively expands is_root deeper into the tree
    until each root branch has only one category with a positive value.
    """
    conn = get_conn()
    cursor = conn.cursor()

    try:
        # Clear existing branches for this project
        cursor.execute("DELETE FROM branches WHERE project_id = %s", (project_id,))
        conn.commit()

        # Populate branches for both dirty and clean files
        dirty_count = populate_branches_for_dirty_type(cursor, conn, project_id, 1)
        clean_count = populate_branches_for_dirty_type(cursor, conn, project_id, 0)

        # Expand is_root deeper for branches with multiple categories
        expand_roots(cursor, conn, project_id)

        return dirty_count + clean_count

    finally:
        cursor.close()
        conn.close()


@router.post("/project/{project_id}/populate-branches")
def populate_branches_endpoint(project_id: int):
    """API endpoint to populate branches for a project."""
    try:
        count = populate_branches(project_id)
        return JSONResponse({"success": True, "branches_count": count})
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@router.post("/project/{project_id}/scan/db-rows/start")
async def start_scan_db_rows(project_id: int, is_dirty: int = 1):
    """
    Start a database rows scan job. Returns the job_id for tracking progress.
    If a scan is already running, returns the existing job_id.
    """
    # Check for existing running job
    job_type = f"scan_db_rows_{is_dirty}"
    existing_job = get_running_job(job_type, project_id)
    if existing_job:
        return JSONResponse({
            "job_id": existing_job["id"],
            "status": existing_job["status"],
            "message": "Job already running",
            "existing": True
        })

    # Get project info
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()
    cursor.close()
    conn.close()

    if not project:
        return JSONResponse({"error": "Project not found"}, status_code=404)

    db_name = project.get("dirty_db") if is_dirty else project.get("clean_db")
    if not db_name:
        return JSONResponse({"error": "Database not configured for this project"}, status_code=400)

    # Create the job
    job_id = create_job(job_type, project_id, message="Starting database rows scan...")

    # Start background task
    run_job_in_background(
        job_id,
        scan_db_rows_background_task(job_id, project_id, db_name, is_dirty)
    )

    return JSONResponse({
        "job_id": job_id,
        "status": "pending",
        "message": "Job started",
        "existing": False
    })
