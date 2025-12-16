from fastapi import APIRouter, Request, Form, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from app.db import get_conn
from datetime import datetime
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
    """
    files_to_insert = []
    count = 0

    for root, dirs, files in os.walk(root_path):
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


@router.websocket("/project/{project_id}/scan/files/ws")
async def scan_files_ws(websocket: WebSocket, project_id: int, is_dirty: int = 1):
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

        # Clear existing files for this is_dirty type only
        cursor.execute("DELETE FROM files WHERE project_id = %s AND is_dirty = %s", (project_id, is_dirty))
        conn.commit()

        # Send start message
        await websocket.send_json({"type": "started"})

        # Scan files and send progress
        files_to_insert = []
        for update in scan_files_generator(project_id, root_path, is_dirty):
            if update["type"] == "progress":
                await websocket.send_json(update)
            elif update["type"] == "complete":
                files_to_insert = update["files"]
                await websocket.send_json({"type": "progress", "count": update["count"]})

        # Batch insert
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

        # Send completion
        await websocket.send_json({"type": "done", "total": len(files_to_insert)})

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
