from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from app.db import get_conn
from app.jobs import (
    create_job, update_job, get_job, get_running_job, get_latest_completed_job,
    start_job, complete_job, fail_job, run_job_in_background
)
import asyncio

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/training")
def training(request: Request, project_id: int = None, data_type: str = "files"):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT id, name FROM projects ORDER BY name")
    projects = cursor.fetchall()

    project = None
    # Initialize stats structure for all four boxes
    stats = {
        "files": {"total": 0, "binary": 0, "code": 0, "valid": 0, "mixed": 0, "research": 0},
        "lines": {"total": 0, "binary": 0, "code": 0, "valid": 0, "mixed": 0, "research": 0},
        "tables": {"total": 0, "binary": 0, "data": 0, "valid": 0, "mixed": 0, "research": 0},
        "rows": {"total": 0, "binary": 0, "data": 0, "valid": 0, "mixed": 0, "research": 0},
    }

    if project_id:
        cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        project = cursor.fetchone()

        # Get all file stats in one query
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN is_binary = 1 THEN 1 ELSE 0 END) as binary_cnt,
                SUM(CASE WHEN is_binary = 0 THEN 1 ELSE 0 END) as code_cnt,
                SUM(CASE WHEN status = 'valid' THEN 1 ELSE 0 END) as valid_cnt,
                SUM(CASE WHEN status = 'mixed' THEN 1 ELSE 0 END) as mixed_cnt,
                SUM(CASE WHEN status = 'research' THEN 1 ELSE 0 END) as research_cnt
            FROM files WHERE project_id = %s AND is_dirty = 1
        """, (project_id,))
        file_stats = cursor.fetchone()
        stats["files"]["total"] = file_stats["total"] or 0
        stats["files"]["binary"] = file_stats["binary_cnt"] or 0
        stats["files"]["code"] = file_stats["code_cnt"] or 0
        stats["files"]["valid"] = file_stats["valid_cnt"] or 0
        stats["files"]["mixed"] = file_stats["mixed_cnt"] or 0
        stats["files"]["research"] = file_stats["research_cnt"] or 0

        # Get all line stats in one query
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN fr.status = 'valid' THEN 1 ELSE 0 END) as valid_cnt,
                SUM(CASE WHEN fr.status = 'research' THEN 1 ELSE 0 END) as research_cnt
            FROM file_rows fr
            JOIN files f ON fr.file_id = f.id
            WHERE f.project_id = %s AND fr.is_dirty = 1
        """, (project_id,))
        line_stats = cursor.fetchone()
        stats["lines"]["total"] = line_stats["total"] or 0
        stats["lines"]["code"] = stats["lines"]["total"]
        stats["lines"]["valid"] = line_stats["valid_cnt"] or 0
        stats["lines"]["research"] = line_stats["research_cnt"] or 0

        # Get all table stats in one query
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'valid' THEN 1 ELSE 0 END) as valid_cnt,
                SUM(CASE WHEN status = 'mixed' THEN 1 ELSE 0 END) as mixed_cnt,
                SUM(CASE WHEN status = 'research' THEN 1 ELSE 0 END) as research_cnt
            FROM db_tables WHERE project_id = %s AND is_dirty = 1
        """, (project_id,))
        table_stats = cursor.fetchone()
        stats["tables"]["total"] = table_stats["total"] or 0
        stats["tables"]["data"] = stats["tables"]["total"]
        stats["tables"]["valid"] = table_stats["valid_cnt"] or 0
        stats["tables"]["mixed"] = table_stats["mixed_cnt"] or 0
        stats["tables"]["research"] = table_stats["research_cnt"] or 0

        # Get all db row stats in one query
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN dr.status = 'valid' THEN 1 ELSE 0 END) as valid_cnt,
                SUM(CASE WHEN dr.status = 'research' THEN 1 ELSE 0 END) as research_cnt
            FROM db_table_rows dr
            JOIN db_tables t ON dr.table_id = t.id
            WHERE t.project_id = %s AND dr.is_dirty = 1
        """, (project_id,))
        row_stats = cursor.fetchone()
        stats["rows"]["total"] = row_stats["total"] or 0
        stats["rows"]["data"] = stats["rows"]["total"]
        stats["rows"]["valid"] = row_stats["valid_cnt"] or 0
        stats["rows"]["research"] = row_stats["research_cnt"] or 0

    # Manual training data
    manual_train = {
        "dirty_file": None,
        "clean_file": None,
        "dirty_lines": [],
        "clean_lines": [],
        "has_clean_match": False,
    }

    if project_id and data_type == "files":
        conn = get_conn()
        cursor = conn.cursor()

        # Find first dirty file with status != 'valid' (bad, mixed, research, or NULL)
        cursor.execute("""
            SELECT id, file_name, path, status
            FROM files
            WHERE project_id = %s AND is_dirty = 1 AND (status IS NULL OR status != 'valid')
            ORDER BY id
            LIMIT 1
        """, (project_id,))
        dirty_file = cursor.fetchone()

        if dirty_file:
            manual_train["dirty_file"] = dirty_file

            # Get all lines from dirty file
            cursor.execute("""
                SELECT id, text, status
                FROM file_rows
                WHERE file_id = %s
                ORDER BY id
            """, (dirty_file["id"],))
            manual_train["dirty_lines"] = cursor.fetchall()

            # Find matching clean file
            cursor.execute("""
                SELECT id, file_name, path
                FROM files
                WHERE project_id = %s AND is_dirty = 0
                    AND file_name = %s AND path = %s
                LIMIT 1
            """, (project_id, dirty_file["file_name"], dirty_file["path"]))
            clean_file = cursor.fetchone()

            if clean_file:
                manual_train["clean_file"] = clean_file
                manual_train["has_clean_match"] = True

                # Get all lines from clean file
                cursor.execute("""
                    SELECT id, text
                    FROM file_rows
                    WHERE file_id = %s
                    ORDER BY id
                """, (clean_file["id"],))
                manual_train["clean_lines"] = cursor.fetchall()

        cursor.close()
        conn.close()

    return templates.TemplateResponse(
        "training.html",
        {
            "request": request,
            "projects": projects,
            "project": project,
            "selected_data_type": data_type,
            "stats": stats,
            "manual_train": manual_train,
        }
    )


def _run_auto_train_sync(project_id: int, progress_callback):
    """
    Synchronous auto-training work. Called via asyncio.to_thread() to avoid blocking.

    For files: compares dirty files to clean files with matching name/path.
    - If no matching clean file exists -> file status = 'research'
    - If matching clean file exists -> compare rows line by line:
      - Matching rows -> row status = 'valid'
      - Non-matching rows -> row status = 'research'
      - If ALL rows match -> file status = 'valid'
      - Otherwise -> file status = 'mixed'

    progress_callback(phase, progress_data) is called periodically with progress updates.
    """
    conn = get_conn()
    cursor = conn.cursor()

    # Get counts for progress tracking
    cursor.execute(
        "SELECT COUNT(*) as cnt FROM files WHERE project_id = %s AND is_dirty = 1",
        (project_id,)
    )
    total_dirty_files = cursor.fetchone()["cnt"]

    cursor.execute(
        "SELECT COUNT(*) as cnt FROM files WHERE project_id = %s AND is_dirty = 1 AND is_binary = 1",
        (project_id,)
    )
    binary_files = cursor.fetchone()["cnt"]

    cursor.execute(
        "SELECT COUNT(*) as cnt FROM files WHERE project_id = %s AND is_dirty = 1 AND is_binary = 0",
        (project_id,)
    )
    code_files = cursor.fetchone()["cnt"]

    cursor.execute(
        "SELECT COUNT(*) as cnt FROM file_rows fr "
        "JOIN files f ON fr.file_id = f.id "
        "WHERE f.project_id = %s AND fr.is_dirty = 1",
        (project_id,)
    )
    total_dirty_lines = cursor.fetchone()["cnt"]

    # Get counts for database tables and rows
    cursor.execute(
        "SELECT COUNT(*) as cnt FROM db_tables WHERE project_id = %s AND is_dirty = 1",
        (project_id,)
    )
    total_dirty_tables = cursor.fetchone()["cnt"]

    cursor.execute(
        "SELECT COUNT(*) as cnt FROM db_table_rows dr "
        "JOIN db_tables t ON dr.table_id = t.id "
        "WHERE t.project_id = %s AND dr.is_dirty = 1",
        (project_id,)
    )
    total_dirty_db_rows = cursor.fetchone()["cnt"]

    # Progress counters for file statuses
    files_valid = 0
    files_mixed = 0
    files_research = 0
    lines_valid = 0
    lines_research = 0

    # Progress counters for table statuses
    tables_valid = 0
    tables_mixed = 0
    tables_research = 0
    db_rows_valid = 0
    db_rows_research = 0

    def make_progress_data():
        return {
            "files": {
                "total": total_dirty_files,
                "binary": binary_files,
                "code": code_files,
                "valid": files_valid,
                "mixed": files_mixed,
                "research": files_research
            },
            "lines": {
                "total": total_dirty_lines,
                "binary": 0,
                "code": total_dirty_lines,
                "valid": lines_valid,
                "mixed": 0,
                "research": lines_research
            },
            "tables": {
                "total": total_dirty_tables,
                "binary": 0,
                "data": total_dirty_tables,
                "valid": tables_valid,
                "mixed": tables_mixed,
                "research": tables_research
            },
            "rows": {
                "total": total_dirty_db_rows,
                "binary": 0,
                "data": total_dirty_db_rows,
                "valid": db_rows_valid,
                "mixed": 0,
                "research": db_rows_research
            },
        }

    progress_callback("init", 0, make_progress_data())

    # PHASE 1: Mark files without clean counterparts as 'research' (bulk operation)
    cursor.execute("""
        UPDATE files d
        LEFT JOIN files c ON c.project_id = d.project_id
            AND c.is_dirty = 0
            AND c.file_name = d.file_name
            AND c.path = d.path
        SET d.status = 'research'
        WHERE d.project_id = %s AND d.is_dirty = 1 AND c.id IS NULL
    """, (project_id,))
    research_files_count = cursor.rowcount
    conn.commit()

    # Mark all rows of research files as research
    cursor.execute("""
        UPDATE file_rows fr
        JOIN files f ON fr.file_id = f.id
        SET fr.status = 'research'
        WHERE f.project_id = %s AND f.is_dirty = 1 AND f.status = 'research'
    """, (project_id,))
    research_lines_count = cursor.rowcount
    conn.commit()

    files_research = research_files_count
    lines_research = research_lines_count

    files_processed = research_files_count
    files_pct = round((files_processed / total_dirty_files) * 100) if total_dirty_files > 0 else 0
    # Files are 0-50% of overall progress, tables are 50-100%
    overall_pct = round((files_processed / total_dirty_files) * 50) if total_dirty_files > 0 else 0
    progress_callback("files_phase1", overall_pct, make_progress_data())

    # PHASE 2: Get dirty files that have clean counterparts

    cursor.execute("""
        SELECT d.id as dirty_id, c.id as clean_id
        FROM files d
        JOIN files c ON c.project_id = d.project_id
            AND c.is_dirty = 0
            AND c.file_name = d.file_name
            AND c.path = d.path
        WHERE d.project_id = %s AND d.is_dirty = 1 AND d.status IS NULL
    """, (project_id,))
    file_pairs = cursor.fetchall()

    BATCH_SIZE = 100  # Process 100 file pairs at a time

    for batch_start in range(0, len(file_pairs), BATCH_SIZE):
        batch = file_pairs[batch_start:batch_start + BATCH_SIZE]

        valid_file_ids = []
        mixed_file_ids = []

        for pair in batch:
            dirty_id = pair["dirty_id"]
            clean_id = pair["clean_id"]

            # Get rows from both files
            cursor.execute(
                "SELECT id, text FROM file_rows WHERE file_id = %s ORDER BY id",
                (dirty_id,)
            )
            dirty_rows = cursor.fetchall()

            cursor.execute(
                "SELECT text FROM file_rows WHERE file_id = %s ORDER BY id",
                (clean_id,)
            )
            clean_rows = cursor.fetchall()

            # Compare rows
            all_match = len(dirty_rows) == len(clean_rows) and len(dirty_rows) > 0
            valid_ids = []
            research_ids = []

            for i, dirty_row in enumerate(dirty_rows):
                if i < len(clean_rows) and dirty_row["text"] == clean_rows[i]["text"]:
                    valid_ids.append(dirty_row["id"])
                    lines_valid += 1
                else:
                    research_ids.append(dirty_row["id"])
                    lines_research += 1
                    all_match = False

            # Bulk update rows for this file
            if valid_ids:
                cursor.execute(
                    f"UPDATE file_rows SET status = 'valid' WHERE id IN ({','.join(map(str, valid_ids))})"
                )
            if research_ids:
                cursor.execute(
                    f"UPDATE file_rows SET status = 'research' WHERE id IN ({','.join(map(str, research_ids))})"
                )

            if all_match:
                valid_file_ids.append(dirty_id)
                files_valid += 1
            else:
                mixed_file_ids.append(dirty_id)
                files_mixed += 1

            files_processed += 1

        # Update file statuses for this batch
        if valid_file_ids:
            cursor.execute(
                f"UPDATE files SET status = 'valid' WHERE id IN ({','.join(map(str, valid_file_ids))})"
            )
        if mixed_file_ids:
            cursor.execute(
                f"UPDATE files SET status = 'mixed' WHERE id IN ({','.join(map(str, mixed_file_ids))})"
            )
        conn.commit()

        # Update progress (files are 0-50% of overall progress)
        overall_pct = round((files_processed / total_dirty_files) * 50) if total_dirty_files > 0 else 50
        progress_callback("files_phase2", overall_pct, make_progress_data())

    # PHASE 3: Mark tables without clean counterparts as 'research' (bulk operation)
    cursor.execute("""
        UPDATE db_tables d
        LEFT JOIN db_tables c ON c.project_id = d.project_id
            AND c.is_dirty = 0
            AND c.table_name = d.table_name
        SET d.status = 'research'
        WHERE d.project_id = %s AND d.is_dirty = 1 AND c.id IS NULL
    """, (project_id,))
    research_tables_count = cursor.rowcount
    conn.commit()

    # Mark all rows of research tables as research
    cursor.execute("""
        UPDATE db_table_rows dr
        JOIN db_tables t ON dr.table_id = t.id
        SET dr.status = 'research'
        WHERE t.project_id = %s AND t.is_dirty = 1 AND t.status = 'research'
    """, (project_id,))
    research_db_rows_count = cursor.rowcount
    conn.commit()

    tables_research = research_tables_count
    db_rows_research = research_db_rows_count

    tables_processed = research_tables_count
    # Update overall progress (files are 50%, tables are 50%)
    files_overall = 50  # Files already complete
    tables_overall = round((tables_processed / total_dirty_tables) * 50) if total_dirty_tables > 0 else 50
    progress_callback("tables_phase1", files_overall + tables_overall, make_progress_data())

    # PHASE 4: Get dirty tables that have clean counterparts
    cursor.execute("""
        SELECT d.id as dirty_id, c.id as clean_id
        FROM db_tables d
        JOIN db_tables c ON c.project_id = d.project_id
            AND c.is_dirty = 0
            AND c.table_name = d.table_name
        WHERE d.project_id = %s AND d.is_dirty = 1 AND d.status IS NULL
    """, (project_id,))
    table_pairs = cursor.fetchall()

    TABLE_BATCH_SIZE = 50  # Process 50 table pairs at a time

    for batch_start in range(0, len(table_pairs), TABLE_BATCH_SIZE):
        batch = table_pairs[batch_start:batch_start + TABLE_BATCH_SIZE]

        valid_table_ids = []
        mixed_table_ids = []

        for pair in batch:
            dirty_id = pair["dirty_id"]
            clean_id = pair["clean_id"]

            # Get rows from both tables (compare by field_name and contents)
            cursor.execute(
                "SELECT id, field_name, contents FROM db_table_rows WHERE table_id = %s ORDER BY id",
                (dirty_id,)
            )
            dirty_rows = cursor.fetchall()

            cursor.execute(
                "SELECT field_name, contents FROM db_table_rows WHERE table_id = %s ORDER BY id",
                (clean_id,)
            )
            clean_rows = cursor.fetchall()

            # Build a set of clean row signatures for comparison
            clean_signatures = set()
            for row in clean_rows:
                clean_signatures.add((row["field_name"], row["contents"]))

            # Compare rows
            all_match = len(dirty_rows) == len(clean_rows) and len(dirty_rows) > 0
            valid_ids = []
            research_ids = []

            for dirty_row in dirty_rows:
                sig = (dirty_row["field_name"], dirty_row["contents"])
                if sig in clean_signatures:
                    valid_ids.append(dirty_row["id"])
                    db_rows_valid += 1
                else:
                    research_ids.append(dirty_row["id"])
                    db_rows_research += 1
                    all_match = False

            # Bulk update rows for this table
            if valid_ids:
                cursor.execute(
                    f"UPDATE db_table_rows SET status = 'valid' WHERE id IN ({','.join(map(str, valid_ids))})"
                )
            if research_ids:
                cursor.execute(
                    f"UPDATE db_table_rows SET status = 'research' WHERE id IN ({','.join(map(str, research_ids))})"
                )

            if all_match:
                valid_table_ids.append(dirty_id)
                tables_valid += 1
            else:
                mixed_table_ids.append(dirty_id)
                tables_mixed += 1

            tables_processed += 1

        # Update table statuses for this batch
        if valid_table_ids:
            cursor.execute(
                f"UPDATE db_tables SET status = 'valid' WHERE id IN ({','.join(map(str, valid_table_ids))})"
            )
        if mixed_table_ids:
            cursor.execute(
                f"UPDATE db_tables SET status = 'mixed' WHERE id IN ({','.join(map(str, mixed_table_ids))})"
            )
        conn.commit()

        # Update progress
        files_overall = 50  # Files already complete
        tables_overall = round((tables_processed / total_dirty_tables) * 50) if total_dirty_tables > 0 else 50
        progress_callback("tables_phase2", files_overall + tables_overall, make_progress_data())

    cursor.close()
    conn.close()

    # Return final results
    return {
        "files_valid": files_valid,
        "files_mixed": files_mixed,
        "files_research": files_research,
        "lines_valid": lines_valid,
        "lines_research": lines_research,
        "tables_valid": tables_valid,
        "tables_mixed": tables_mixed,
        "tables_research": tables_research,
        "db_rows_valid": db_rows_valid,
        "db_rows_research": db_rows_research,
        "total_dirty_files": total_dirty_files,
        "total_dirty_lines": total_dirty_lines,
        "total_dirty_tables": total_dirty_tables,
        "total_dirty_db_rows": total_dirty_db_rows,
    }


async def auto_train_background_task(
    job_id: int,
    project_id: int,
    files_job_id: int,
    lines_job_id: int,
    tables_job_id: int,
    rows_job_id: int
):
    """
    Background task that runs auto-training. Uses asyncio.to_thread() to run
    synchronous database operations without blocking the event loop.
    """
    import json
    from queue import Queue
    import threading

    try:
        start_job(job_id)
        start_job(files_job_id)
        start_job(lines_job_id)
        start_job(tables_job_id)
        start_job(rows_job_id)

        # Queue for receiving progress updates from the sync thread
        progress_queue = Queue()

        def progress_callback(phase, overall_pct, progress_data):
            """Called from sync thread to queue progress updates."""
            progress_queue.put((phase, overall_pct, progress_data))

        # Start a task to process progress updates from the queue
        async def process_progress_updates():
            """Process progress updates from the sync thread."""
            while True:
                # Check queue periodically without blocking
                await asyncio.sleep(0.1)
                while not progress_queue.empty():
                    try:
                        phase, overall_pct, progress_data = progress_queue.get_nowait()

                        update_job(job_id, progress=overall_pct, message=json.dumps({"data": progress_data}))

                        files_pct = round((progress_data["files"]["valid"] + progress_data["files"]["mixed"] + progress_data["files"]["research"]) / progress_data["files"]["total"] * 100) if progress_data["files"]["total"] > 0 else 100
                        lines_pct = round((progress_data["lines"]["valid"] + progress_data["lines"]["research"]) / progress_data["lines"]["total"] * 100) if progress_data["lines"]["total"] > 0 else 100
                        tables_pct = round((progress_data["tables"]["valid"] + progress_data["tables"]["mixed"] + progress_data["tables"]["research"]) / progress_data["tables"]["total"] * 100) if progress_data["tables"]["total"] > 0 else 100
                        rows_pct = round((progress_data["rows"]["valid"] + progress_data["rows"]["research"]) / progress_data["rows"]["total"] * 100) if progress_data["rows"]["total"] > 0 else 100

                        update_job(files_job_id, progress=files_pct, total=100, message=json.dumps(progress_data["files"]))
                        update_job(lines_job_id, progress=lines_pct, total=100, message=json.dumps(progress_data["lines"]))
                        update_job(tables_job_id, progress=tables_pct, total=100, message=json.dumps(progress_data["tables"]))
                        update_job(rows_job_id, progress=rows_pct, total=100, message=json.dumps(progress_data["rows"]))
                    except Exception:
                        pass

        # Create progress update task
        progress_task = asyncio.create_task(process_progress_updates())

        try:
            # Run the sync database work in a thread pool
            result = await asyncio.to_thread(_run_auto_train_sync, project_id, progress_callback)
        finally:
            # Cancel the progress update task
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass

        # Complete all jobs with final results
        complete_job(job_id, total=100, message="Training completed successfully")
        complete_job(files_job_id, total=result["total_dirty_files"],
                     message=f"Files: {result['files_valid']} valid, {result['files_mixed']} mixed, {result['files_research']} research")
        complete_job(lines_job_id, total=result["total_dirty_lines"],
                     message=f"Lines: {result['lines_valid']} valid, {result['lines_research']} research")
        complete_job(tables_job_id, total=result["total_dirty_tables"],
                     message=f"Tables: {result['tables_valid']} valid, {result['tables_mixed']} mixed, {result['tables_research']} research")
        complete_job(rows_job_id, total=result["total_dirty_db_rows"],
                     message=f"Rows: {result['db_rows_valid']} valid, {result['db_rows_research']} research")

    except asyncio.CancelledError:
        raise
    except Exception as e:
        fail_job(job_id, str(e))
        for sub_job_id in [files_job_id, lines_job_id, tables_job_id, rows_job_id]:
            fail_job(sub_job_id, str(e))


@router.post("/project/{project_id}/auto-train/start")
async def start_auto_train(project_id: int):
    """
    Start an auto-train job. Returns the job_id for tracking progress.
    Creates 5 jobs: main auto_train plus auto_files, auto_lines, auto_tables, auto_rows.
    If training is already running, returns the existing job_id.
    """
    # Check for existing running job
    job_type = f"auto_train_{project_id}"
    existing_job = get_running_job(job_type, project_id)
    if existing_job:
        return JSONResponse({
            "job_id": existing_job["id"],
            "status": existing_job["status"],
            "message": "Job already running",
            "existing": True
        })

    # Verify project exists
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()
    cursor.close()
    conn.close()

    if not project:
        return JSONResponse({"error": "Project not found"}, status_code=404)

    # Create the main job
    job_id = create_job(job_type, project_id, message="Starting auto-train...")

    # Create the 4 sub-jobs for each data type
    files_job_id = create_job(f"auto_files_{project_id}", project_id, message="Pending...")
    lines_job_id = create_job(f"auto_lines_{project_id}", project_id, message="Pending...")
    tables_job_id = create_job(f"auto_tables_{project_id}", project_id, message="Pending...")
    rows_job_id = create_job(f"auto_rows_{project_id}", project_id, message="Pending...")

    # Start background task with all job IDs
    run_job_in_background(
        job_id,
        auto_train_background_task(job_id, project_id, files_job_id, lines_job_id, tables_job_id, rows_job_id)
    )

    return JSONResponse({
        "job_id": job_id,
        "status": "pending",
        "message": "Job started",
        "existing": False,
        "sub_jobs": {
            "files": files_job_id,
            "lines": lines_job_id,
            "tables": tables_job_id,
            "rows": rows_job_id
        }
    })


@router.post("/project/{project_id}/auto-train/clear")
def clear_training_data(project_id: int):
    """
    Clear all training data (status fields) for a project.
    Resets files, file_rows, db_tables, and db_table_rows status to NULL.
    """
    conn = get_conn()
    cursor = conn.cursor()

    # Verify project exists
    cursor.execute("SELECT id FROM projects WHERE id = %s", (project_id,))
    project = cursor.fetchone()
    if not project:
        cursor.close()
        conn.close()
        return JSONResponse({"error": "Project not found"}, status_code=404)

    # Clear file status for dirty files
    cursor.execute("""
        UPDATE files SET status = NULL
        WHERE project_id = %s AND is_dirty = 1
    """, (project_id,))
    files_cleared = cursor.rowcount

    # Clear file_rows status for dirty files
    cursor.execute("""
        UPDATE file_rows fr
        JOIN files f ON fr.file_id = f.id
        SET fr.status = NULL
        WHERE f.project_id = %s AND f.is_dirty = 1
    """, (project_id,))
    lines_cleared = cursor.rowcount

    # Clear db_tables status for dirty tables
    cursor.execute("""
        UPDATE db_tables SET status = NULL
        WHERE project_id = %s AND is_dirty = 1
    """, (project_id,))
    tables_cleared = cursor.rowcount

    # Clear db_table_rows status for dirty tables
    cursor.execute("""
        UPDATE db_table_rows dr
        JOIN db_tables t ON dr.table_id = t.id
        SET dr.status = NULL
        WHERE t.project_id = %s AND t.is_dirty = 1
    """, (project_id,))
    rows_cleared = cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    return JSONResponse({
        "success": True,
        "cleared": {
            "files": files_cleared,
            "lines": lines_cleared,
            "tables": tables_cleared,
            "rows": rows_cleared
        }
    })


@router.websocket("/project/{project_id}/auto-train/ws")
async def auto_train_ws(websocket: WebSocket, project_id: int):
    await websocket.accept()

    try:
        # Mock data - 5 updates, 1 second apart
        mock_updates = [
            {
                "files": {"processed": 100, "matched": 10},
                "lines": {"processed": 0, "matched": 0},
                "tables": {"processed": 0, "matched": 0},
                "rows": {"processed": 0, "matched": 0},
            },
            {
                "files": {"processed": 250, "matched": 25},
                "lines": {"processed": 1000, "matched": 50},
                "tables": {"processed": 0, "matched": 0},
                "rows": {"processed": 0, "matched": 0},
            },
            {
                "files": {"processed": 500, "matched": 45},
                "lines": {"processed": 5000, "matched": 200},
                "tables": {"processed": 5, "matched": 2},
                "rows": {"processed": 0, "matched": 0},
            },
            {
                "files": {"processed": 750, "matched": 60},
                "lines": {"processed": 10000, "matched": 400},
                "tables": {"processed": 10, "matched": 4},
                "rows": {"processed": 500, "matched": 20},
            },
            {
                "files": {"processed": 1000, "matched": 80},
                "lines": {"processed": 15000, "matched": 600},
                "tables": {"processed": 12, "matched": 5},
                "rows": {"processed": 1200, "matched": 50},
            },
        ]

        await websocket.send_json({"type": "started"})

        for i, update in enumerate(mock_updates):
            await asyncio.sleep(1)
            await websocket.send_json({
                "type": "progress",
                "data": update,
                "step": i + 1,
                "total_steps": len(mock_updates)
            })

        await websocket.send_json({"type": "done"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except:
            pass
