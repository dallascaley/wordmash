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
    scan_totals = {"files": 0, "lines": 0, "tables": 0, "rows": 0}

    if project_id:
        cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        project = cursor.fetchone()

        # Get totals from latest completed scan jobs
        scan_job_types = {
            "files": f"scan_files_{project_id}",
            "lines": f"scan_lines_{project_id}",
            "tables": f"scan_tables_{project_id}",
            "rows": f"scan_db_rows_{project_id}",
        }
        for key, job_type in scan_job_types.items():
            job = get_latest_completed_job(job_type, project_id)
            if job and job.get("total"):
                scan_totals[key] = job["total"]

    cursor.close()
    conn.close()

    # Valid data types
    data_types = [
        {"value": "files", "label": "Files"},
        {"value": "lines", "label": "Lines of Code"},
        {"value": "tables", "label": "Database Tables"},
        {"value": "rows", "label": "Database Rows"},
    ]

    return templates.TemplateResponse(
        "training.html",
        {
            "request": request,
            "projects": projects,
            "project": project,
            "data_types": data_types,
            "selected_data_type": data_type,
            "scan_totals": scan_totals,
        }
    )


async def auto_train_background_task(
    job_id: int,
    project_id: int,
    files_job_id: int,
    lines_job_id: int,
    tables_job_id: int,
    rows_job_id: int
):
    """
    Background task that runs auto-training using efficient SQL operations.

    For files: compares dirty files to clean files with matching name/path.
    - If no matching clean file exists -> file status = 'research'
    - If matching clean file exists -> compare rows line by line:
      - Matching rows -> row status = 'valid'
      - Non-matching rows -> row status = 'research'
      - If ALL rows match -> file status = 'valid'
      - Otherwise -> file status = 'mixed'
    """
    import json

    try:
        start_job(job_id)
        start_job(files_job_id)
        start_job(lines_job_id)
        start_job(tables_job_id)
        start_job(rows_job_id)

        conn = get_conn()
        cursor = conn.cursor()

        # Get counts for progress tracking
        cursor.execute(
            "SELECT COUNT(*) as cnt FROM files WHERE project_id = %s AND is_dirty = 1",
            (project_id,)
        )
        total_dirty_files = cursor.fetchone()["cnt"]

        cursor.execute(
            "SELECT COUNT(*) as cnt FROM file_rows fr "
            "JOIN files f ON fr.file_id = f.id "
            "WHERE f.project_id = %s AND fr.is_dirty = 1",
            (project_id,)
        )
        total_dirty_lines = cursor.fetchone()["cnt"]

        update_job(job_id, message=json.dumps({"data": {"status": "Phase 1: Finding files without clean matches..."}}))
        await asyncio.sleep(0)

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

        files_processed = research_files_count
        lines_processed = research_lines_count

        update_job(
            files_job_id,
            progress=files_processed,
            total=total_dirty_files,
            message=json.dumps({"processed": files_processed, "matched": 0})
        )
        update_job(job_id, progress=5, message=json.dumps({"data": {"status": f"Phase 1 complete: {research_files_count} files marked as research"}}))
        await asyncio.sleep(0)

        # PHASE 2: Get dirty files that have clean counterparts
        update_job(job_id, message=json.dumps({"data": {"status": "Phase 2: Comparing files with clean matches..."}}))

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

        files_matched = 0
        lines_matched = 0
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
                        lines_matched += 1
                    else:
                        research_ids.append(dirty_row["id"])
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

                lines_processed += len(dirty_rows)

                if all_match:
                    valid_file_ids.append(dirty_id)
                    files_matched += 1
                else:
                    mixed_file_ids.append(dirty_id)

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

            # Update progress
            pct = round((files_processed / total_dirty_files) * 100) if total_dirty_files > 0 else 100
            progress_data = {
                "files": {"processed": files_processed, "matched": files_matched},
                "lines": {"processed": lines_processed, "matched": lines_matched},
                "tables": {"processed": 0, "matched": 0},
                "rows": {"processed": 0, "matched": 0},
            }

            update_job(
                job_id,
                progress=pct,
                total=100,
                message=json.dumps({"data": progress_data})
            )
            update_job(
                files_job_id,
                progress=files_processed,
                total=total_dirty_files,
                message=json.dumps(progress_data["files"])
            )
            update_job(
                lines_job_id,
                progress=lines_processed,
                total=total_dirty_lines,
                message=json.dumps(progress_data["lines"])
            )

            await asyncio.sleep(0)

        cursor.close()
        conn.close()

        # Complete all jobs
        complete_job(job_id, total=100, message="Training completed successfully")
        complete_job(files_job_id, total=total_dirty_files, message=f"Files training completed: {files_matched}/{files_processed} matched")
        complete_job(lines_job_id, total=total_dirty_lines, message=f"Lines training completed: {lines_matched}/{lines_processed} matched")
        complete_job(tables_job_id, message="Tables training not yet implemented")
        complete_job(rows_job_id, message="DB rows training not yet implemented")

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
