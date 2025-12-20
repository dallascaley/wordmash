from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from app.db import get_conn
from app.jobs import (
    create_job, update_job, get_job, get_running_job,
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
    if project_id:
        cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        project = cursor.fetchone()

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
    Background task that runs auto-training and updates the jobs table with progress.
    Updates both the main job and each sub-job (files, lines, tables, rows).
    This is a placeholder that will be replaced with actual training logic.
    """
    try:
        start_job(job_id)
        start_job(files_job_id)
        start_job(lines_job_id)
        start_job(tables_job_id)
        start_job(rows_job_id)

        # Mock data - 5 updates representing training steps
        # This will be replaced with actual training logic later
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

        # Map sub-job IDs to their data keys
        sub_jobs = {
            "files": files_job_id,
            "lines": lines_job_id,
            "tables": tables_job_id,
            "rows": rows_job_id,
        }

        total_steps = len(mock_updates)
        import json

        for i, data in enumerate(mock_updates):
            step = i + 1
            pct = round((step / total_steps) * 100)

            # Update main job with overall progress
            progress_data = {
                "step": step,
                "total_steps": total_steps,
                "data": data
            }
            update_job(
                job_id,
                progress=pct,
                total=100,
                message=json.dumps(progress_data)
            )

            # Update each sub-job with its individual progress
            for key, sub_job_id in sub_jobs.items():
                sub_data = data[key]
                update_job(
                    sub_job_id,
                    progress=sub_data["processed"],
                    total=sub_data["processed"],  # Will be replaced with actual totals
                    message=json.dumps(sub_data)
                )

            await asyncio.sleep(1)

        # Complete all jobs
        complete_job(job_id, total=100, message="Training completed successfully")
        for key, sub_job_id in sub_jobs.items():
            complete_job(sub_job_id, message=f"{key.capitalize()} scan completed")

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
