"""
Background job runner module.
Handles running long-running tasks asynchronously and updating the jobs table.
"""
import asyncio
import threading
from datetime import datetime
from typing import Callable, Any, Optional
from app.db import get_conn


def create_job(job_type: str, project_id: Optional[int] = None, message: str = "") -> int:
    """Create a new job record and return its ID."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO jobs (job_type, status, progress, total, message, project_id, created_at)
           VALUES (%s, 'pending', 0, NULL, %s, %s, %s)""",
        (job_type, message, project_id, datetime.now())
    )
    conn.commit()
    job_id = cursor.lastrowid
    cursor.close()
    conn.close()
    return job_id


def update_job(job_id: int, **kwargs):
    """Update job fields. Accepts: status, progress, total, message, error_details."""
    conn = get_conn()
    cursor = conn.cursor()

    allowed_fields = {'status', 'progress', 'total', 'message', 'error_details', 'started_at', 'ended_at'}
    updates = {k: v for k, v in kwargs.items() if k in allowed_fields}

    if not updates:
        cursor.close()
        conn.close()
        return

    set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
    values = list(updates.values()) + [job_id]

    cursor.execute(f"UPDATE jobs SET {set_clause} WHERE id = %s", values)
    conn.commit()
    cursor.close()
    conn.close()


def get_job(job_id: int) -> Optional[dict]:
    """Get job by ID."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
    job = cursor.fetchone()
    cursor.close()
    conn.close()
    return job


def get_running_job(job_type: str, project_id: int) -> Optional[dict]:
    """Get a running or pending job for a specific type and project."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT * FROM jobs
           WHERE job_type = %s AND project_id = %s AND status IN ('pending', 'running')
           ORDER BY created_at DESC LIMIT 1""",
        (job_type, project_id)
    )
    job = cursor.fetchone()
    cursor.close()
    conn.close()
    return job


def get_latest_completed_job(job_type: str, project_id: int) -> Optional[dict]:
    """Get the latest completed job for a specific type and project."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT * FROM jobs
           WHERE job_type = %s AND project_id = %s AND status = 'completed'
           ORDER BY ended_at DESC LIMIT 1""",
        (job_type, project_id)
    )
    job = cursor.fetchone()
    cursor.close()
    conn.close()
    return job


def start_job(job_id: int):
    """Mark job as started."""
    update_job(job_id, status='running', started_at=datetime.now())


def complete_job(job_id: int, total: Optional[int] = None, message: str = ""):
    """Mark job as completed."""
    updates = {'status': 'completed', 'ended_at': datetime.now()}
    if total is not None:
        updates['total'] = total
        updates['progress'] = total
    if message:
        updates['message'] = message
    update_job(job_id, **updates)


def fail_job(job_id: int, error: str):
    """Mark job as failed."""
    update_job(job_id, status='failed', error_details=error, ended_at=datetime.now())


def cancel_job(job_id: int):
    """Mark job as cancelled."""
    update_job(job_id, status='cancelled', ended_at=datetime.now())


# Track running background tasks
_running_tasks: dict[int, asyncio.Task] = {}


def run_job_in_background(job_id: int, coro):
    """
    Run a coroutine as a background task.
    The coroutine should handle its own job updates.
    """
    async def wrapper():
        try:
            await coro
        except asyncio.CancelledError:
            cancel_job(job_id)
        except Exception as e:
            fail_job(job_id, str(e))
        finally:
            _running_tasks.pop(job_id, None)

    task = asyncio.create_task(wrapper())
    _running_tasks[job_id] = task
    return task


def cancel_background_job(job_id: int) -> bool:
    """Cancel a running background job."""
    task = _running_tasks.get(job_id)
    if task and not task.done():
        task.cancel()
        return True
    return False


def is_job_running(job_id: int) -> bool:
    """Check if a job's background task is still running."""
    task = _running_tasks.get(job_id)
    return task is not None and not task.done()


def cleanup_stale_jobs() -> int:
    """
    Mark any running/pending jobs as cancelled on server startup.
    Returns the number of jobs cancelled.
    """
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE jobs
        SET status = 'cancelled',
            ended_at = %s,
            error_details = 'Server restarted - job was interrupted'
        WHERE status IN ('running', 'pending')
    """, (datetime.now(),))

    cancelled_count = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()

    return cancelled_count


def cleanup_old_stale_jobs(max_age_minutes: int = 30) -> int:
    """
    Mark jobs as failed if they've been 'running' for too long without updates.
    This catches jobs that got stuck without proper error handling.
    Returns the number of jobs marked as failed.
    """
    conn = get_conn()
    cursor = conn.cursor()

    # Find jobs that have been running for longer than max_age_minutes
    # and aren't being tracked in memory (meaning they're actually dead)
    cursor.execute("""
        SELECT id FROM jobs
        WHERE status = 'running'
        AND started_at < DATE_SUB(NOW(), INTERVAL %s MINUTE)
    """, (max_age_minutes,))

    stale_jobs = cursor.fetchall()
    stale_count = 0

    for job in stale_jobs:
        job_id = job['id']
        # Only mark as failed if we're not tracking it in memory
        if job_id not in _running_tasks:
            cursor.execute("""
                UPDATE jobs
                SET status = 'failed',
                    ended_at = %s,
                    error_details = 'Job timed out - no progress for over %s minutes'
                WHERE id = %s
            """, (datetime.now(), max_age_minutes, job_id))
            stale_count += 1

    conn.commit()
    cursor.close()
    conn.close()

    return stale_count
