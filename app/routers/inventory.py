from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from app.db import get_conn

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.post("/project/{project_id}/reset")
def reset_project_data(project_id: int):
    try:
        conn = get_conn()
        cursor = conn.cursor()

        # Delete file_rows for this project's files
        cursor.execute("""
            DELETE fr FROM file_rows fr
            JOIN files f ON fr.file_id = f.id
            WHERE f.project_id = %s
        """, (project_id,))

        # Delete files for this project
        cursor.execute("DELETE FROM files WHERE project_id = %s", (project_id,))

        conn.commit()

        # Reset auto-increment (set to 1, MySQL will use next available)
        cursor.execute("ALTER TABLE file_rows AUTO_INCREMENT = 1")
        cursor.execute("ALTER TABLE files AUTO_INCREMENT = 1")

        cursor.close()
        conn.close()

        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


def get_stats_for_type(cursor, project_id: int, is_dirty: int):
    """Get stats for either clean (is_dirty=0) or dirty (is_dirty=1) data."""
    cursor.execute("""
        SELECT
            COUNT(*) as total_files,
            SUM(CASE WHEN processed = 1 THEN 1 ELSE 0 END) as processed_files
        FROM files WHERE project_id = %s AND is_dirty = %s
    """, (project_id, is_dirty))
    file_stats = cursor.fetchone()

    cursor.execute("""
        SELECT
            COUNT(*) as total_rows,
            SUM(CASE WHEN fr.processed = 1 THEN 1 ELSE 0 END) as processed_rows
        FROM file_rows fr
        JOIN files f ON fr.file_id = f.id
        WHERE f.project_id = %s AND f.is_dirty = %s
    """, (project_id, is_dirty))
    row_stats = cursor.fetchone()

    cursor.execute("""
        SELECT
            COUNT(*) as total_tables,
            SUM(CASE WHEN processed = 1 THEN 1 ELSE 0 END) as processed_tables
        FROM db_tables WHERE project_id = %s AND is_dirty = %s
    """, (project_id, is_dirty))
    table_stats = cursor.fetchone()

    cursor.execute("""
        SELECT
            COUNT(*) as total_rows,
            SUM(CASE WHEN dtr.processed = 1 THEN 1 ELSE 0 END) as processed_rows
        FROM db_table_rows dtr
        JOIN db_tables dt ON dtr.table_id = dt.id
        WHERE dt.project_id = %s AND dt.is_dirty = %s
    """, (project_id, is_dirty))
    db_row_stats = cursor.fetchone()

    return {
        "files": file_stats,
        "file_rows": row_stats,
        "db_tables": table_stats,
        "db_table_rows": db_row_stats
    }


@router.get("/inventory")
def inventory(request: Request, project_id: int = None):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT id, name FROM projects ORDER BY name")
    projects = cursor.fetchall()

    clean_stats = None
    dirty_stats = None
    project = None

    if project_id:
        cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        project = cursor.fetchone()

        if project:
            clean_stats = get_stats_for_type(cursor, project_id, 0)
            dirty_stats = get_stats_for_type(cursor, project_id, 1)

    cursor.close()
    conn.close()

    return templates.TemplateResponse(
        "inventory.html",
        {"request": request, "projects": projects, "project": project, "clean_stats": clean_stats, "dirty_stats": dirty_stats}
    )
