from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from app.db import get_conn

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/inventory")
def inventory(request: Request, project_id: int = None):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT id, name FROM projects ORDER BY name")
    projects = cursor.fetchall()

    stats = None
    project = None

    if project_id:
        cursor.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        project = cursor.fetchone()

        if project:
            cursor.execute("""
                SELECT
                    COUNT(*) as total_files,
                    SUM(CASE WHEN processed = 1 THEN 1 ELSE 0 END) as processed_files
                FROM files WHERE project_id = %s
            """, (project_id,))
            file_stats = cursor.fetchone()

            cursor.execute("""
                SELECT
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN fr.processed = 1 THEN 1 ELSE 0 END) as processed_rows
                FROM file_rows fr
                JOIN files f ON fr.file_id = f.id
                WHERE f.project_id = %s
            """, (project_id,))
            row_stats = cursor.fetchone()

            cursor.execute("""
                SELECT
                    COUNT(*) as total_tables,
                    SUM(CASE WHEN processed = 1 THEN 1 ELSE 0 END) as processed_tables
                FROM db_tables WHERE project_id = %s
            """, (project_id,))
            table_stats = cursor.fetchone()

            cursor.execute("""
                SELECT
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN dtr.processed = 1 THEN 1 ELSE 0 END) as processed_rows
                FROM db_table_rows dtr
                JOIN db_tables dt ON dtr.table_id = dt.id
                WHERE dt.project_id = %s
            """, (project_id,))
            db_row_stats = cursor.fetchone()

            stats = {
                "files": file_stats,
                "file_rows": row_stats,
                "db_tables": table_stats,
                "db_table_rows": db_row_stats
            }

    cursor.close()
    conn.close()

    return templates.TemplateResponse(
        "inventory.html",
        {"request": request, "projects": projects, "project": project, "stats": stats}
    )
