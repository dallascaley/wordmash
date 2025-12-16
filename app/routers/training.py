from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from app.db import get_conn

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
