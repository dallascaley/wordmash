from fastapi import FastAPI, Request, WebSocket
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from app.routers import projects, inventory
import os

app = FastAPI()


@app.websocket("/ws/ping")
async def websocket_ping(websocket: WebSocket):
    await websocket.accept()
    await websocket.close()


templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routers
app.include_router(projects.router)
app.include_router(inventory.router)

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
    return templates.TemplateResponse(
        "statistics.html",
        {"request": request}
    )


@app.get("/admin")
def admin(request: Request):
    return templates.TemplateResponse(
        "admin.html",
        {"request": request}
    )
