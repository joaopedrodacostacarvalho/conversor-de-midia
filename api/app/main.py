import io
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .broker import publish_job
from .config import settings
from .db import Base, SessionLocal, engine
from .models import Job
from .storage import upload_stream

app = FastAPI(title="Media Converter API")

Base.metadata.create_all(bind=engine)

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

VIDEO = {"mp4", "avi", "mkv", "mov", "webm"}
AUDIO = {"mp3", "wav", "ogg", "flac", "aac", "m4a"}
IMAGE = {"png", "jpg", "jpeg", "webp", "gif", "bmp", "tiff"}

ALLOWED_BY_TYPE = {"video": VIDEO, "audio": AUDIO, "image": IMAGE}
EXT_TO_TYPE = {}
for f in VIDEO:
    EXT_TO_TYPE[f] = "video"
for f in AUDIO:
    EXT_TO_TYPE[f] = "audio"
for f in IMAGE:
    EXT_TO_TYPE[f] = "image"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.post("/jobs", status_code=202)
def create_job(
    file: UploadFile = File(...),
    targetFormat: str = Form(...),
    email: str = Form(...),
):
    target = targetFormat.lower().lstrip(".")
    ext = (file.filename.rsplit(".", 1)[-1] or "").lower()

    source_type = EXT_TO_TYPE.get(ext)
    if not source_type:
        raise HTTPException(400, f"Extensão '{ext}' não suportada")
    if target not in ALLOWED_BY_TYPE[source_type]:
        raise HTTPException(
            400, f"Formato destino '{target}' inválido para {source_type}"
        )

    job_id = str(uuid.uuid4())
    source_key = f"{job_id}/source.{ext}"

    # Obtém tamanho sem carregar tudo em memória
    file.file.seek(0, 2)
    size = file.file.tell()
    file.file.seek(0)

    upload_stream(
        settings.UPLOADS_BUCKET,
        source_key,
        file.file,
        size,
        file.content_type or "application/octet-stream",
    )

    db = SessionLocal()
    try:
        db.add(
            Job(
                id=job_id,
                status="PENDENTE",
                source_key=source_key,
                source_type=source_type,
                source_format=ext,
                target_format=target,
                notify_email=email,
            )
        )
        db.commit()
    finally:
        db.close()

    publish_job(
        {
            "jobId": job_id,
            "sourceUrl": f"s3://{settings.UPLOADS_BUCKET}/{source_key}",
            "sourceType": source_type,
            "sourceFormat": ext,
            "targetFormat": target,
            "notifyEmail": email,
            "requestedAt": datetime.utcnow().isoformat() + "Z",
            "attempt": 1,
        }
    )

    return {"jobId": job_id}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    db = SessionLocal()
    try:
        job = db.get(Job, job_id)
        if not job:
            raise HTTPException(404, "Job não encontrado")
        return {
            "jobId": job.id,
            "status": job.status,
            "convertedUrl": job.converted_url,
            "errorMessage": job.error_message,
            "createdAt": job.created_at.isoformat() if job.created_at else None,
            "updatedAt": job.updated_at.isoformat() if job.updated_at else None,
        }
    finally:
        db.close()
