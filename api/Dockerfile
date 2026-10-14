# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

# ---- Variáveis de ambiente Python ----------------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=100

# ---- Dependências de sistema ---------------------------------------------
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        curl \
        tini \
 && rm -rf /var/lib/apt/lists/*

# ---- Usuário não-root ----------------------------------------------------
RUN groupadd --system --gid 1001 app \
 && useradd  --system --uid 1001 --gid app --create-home app

WORKDIR /app

# ---- Camada de dependências (cacheadas separadamente) --------------------
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# ---- Código da aplicação --------------------------------------------------
COPY --chown=app:app app    ./app
COPY --chown=app:app static ./static

USER app

EXPOSE 8000

# ---- Healthcheck ---------------------------------------------------------
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
  CMD curl -fsS http://localhost:8000/health || exit 1

# ---- Entrypoint (tini como PID 1) ---------------------------------------
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
