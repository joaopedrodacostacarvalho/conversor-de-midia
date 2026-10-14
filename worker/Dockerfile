# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

# ---- Variáveis de ambiente Python ----------------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=100

# ---- Dependências de sistema (FFmpeg + ImageMagick) ----------------------
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ffmpeg \
        imagemagick \
        fonts-dejavu-core \
        tini \
 && rm -rf /var/lib/apt/lists/* \
 # Permite que o ImageMagick processe formatos comuns sem reclamar de política
 # (o padrão do Debian é restritivo e bloqueia PDF/PS/... — aqui só afrouxamos
 #  o que precisamos, mantendo o resto bloqueado).
 && sed -i 's/rights="none" pattern="PDF"/rights="read|write" pattern="PDF"/' \
        /etc/ImageMagick-6/policy.xml || true

# ---- Usuário não-root ----------------------------------------------------
RUN groupadd --system --gid 1001 app \
 && useradd  --system --uid 1001 --gid app --create-home app

WORKDIR /app

# ---- Camada de dependências ----------------------------------------------
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# ---- Código ---------------------------------------------------------------
COPY --chown=app:app app ./app

USER app

# ---- Tini como PID 1 ------------------------------------------------------
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-u", "-m", "app.main"]
