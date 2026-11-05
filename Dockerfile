FROM python:3.11-slim

# Evita geração de arquivos .pyc e força saída de logs em tempo real (sem buffer)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC \
    POLARS_MAX_THREADS=4

WORKDIR /code

# Instala dependências mínimas de sistema (curl para healthcheck no Coolify e certificados)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Instala dependências Python aproveitando cache de camadas do Docker
COPY requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /code/requirements.txt

# Copia o código-fonte do microsserviço
COPY ./app /code/app

# Expõe a porta padrão para o proxy reverso do Coolify (Traefik/Caddy)
EXPOSE 8000

# Healthcheck nativo para monitoramento 24/7 no Coolify
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:8000/api/status || exit 1

# Inicializa o servidor ASGI Uvicorn em loop assíncrono único (necessário para o estado em memória do APScheduler)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
