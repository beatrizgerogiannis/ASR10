# Imagem única usada pelas três "máquinas" do docker-compose.yml.
FROM python:3.12-slim

# procps traz o pkill, usado para encerrar workers/subscribers no cenário de teste
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONUNBUFFERED=1

# O container só fica de pé; os programas são executados com "docker compose exec"
CMD ["sleep", "infinity"]
