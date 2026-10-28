FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    IOMT_REGISTRY_PATH=/app/device_registry.json

WORKDIR /app

COPY simulator-worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator/simulator.py ./simulator.py
COPY device_registry.json ./device_registry.json

CMD ["python", "-u", "simulator.py"]