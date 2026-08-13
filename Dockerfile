FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
COPY tests ./tests

RUN pip install --no-cache-dir .

# Entrypoint por definir cuando existan producer.py / pipeline.py / materializer.py
CMD ["python", "--version"]
