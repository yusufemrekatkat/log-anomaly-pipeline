FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-cache

# Copy the rest of the application
COPY . .

# Environment variable to ensure output is printed immediately
ENV PYTHONUNBUFFERED=1

# Default command (will be overridden in docker-compose)
CMD ["uv", "run", "ingestion/main.py"]
