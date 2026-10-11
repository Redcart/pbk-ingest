FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.18 /uv /usr/local/bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY main_extract.py ./

# Cloud Run Job entry point; configuration comes from environment variables
CMD ["uv", "run", "--no-sync", "python", "main_extract.py"]
