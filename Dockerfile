# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

# Stage 1: Builder
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN pip install --no-cache-dir poetry==1.8.3

# Copy project files
COPY pyproject.toml poetry.lock ./
COPY src/ ./src/
COPY README.md LICENSE ./

# Build wheel
RUN poetry build --format wheel && \
    mkdir /wheels && \
    cp dist/*.whl /wheels/

# Stage 2: Runtime
FROM python:3.12-slim AS runtime

# Install System Dependencies
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    postgresql-client \
    ca-certificates \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser -d /home/appuser -m appuser

WORKDIR /home/appuser/app

# Install Application + curl-cffi
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl pytest==8.2.2 "dlt[postgres]==1.20.0" "curl-cffi==0.14.0"

# Set permissions
RUN chown -R appuser:appuser /home/appuser/app

USER appuser

CMD ["python", "-m", "coreason_etl_fda_orange_book.main"]
