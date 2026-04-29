# Lightweight container for the Express ingestion API and local helper scripts.
FROM node:20-bookworm-slim

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first so Docker layer caching works when only app code
# changes.
COPY package*.json ./
RUN npm ci

COPY pyproject.toml README.md ./
COPY dashboard ./dashboard
COPY historical ./historical
COPY ml_model ./ml_model
COPY poker_platform ./poker_platform
COPY simulation ./simulation
COPY sql ./sql
COPY sample_data ./sample_data
RUN python3 -m pip install --break-system-packages --no-cache-dir -e .[kafka,snowflake,openai]

ENV PYTHON_BIN=python3
ENV PYTHONPATH=/usr/src/app:/usr/src/app/historical:/usr/src/app/ml_model:/usr/src/app/simulation

EXPOSE 3000

CMD ["npm", "start"]
