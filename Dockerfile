FROM python:3.12-slim AS base
RUN apt-get update && apt-get install -y --no-install-recommends \
git \
curl \
&& rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt


FROM base AS test
RUN pip install pytest flake8
COPY tests ./tests
COPY pipeline ./pipeline
CMD ["pytest", "tests/"]


FROM base AS deploy
COPY pipeline ./pipeline
RUN mkdir -p ./data
ENTRYPOINT ["python", "-m", "pipeline.main"]