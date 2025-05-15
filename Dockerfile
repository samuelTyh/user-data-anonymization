FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
git \
curl \
&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt setup.py ./

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -e .

COPY pipeline ./pipeline
COPY tests ./tests

RUN mkdir -p ./data

ENTRYPOINT ["python", "-m", "pipeline.main"]
