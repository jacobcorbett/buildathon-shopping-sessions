FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/*
COPY pipeline/normalise.cpp /src/normalise.cpp
RUN g++ -std=c++17 -O3 -Wall -Wextra -Werror /src/normalise.cpp -o /normalise

FROM python:3.12-slim-bookworm
RUN pip install --no-cache-dir duckdb==1.5.6
COPY --from=build /normalise /usr/local/bin/normalise
COPY pipeline/ /app/
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
CMD ["python", "/app/pipeline.py"]
