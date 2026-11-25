FROM apache/spark:4.1.4-scala2.13-java17-python3-ubuntu

ARG ICEBERG_VERSION=1.11.0
ARG PYTHON_VERSION=3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv from official pinned release image
COPY --from=ghcr.io/astral-sh/uv:0.11.15 /uv /uvx /usr/local/bin/

# Install Python 3.12 in a world-readable location so non-root spark user (185) can execute it
ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

RUN uv python install "${PYTHON_VERSION}" \
    && uv venv --python "${PYTHON_VERSION}" /opt/venv \
    && chmod -R a+rX /opt/uv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}" \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python

# Reuse base image's /opt/spark/python and py4j archive inside /opt/venv without reinstalling pyspark
RUN site_pkg="$(/opt/venv/bin/python -c 'import site; print(site.getsitepackages()[0])')" \
    && { \
         echo "/opt/spark/python"; \
         for zip_path in /opt/spark/python/lib/py4j-*.zip; do \
           echo "${zip_path}"; \
         done; \
       } > "${site_pkg}/spark.pth"

# Pre-bake Iceberg Spark 4.1 runtime and shaded AWS bundle (without unshaded Hadoop S3A AWS SDK bundle)
RUN curl -fsSL -o "/opt/spark/jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
    && curl -fsSL -o "/opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && chmod 644 /opt/spark/jars/iceberg-*.jar

WORKDIR /app
COPY pyproject.toml requirements.txt README.md ./
COPY src ./src

# Install pinned Python runtime dependencies excluding pyspark (reused from /opt/spark/python), then ie212 editable
RUN grep -vE '^(pyspark|#|[[:space:]]*$)' requirements.txt > /tmp/requirements-docker.txt \
    && uv pip install --python /opt/venv/bin/python --no-cache -r /tmp/requirements-docker.txt \
    && uv pip install --python /opt/venv/bin/python --no-cache --no-deps -e . \
    && rm -f /tmp/requirements-docker.txt \
    && chmod -R a+rX /opt/venv /app

# Create writable HOME for UID 185 (base image defaults UID 185 to /nonexistent)
RUN mkdir -p /home/spark && chown -R 185:185 /home/spark

ENV PYTHONPATH=/app/src:/opt/spark/python \
    HOME=/home/spark

USER 185

# Pre-install and verify DuckDB httpfs/iceberg extensions as UID 185 so /home/spark/.duckdb is owned by 185:185
RUN /opt/venv/bin/python -c "import duckdb; conn = duckdb.connect(':memory:'); conn.execute('INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;'); conn.close()"
