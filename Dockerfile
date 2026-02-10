FROM python:3.12-slim

# PostgreSQL client versions to install (space-separated).
# Single version: "17"  |  Multiple: "14 16 17"
ARG PG_VERSIONS="17"

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl gnupg lsb-release && \
    echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      | gpg --dearmor -o /etc/apt/trusted.gpg.d/pgdg.gpg && \
    apt-get update && \
    for v in $PG_VERSIONS; do \
      apt-get install -y --no-install-recommends postgresql-client-$v; \
    done && \
    apt-get install -y --no-install-recommends openssh-client && \
    apt-get purge -y curl gnupg lsb-release && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app/
WORKDIR /app

RUN useradd -r -s /bin/false dbbackup && \
    chown -R dbbackup:dbbackup /app

USER dbbackup

ENTRYPOINT ["python", "dbbackup.py"]
