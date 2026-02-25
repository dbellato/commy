# # syntax=docker/dockerfile:1

# FROM python:3.13.8-slim-bookworm

# WORKDIR /code

# COPY . .

# # Upgrade pip to fix security vulnerability GHSA-4xh5-x5gv-qwph
# RUN pip install --no-cache-dir --upgrade pip>=25.3

# RUN pip install --no-cache-dir --upgrade -r requirements.txt

# # Install Node.js and pnpm with specific versions
# RUN apt-get update \
#     && apt-get install -y curl \
#     && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
#     && apt-get install -y nodejs \
#     && npm install -g pnpm@10.6.0 \
#     && node --version \
#     && pnpm --version

# # Build React frontend
# WORKDIR /code/frontend
# RUN pnpm install --frozen-lockfile=false \
#     && pnpm build \
#     && rm -rf node_modules \
#     && rm -rf /root/.local/share/pnpm \
#     && rm -rf /root/.pnpm-store
# # RUN rm -f package-lock.json pnpm-lock.yaml yarn. lock \
# #     && rm -rf node_modules \
# #     && pnpm install -- no-frozen-lockfile \
# #     && pnpm build


# RUN apt-get purge -y krb5-user libkrb5-3 libkrb5support0 libgssapi-krb5-2

# # Return to backend directory
# WORKDIR /code

# EXPOSE 50505

# CMD ["gunicorn", "api.main:create_app"]

# syntax=docker/dockerfile:1.5
FROM python:3.13.8-slim-bookworm

WORKDIR /code

# Force unixODBC to use /etc/odbcinst.ini at runtime
ENV ODBCSYSINI=/etc
ENV ODBCINSTINI=odbcinst.ini

# --- ODBC Driver 18 for SQL Server (Debian 12 / bookworm) + build deps for pyodbc ---
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        curl ca-certificates gnupg apt-transport-https \
        unixodbc unixodbc-dev; \
    mkdir -p /etc/apt/keyrings; \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg; \
    ARCH="$(dpkg --print-architecture)"; \
    echo "deb [arch=${ARCH} signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/microsoft-prod.list; \
    apt-get update; \
    # install must succeed, otherwise build must fail
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18; \
    # HARD PROOF: package installed
    dpkg -l | grep -E '^ii\s+msodbcsql18\s'; \
    # find actual driver .so (must exist)
    DRIVER_PATH="$(dpkg -L msodbcsql18 | grep -E 'libmsodbcsql-18.*\.so' | head -n 1)"; \
    test -n "${DRIVER_PATH}"; \
    echo "Using driver: ${DRIVER_PATH}"; \
    # write driver registration
    printf "[ODBC Driver 18 for SQL Server]\nDriver=%s\nUsageCount=1\n" "${DRIVER_PATH}" > /etc/odbcinst.ini; \
    chmod 644 /etc/odbcinst.ini; \
    # verify registration
    odbcinst -j; \
    odbcinst -q -d; \
    rm -rf /var/lib/apt/lists/*
# --- END BLOCK ---

# 1) Copy and install Python deps first (cache-friendly)
COPY requirements.txt ./
# Upgrade pip to fix security vulnerability GHSA-4xh5-x5gv-qwph
RUN pip install --no-cache-dir --upgrade "pip>=25.3"
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# 2) Install Node.js + pnpm (unchanged, but moved earlier is fine)
RUN apt-get update \
    && apt-get install -y curl \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g pnpm@10.6.0 \
    && node --version \
    && pnpm --version \
    && rm -rf /var/lib/apt/lists/*

# 3) Copy ONLY frontend lockfiles/manifests first so pnpm install can be cached
WORKDIR /code/frontend
COPY frontend/package.json frontend/pnpm-lock.yaml ./

# Use BuildKit cache for pnpm store (big win). Keep frozen lockfile for determinism/speed.
RUN --mount=type=cache,id=pnpm-store,target=/root/.pnpm-store \
    pnpm install --frozen-lockfile

# 4) Now copy the rest of the repo (this is what changes often)
WORKDIR /code
COPY . .

# 5) Build frontend (often fast once deps cached)
WORKDIR /code/frontend
RUN pnpm build \
    && rm -rf node_modules

# (Optional) you can keep these deletions if you really want, but they reduce cache benefit.
# With the cache mount above, you should NOT delete /root/.pnpm-store.
# RUN rm -rf /root/.local/share/pnpm /root/.pnpm-store

#RUN apt-get purge -y krb5-user libkrb5-3 libkrb5support0 libgssapi-krb5-2 || true
RUN apt-get purge -y --auto-remove unixodbc-dev gnupg apt-transport-https || true \
 && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
  odbcinst -q -d; \
  python -c "import pyodbc; print('pyodbc drivers:', pyodbc.drivers())"; \
  DRIVER="$(dpkg -L msodbcsql18 | grep -E 'libmsodbcsql-18.*\.so' | head -n 1)"; \
  ldd "$DRIVER" | tee /tmp/ldd.txt; \
  ! grep -q "not found" /tmp/ldd.txt

WORKDIR /code
ENV PORT=50505
EXPOSE $PORT
CMD ["gunicorn", "api.main:create_app"]