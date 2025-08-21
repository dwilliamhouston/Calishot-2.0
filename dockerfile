# Use Python alpine as base image
FROM python:3.9-alpine

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    DATA_DIR=/data \
    FLASK_APP=calishot_web/app.py \
    FLASK_ENV=development

# Install minimal system dependencies for Alpine (crond is built-in via busybox)
# Runtime libs and certs
RUN apk add --no-cache ca-certificates libffi openssl && update-ca-certificates

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install build dependencies (for packages like gevent) then remove them after pip install
RUN apk add --no-cache --virtual .build-deps build-base linux-headers musl-dev python3-dev libffi-dev openssl-dev && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir flask shodan requests python-dotenv gunicorn && \
    apk del .build-deps

# Copy application files
COPY . .

# Create necessary directories
RUN mkdir -p ${DATA_DIR} /var/log/calishot /app/books /app/calishot_webtemplates && \
    touch /var/log/cron.log

# Install cron schedule (Alpine busybox crond reads /etc/crontabs/root)
# If you already have a 'crontab' file in repo, copy it to the expected Alpine location
COPY crontab /etc/crontabs/root
RUN chmod 0644 /etc/crontabs/root

# Expose the web server port
EXPOSE 5003

# Set the data directory as a volume
VOLUME ["/data", "/app/books", "/app/calishot_webtemplates"]

# Startup: run crond in background and gunicorn in foreground
RUN echo '#!/bin/sh\n\n# Start cron (busybox crond) in background\ncrond -b -l 8 -L /var/log/cron.log\n\n# Start gunicorn for Flask app\nexec gunicorn --bind 0.0.0.0:5003 "calishot_web.app:create_app()"\n' > /startup.sh && \
    chmod +x /startup.sh

CMD ["/startup.sh"]
