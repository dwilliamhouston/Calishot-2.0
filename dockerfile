# Creating a Dockerfile for Python 3
# Use an existing base image from Docker Hub
FROM alpine:latest
# Set the working directory inside the container and create /mnt/database as VOLUME
WORKDIR /app
# Copy the application files from the host to the container
COPY . .
# Install any required dependencies
RUN apk update && apk add --no-cache python3 pipx bash py3-pip

# Install Datasette using pip
#
RUN pipx ensurepath

RUN export PATH=$PATH:/root/.local/bin && source /root/.bashrc && \
    pipx install --include-deps --force datasette && \
    pipx install --include-deps --force datasette-json-html && \
    pipx install --include-deps --force datasette-pretty-json

# Expose a port on the container
EXPOSE 5000
# Expose a local host directory to the container
CMD ["/bin/bash", "-c", "/root/.local/bin/datasette -p 5000 -h 0.0.0.0 /app/data/index.db --config sql_time_limit_ms:50000 --config allow_download:off --config max_returned_rows:2000 --config num_sql_threads:10 --config allow_csv_stream:off --metadata metadata.json"]

