# Creating a Dockerfile for Python 3
# Use an existing base image from Docker Hub
FROM ubuntu:latest
# Set the working directory inside the container
WORKDIR /app
# Copy the application files from the host to the container
COPY . .
# Install any required dependencies
RUN apt-get update && apt-get install -y python3 python3-pip
# Install Datasette using pip
RUN pip3 install datasette
RUN pip3 install datasette-json-html
RUN pip3 install datasette-pretty-json
RUN pip3 install -r requirements.txt
# Expose a port on the container
EXPOSE 5000
# Specify the command to run when the container starts
#RUN datasette serve index-non-eng.db --config sql_time_limit_ms:50000 --config allow_download:off --config max_returned_rows:2000  --config num_sql_threads:10 --config allow_csv_stream:off  --metadata metadata.json


