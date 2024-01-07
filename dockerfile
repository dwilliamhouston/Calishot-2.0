# Creating a Dockerfile for Python 3
# Use an existing base image from Docker Hub
FROM ubuntu:latest
# Set the working directory inside the container and create /mnt/database as VOLUME
WORKDIR /app
VOLUME /mnt/database
# Copy the application files from the host to the container
COPY . .
COPY *.db /mnt/database
# Install any required dependencies
RUN apt-get update && apt-get install -y python3 python3-pip
# Install Datasette using pip
RUN pip3 install datasette
RUN pip3 install datasette-json-html
RUN pip3 install datasette-pretty-json
RUN pip3 install -r requirements.txt
# Expose a port on the container
EXPOSE 5000
