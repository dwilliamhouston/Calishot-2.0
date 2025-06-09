# Calishot-2.0 Docker Setup

This guide explains how to set up and run Calishot-2.0 using Docker.

## Prerequisites

- Docker installed on your Linux host
- Docker Compose (usually included with Docker Desktop)
- At least 1GB of free disk space for the data directory

## Setup Instructions

1. **Clone the repository** (if you haven't already):
   ```bash
   git clone <repository-url>
   cd Calishot-2.0
   ```

2. **Build the Docker image**:
   ```bash
   sudo mkdir -p /opt/shodantest2
   sudo chown -R $USER:$USER /opt/shodantest2
   docker-compose build
   ```

3. **Run the container**:
   ```bash
   docker-compose up -d
   ```

## Accessing the Application

- The web interface will be available at: `http://<your-server-ip>:5003`
- The data directory is mounted at: `/opt/shodantest2` on the host
- Logs can be viewed with: `docker logs -f calishot`

## Scheduled Tasks

The Calishot script is configured to run every 12 hours via cron. You can check the logs with:

```bash
docker exec calishot tail -f /var/log/calishot/cron.log
```

## Stopping the Container

To stop the container:

```bash
docker-compose down
```

## Data Persistence

All data is stored in the host directory `/opt/shodantest2`. This includes:
- Database files
- Logs
- Configuration files

## Updating

To update to the latest version:

```bash
git pull
docker-compose build --no-cache
docker-compose down
docker-compose up -d
```

## Troubleshooting

- If the web interface is not accessible, check the logs:
  ```bash
  docker logs calishot
  ```

- If you need to modify the cron schedule, edit the `crontab` file and rebuild the container.

- For persistent issues, you can enter the container's shell:
  ```bash
  docker exec -it calishot /bin/bash
  ```
