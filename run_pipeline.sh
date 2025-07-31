#!/bin/bash

# Run Docker Compose if not already running

if ! docker compose ps --services --filter "status=running" | grep -q .; then
  echo "Starting Docker Compose services..."
else
  echo "Docker Compose services already running."
  docker compose down -v
fi
docker compose up -d --build

# Display the current status of Docker Compose services
echo "Current status of Docker Compose services:"
docker compose ps

# Wait for services to stabilize
echo "Waiting for services to stabilize..."
#sleep 10

# Setup Python virtual environment and activate it
python3 -m venv venv
source venv/bin/activate

# Install required Python packages
echo "Installing required Python packages..."
python3 -m pip install --upgrade pip
pip install pyspark faker

# Start Spark Consumer
echo "Starting Spark Consumer..."
python3 ./spark/stream_consumer.py