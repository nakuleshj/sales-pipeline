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

# Open Metabase in the default web browser
echo "open Metabase at http://localhost:3000"