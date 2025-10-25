#!/bin/bash

# Function to cleanup processes on exit
cleanup() {
    echo "Stopping servers..."
    kill $BACKEND_PID 2>/dev/null
    kill $FRONTEND_PID 2>/dev/null
    exit 0
}

# Set up trap to catch termination signals
trap cleanup SIGTERM SIGINT EXIT

# Start backend server in background
node server.js &
BACKEND_PID=$!
echo "Backend started with PID: $BACKEND_PID"

# Start frontend server in background
npx vite &
FRONTEND_PID=$!
echo "Frontend started with PID: $FRONTEND_PID"

# Wait for both processes
wait
