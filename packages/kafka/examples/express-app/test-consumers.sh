#!/bin/bash

# Test script for consumer example
# This script sends test messages to both user and order topics

BASE_URL="${BASE_URL:-http://localhost:3000}"

echo "🧪 Testing Kafka Consumer Example"
echo "=================================="
echo ""

# Test 1: User Event
echo "📤 Test 1: Publishing user event..."
curl -X POST "${BASE_URL}/events/user" \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-123",
    "event": "login"
  }' \
  -w "\n"

echo ""
sleep 2

# Test 2: Order Event - Completed
echo "📤 Test 2: Publishing completed order event..."
curl -X POST "${BASE_URL}/events/order" \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "order-456",
    "userId": "user-123",
    "amount": 99.99,
    "status": "completed"
  }' \
  -w "\n"

echo ""
sleep 2

# Test 3: Order Event - Cancelled
echo "📤 Test 3: Publishing cancelled order event..."
curl -X POST "${BASE_URL}/events/order" \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "order-789",
    "userId": "user-456",
    "amount": 49.99,
    "status": "cancelled"
  }' \
  -w "\n"

echo ""
sleep 2

# Test 4: DLQ Test - This will trigger an error and retry
echo "📤 Test 4: Publishing DLQ test event (will trigger retries)..."
curl -X POST "${BASE_URL}/events/user" \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-dlq",
    "event": "simulate-error"
  }' \
  -w "\n"

echo ""
echo "✅ All test messages sent!"
echo ""
echo "👀 Check the application logs to see:"
echo "  - User events being processed"
echo "  - Order events triggering business logic"
echo "  - DLQ retry behavior and final DLQ publish"
echo ""
echo "📊 View metrics at: ${BASE_URL}/metrics"
