#!/bin/bash

# Configuration
BOOTSTRAP_SERVER="localhost:9092"
REPLICATION_FACTOR=1             
PARTITIONS=10
INITIAL_SLEEP=30                
MAX_READINESS_ATTEMPTS=15       
READINESS_RETRY_DELAY=5         
MAX_CREATION_ATTEMPTS=3         
CREATION_RETRY_DELAY=5          
TOPIC_CONFIG_FILE="$(dirname "$0")/../kafka-topics-config.json"

# Initialize empty topic configs
declare -A TOPIC_CONFIGS

# Logging function with log levels
log() {
  local level=$1
  local message=$2
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  case $level in
    "INFO") echo "[$timestamp] ℹ️  $message" ;;
    "WARN") echo "[$timestamp] ⚠️  $message" ;;
    "ERROR") echo "[$timestamp] ❌ $message" ;;
    "SUCCESS") echo "[$timestamp] ✅ $message" ;;
    *) echo "[$timestamp] $message" ;;
  esac
}

# Load topic configurations from JSON file
load_topic_configs() {
  if [ ! -f "$TOPIC_CONFIG_FILE" ]; then
    log "ERROR" "Topic configuration file $TOPIC_CONFIG_FILE not found"
    exit 1
  fi

  # Parse JSON into bash associative array
  while IFS="=" read -r key value; do
    # Remove quotes and trim
    key=${key//\"/}
    key=${key// /}
    value=${value//\"/}
    value=${value%,}
    value=${value# }
    TOPIC_CONFIGS["$key"]="$value"
  done < <(jq -r 'to_entries[] | "\"\(.key)\" = \"\(.value)\""' "$TOPIC_CONFIG_FILE")
}

# Determine how to invoke Kafka CLI tools.
# If a docker container named "kafka" is running, prefer using docker exec to call the bundled CLI.
determine_kafka_cli() {
  if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
    KAFKA_TOPICS_CMD=(docker exec kafka /opt/kafka/bin/kafka-topics.sh)
    KAFKA_METADATA_CMD=(docker exec kafka /opt/kafka/bin/kafka-metadata-quorum.sh)
    return 0
  fi

  # Fallback to host-installed Kafka CLI if available in PATH
  if command -v kafka-topics.sh >/dev/null 2>&1 && command -v kafka-metadata-quorum.sh >/dev/null 2>&1; then
    KAFKA_TOPICS_CMD=(kafka-topics.sh)
    KAFKA_METADATA_CMD=(kafka-metadata-quorum.sh)
    return 0
  fi

  log "ERROR" "kafka-topics.sh and kafka-metadata-quorum.sh not found on PATH and no docker container named 'kafka' detected. Start Kafka or install the Kafka CLI."
  exit 1
}

# Health check for Kafka
check_kafka_ready() {
  log "INFO" "Checking Kafka broker readiness..."
  local attempt=0
  determine_kafka_cli

  until [[ $attempt -ge $MAX_READINESS_ATTEMPTS ]]; do
    if "${KAFKA_TOPICS_CMD[@]}" --list --bootstrap-server "$BOOTSTRAP_SERVER" >/dev/null 2>&1; then
      if "${KAFKA_METADATA_CMD[@]}" --bootstrap-server "$BOOTSTRAP_SERVER" describe --status | grep -q "LeaderId:"; then
        log "SUCCESS" "Kafka broker and controller are ready!"
        return 0
      else
        log "WARN" "Broker connected but controller not ready yet"
      fi
    fi

    attempt=$((attempt + 1))
    log "INFO" "Attempt $attempt/$MAX_READINESS_ATTEMPTS: Broker not ready. Retrying in ${READINESS_RETRY_DELAY}s..."
    sleep "$READINESS_RETRY_DELAY"
  done
  
  log "ERROR" "Kafka broker did not become ready after $MAX_READINESS_ATTEMPTS attempts"
  exit 1
}

# Topic creation function
create_topic() {
  local topic=$1
  local configs=${TOPIC_CONFIGS[$topic]}
  local attempt=0
  
  log "INFO" "Attempting to create topic: $topic"

  # If the topic already exists, consider it a success and skip creation.
  if determine_kafka_cli >/dev/null 2>&1 && "${KAFKA_TOPICS_CMD[@]}" --describe --topic "$topic" --bootstrap-server "$BOOTSTRAP_SERVER" >/dev/null 2>&1; then
    log "SUCCESS" "Topic already exists: $topic"
    return 0
  fi
  
  until [[ $attempt -ge $MAX_CREATION_ATTEMPTS ]]; do
    # Build the create command using the resolved kafka topics CLI
    local create_cmd=("${KAFKA_TOPICS_CMD[@]}" --create --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$REPLICATION_FACTOR" --if-not-exists --bootstrap-server "$BOOTSTRAP_SERVER")
    
    if [[ -n "$configs" ]]; then
      IFS=',' read -ra CONFIG_ARRAY <<< "$configs"
      for config in "${CONFIG_ARRAY[@]}"; do
        create_cmd+=(--config "$config")
      done
    fi
    
    if "${create_cmd[@]}" >/dev/null 2>&1; then
      log "SUCCESS" "Created topic: $topic"
      return 0
    fi
    
    attempt=$((attempt + 1))
    log "WARN" "Attempt $attempt failed for topic: $topic. Retrying in ${CREATION_RETRY_DELAY}s..."
    sleep "$CREATION_RETRY_DELAY"
  done
  
  log "ERROR" "Failed to create topic: $topic after $MAX_CREATION_ATTEMPTS attempts"
  exit 1
}

# Verify topic creation
verify_topic() {
  local topic=$1
  if "${KAFKA_TOPICS_CMD[@]}" --describe --topic "$topic" --bootstrap-server "$BOOTSTRAP_SERVER" >/dev/null 2>&1; then
    log "SUCCESS" "Verified topic exists: $topic"
    return 0
  else
    log "ERROR" "Failed to verify topic: $topic"
    return 1
  fi
}

# Main execution
main() {
  log "INFO" "🚀 Starting Kafka topic initialization"
  
  load_topic_configs
  
  log "INFO" "Waiting ${INITIAL_SLEEP}s for Kafka to start..."
  sleep "$INITIAL_SLEEP"
  
  check_kafka_ready
  
  log "INFO" "Initializing topics..."
  for topic in "${!TOPIC_CONFIGS[@]}"; do
    create_topic "$topic"
    verify_topic "$topic"
  done
  
  log "INFO" "Listing all topics:"
  # Use resolved kafka topics CLI (docker exec or host CLI)
  if determine_kafka_cli >/dev/null 2>&1; then
    "${KAFKA_TOPICS_CMD[@]}" --list --bootstrap-server "$BOOTSTRAP_SERVER"
  else
    kafka-topics.sh --list --bootstrap-server "$BOOTSTRAP_SERVER" || true
  fi
  
  log "SUCCESS" "🎉 All topics initialized successfully!"
}

# Run main
main "$@"