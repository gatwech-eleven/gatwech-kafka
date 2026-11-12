#!/bin/bash

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TS_FILE="${SCRIPT_DIR}/../src/topics.ts"
OUTPUT_FILE="${SCRIPT_DIR}/../kafka-topics-config.json"

echo "Generating topic configuration from $TS_FILE..."

# Use this alternative approach that doesn't require direct .ts imports
if ! npx ts-node -e "
  import('${TS_FILE}?.js').then(m => {
    if (m && m.exportTopicsToJSON) {
      console.log(m.exportTopicsToJSON());
    } else {
      console.error('Failed to load module or exportTopicsToJSON function');
      process.exit(1);
    }
  }).catch(err => {
    console.error('Import error:', err);
    process.exit(1);
  });
" > "$OUTPUT_FILE"; then
  echo "Error: Failed to generate topic configuration"
  exit 1
fi

echo "Topic configuration saved to $OUTPUT_FILE"