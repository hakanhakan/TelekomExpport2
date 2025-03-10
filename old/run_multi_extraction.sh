#!/bin/bash
# Script to run multi-session property extraction

# Default values
AREA="Bad Sooden-Allendorf, Stadt"
SESSIONS=4
HEADLESS=false
DB_PATH="property_extraction.db"
DEBUG=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --area)
      AREA="$2"
      shift 2
      ;;
    --sessions)
      SESSIONS="$2"
      shift 2
      ;;
    --headless)
      HEADLESS=true
      shift
      ;;
    --db-path)
      DB_PATH="$2"
      shift 2
      ;;
    --debug)
      DEBUG=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check if area is provided
if [ -z "$AREA" ]; then
  echo "Error: --area parameter is required"
  echo "Usage: $0 --area \"City Name\" [--sessions N] [--headless] [--db-path path] [--debug]"
  exit 1
fi

# Install required packages
echo "Installing required packages..."
pip install -r requirements.txt

# Build command
CMD="python3 multi_session_extractor_new.py --area \"$AREA\""

# Add optional parameters
if [ "$HEADLESS" = true ]; then
  CMD="$CMD --headless"
fi

if [ "$DEBUG" = true ]; then
  CMD="$CMD --debug"
fi

CMD="$CMD --sessions $SESSIONS --db-path \"$DB_PATH\""

# Run the extraction
echo "Running multi-session extraction with command:"
echo "$CMD"
echo "---------------------------------------------"
eval $CMD

# Check exit code
if [ $? -eq 0 ]; then
  echo "Extraction completed successfully!"
else
  echo "Extraction failed with error code $?"
fi
