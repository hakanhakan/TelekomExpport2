#!/bin/bash
# Run the entire IBT property extraction process from start to finish

# Default values
AREA="Berlin"
SESSIONS=4
HEADLESS=false
DB_PATH="property_extraction.db"
OUTPUT_DIR="output"
ANALYSIS_DIR="analysis"
REFRESH_INTERVAL=5

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
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --analysis-dir)
      ANALYSIS_DIR="$2"
      shift 2
      ;;
    --refresh)
      REFRESH_INTERVAL="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --area AREA            Area to search for properties (default: Berlin)"
      echo "  --sessions N           Number of parallel browser sessions (default: 4)"
      echo "  --headless             Run in headless mode (no visible browser windows)"
      echo "  --db-path PATH         Path to the SQLite database file (default: property_extraction.db)"
      echo "  --output-dir DIR       Directory to save extraction results (default: output)"
      echo "  --analysis-dir DIR     Directory to save analysis results (default: analysis)"
      echo "  --refresh N            Refresh interval in seconds for monitoring (default: 5)"
      echo "  --help                 Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Create output directories
mkdir -p "$OUTPUT_DIR"
mkdir -p "$ANALYSIS_DIR"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed. Please install Python 3 and try again."
    exit 1
fi

# Check if required packages are installed
if ! python3 -c "import playwright, pandas, rich, pydantic, dotenv, matplotlib" &> /dev/null; then
    echo "Installing required packages..."
    pip install -r requirements.txt
    
    # Install Playwright browsers
    python3 -m playwright install
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Creating .env file..."
    echo "TELEKOM_USERNAME=your_username" > .env
    echo "TELEKOM_PASSWORD=your_password" >> .env
    echo "TELEKOM_OTP_SECRET=your_otp_secret  # Optional" >> .env
    
    echo "Please edit the .env file with your credentials and run this script again."
    exit 1
fi

# Function to run a command and check for errors
run_command() {
    echo "Running: $1"
    eval "$1"
    if [ $? -ne 0 ]; then
        echo "Error running command: $1"
        exit 1
    fi
}

# Step 1: Run the extraction
HEADLESS_OPTION=""
if [ "$HEADLESS" = true ]; then
    HEADLESS_OPTION="--headless"
fi

echo "Step 1: Running extraction for area: $AREA with $SESSIONS sessions..."
run_command "python3 multi_session_extractor.py --area \"$AREA\" --sessions $SESSIONS $HEADLESS_OPTION --db-path \"$DB_PATH\""

# Step 2: Monitor the extraction progress
echo "Step 2: Monitoring extraction progress..."
run_command "python3 monitor_extraction.py --db-path \"$DB_PATH\" --refresh $REFRESH_INTERVAL"

# Step 3: Analyze the results
echo "Step 3: Analyzing extraction results..."
run_command "python3 analyze_results.py --db-path \"$DB_PATH\" --output-dir \"$ANALYSIS_DIR\""

echo "Extraction process completed successfully!"
echo "Results saved to $OUTPUT_DIR"
echo "Analysis saved to $ANALYSIS_DIR"
