# Multi-Session IBT Property Extraction System

This system provides a robust solution for extracting property information from the Telekom IBT portal using multiple browser sessions in parallel. It's designed to handle large datasets efficiently while providing resilience against failures.

## Features

- **Multi-Session Processing**: Run multiple browser sessions in parallel to speed up extraction
- **Checkpointing**: Automatically save progress to a SQLite database to resume after interruptions
- **Resilience**: Automatically recover from failures and continue processing
- **Progress Monitoring**: Real-time statistics on extraction progress
- **Export Options**: Export results to Excel and CSV formats

## Installation

### Prerequisites

- Python 3.8 or higher
- Playwright for Python
- Required Python packages

### Setup

1. Install the required Python packages using the provided requirements.txt file:

```bash
pip install -r requirements.txt
```

2. Install Playwright browsers:

```bash
playwright install
```

3. Set up environment variables by copying the template:

```bash
cp .env.template .env
```

Then edit the `.env` file with your credentials:

```
TELEKOM_USERNAME=your_username
TELEKOM_PASSWORD=your_password
TELEKOM_OTP_SECRET=your_otp_secret  # Optional
```

## Usage

### One-Step Extraction

For convenience, scripts are provided that run the entire extraction process from start to finish:

**Linux/macOS:**
```bash
./run_extraction.sh --area "Berlin" --sessions 4
```

**Windows:**
```cmd
run_extraction.bat --area "Berlin" --sessions 4
```

Options:
- `--area`: Area to search for properties (default: Berlin)
- `--sessions`: Number of parallel browser sessions (default: 4)
- `--headless`: Run in headless mode (no visible browser windows)
- `--db-path`: Path to the SQLite database file (default: property_extraction.db)
- `--output-dir`: Directory to save extraction results (default: output)
- `--analysis-dir`: Directory to save analysis results (default: analysis)
- `--refresh`: Refresh interval in seconds for monitoring (default: 5)
- `--help`: Show help message

These scripts will:
1. Run the extraction process
2. Monitor the progress
3. Analyze the results when complete

### Testing the System

Before running a full extraction, it's recommended to test the system with a small sample:

```bash
python test_extraction.py --excel-file "downloads/ibt-properties_20250306_123315.xlsx" --num-properties 5
```

Options:
- `--excel-file`: Path to the Excel file with property IDs (required)
- `--num-properties`: Number of properties to test with (default: 5)
- `--sessions`: Number of parallel browser sessions (default: 2)
- `--headless`: Run in headless mode (no visible browser windows)
- `--db-path`: Path to the SQLite database file (default: test_extraction.db)
- `--debug`: Enable debug logging
- `--quiet`: Suppress non-essential output

This will run a test extraction with a small number of properties to verify that the system is working correctly.

### Initial Extraction

To start a new extraction process:

```bash
python multi_session_extractor.py --area "Berlin" --sessions 4
```

Options:
- `--area`: The area to search for properties (required)
- `--sessions`: Number of parallel browser sessions (default: 4)
- `--headless`: Run in headless mode (no visible browser windows)
- `--db-path`: Path to the SQLite database file (default: property_extraction.db)
- `--debug`: Enable debug logging
- `--quiet`: Suppress non-essential output

### Resume Extraction

If the extraction process was interrupted, you can resume from an existing Excel file:

```bash
python resume_extraction.py --excel-file "downloads/ibt-properties_20250306_123315.xlsx" --sessions 4
```

Options:
- `--excel-file`: Path to the Excel file with property IDs (required)
- `--sessions`: Number of parallel browser sessions (default: 4)
- `--headless`: Run in headless mode (no visible browser windows)
- `--db-path`: Path to the SQLite database file (default: property_extraction.db)
- `--debug`: Enable debug logging
- `--quiet`: Suppress non-essential output

### Monitoring Extraction Progress

To monitor the progress of an ongoing extraction in a more visual way:

```bash
python monitor_extraction.py --db-path "property_extraction.db" --refresh 5
```

Options:
- `--db-path`: Path to the SQLite database file (default: property_extraction.db)
- `--refresh`: Refresh interval in seconds (default: 5)

This will display a rich interface with:
- Overall progress bar
- Per-session progress bars
- Extraction statistics (completion percentage, processing rate, estimated time remaining)
- Recently completed properties
- Recently failed properties

The monitor will automatically exit when the extraction is complete, or you can press Ctrl+C to stop it at any time.

### Analyzing Results

After the extraction is complete, you can analyze the results to gain insights:

```bash
python analyze_results.py --db-path "property_extraction.db" --output-dir "analysis"
```

Options:
- `--db-path`: Path to the SQLite database file (default: property_extraction.db)
- `--output-dir`: Directory to save analysis results (default: analysis)

This will generate:
- Tables and charts showing extraction status
- Distribution of properties by city
- Distribution of properties by property status
- Owner information statistics
- Analysis of failure reasons
- Summary of the extraction results

The analysis results are saved to the specified output directory in both CSV and Excel formats, along with visualizations as PNG images.

## Architecture

The system consists of the following components:

### Coordinator

The `PropertyExtractionCoordinator` class manages the overall extraction process:
- Downloads the Excel file with property data
- Extracts property IDs from the Excel file
- Initializes the database with property IDs
- Starts and monitors worker sessions
- Exports results when extraction is complete

### Workers

The `PropertyExtractionWorker` class handles property extraction in a single browser session:
- Logs in to the IBT portal
- Processes batches of properties
- Reports progress to the coordinator
- Implements retry logic for resilience

### Database Manager

The `DatabaseManager` class manages the SQLite database for checkpointing:
- Tracks the status of each property (pending, in-progress, completed, failed)
- Assigns properties to worker sessions
- Stores extracted property data
- Provides progress statistics
- Exports results to Excel and CSV

## Troubleshooting

### Common Issues

1. **Login Failures**:
   - Check your credentials in the `.env` file
   - Ensure your OTP secret is correct if using OTP authentication

2. **Browser Crashes**:
   - Reduce the number of parallel sessions
   - Ensure your system has enough memory

3. **Stalled Properties**:
   - Properties that are stuck in "in-progress" state for more than 30 minutes will be automatically reset and reassigned

4. **Database Errors**:
   - If the database becomes corrupted, you can delete it and start fresh
   - Use the resume_extraction.py script to restart from your Excel file

### Logs

Detailed logs are saved to:
- `ibt_search.log`: Main log file for the IBT property search module
- Console output: Real-time progress information

## Output

Extraction results are saved to the `output` directory in both Excel and CSV formats. The files are named with a timestamp to avoid overwriting previous results.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
