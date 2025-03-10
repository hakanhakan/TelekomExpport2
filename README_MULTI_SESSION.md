# Multi-Session Property Extraction System

This system allows extracting property information from the Telekom IBT portal using multiple browser sessions in parallel for improved performance and resilience.

## Features

- **Multiple Sessions**: Run multiple browser sessions in parallel to speed up data extraction
- **Checkpoint System**: Save progress to a SQLite database to allow resuming extraction if interrupted
- **Automatic Retry**: Automatically retry failed properties
- **Export Results**: Export results to Excel and CSV formats
- **Monitoring**: Real-time monitoring of extraction progress

## Requirements

- Python 3.8 or higher
- Playwright
- Pandas
- Rich (for console output)
- Dotenv (for environment variables)

## Setup

1. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

2. Create a `.env` file with your credentials:
   ```
   TELEKOM_USERNAME=your_username
   TELEKOM_PASSWORD=your_password
   TELEKOM_OTP_SECRET=your_otp_secret  # Optional
   ```

3. Make the shell script executable (Linux/macOS only):
   ```
   chmod +x run_multi_extraction.sh
   ```

## Usage

### Linux/macOS

```bash
./run_multi_extraction.sh --area "City Name" --sessions 4 --headless
```

### Windows

```batch
run_multi_extraction.bat --area "City Name" --sessions 4 --headless
```

### Command-line Options

- `--area`: (Required) The area/city to search for properties
- `--sessions`: (Optional) Number of browser sessions to run in parallel (default: 4)
- `--headless`: (Optional) Run browsers in headless mode (no GUI)
- `--db-path`: (Optional) Path to SQLite database file (default: property_extraction.db)
- `--debug`: (Optional) Enable debug logging

## How It Works

1. The system first downloads an Excel file with property data for the specified area
2. It extracts property IDs from the Excel file and stores them in a SQLite database
3. Multiple browser sessions are launched in parallel to process properties
4. Each session logs in, navigates to the property details page, and extracts owner information
5. Progress is saved to the database in real-time, allowing for resumption if interrupted
6. When all properties are processed, results are exported to Excel and CSV files

## Resuming Extraction

If the extraction process is interrupted, you can simply run the same command again. The system will automatically resume from where it left off, processing any remaining properties.

## Monitoring

During extraction, the system displays real-time progress information in the console, including:
- Total number of properties
- Number of pending, in-progress, completed, and failed properties
- Status of each worker session
- Properties processed by each session

## Output

The extraction results are saved to the `output` directory in both Excel and CSV formats. The filenames include a timestamp to avoid overwriting previous results.

## Troubleshooting

- If a session fails to log in, check your credentials in the `.env` file
- If the system fails to extract property IDs from the Excel file, check that the file contains a column with property IDs
- If a property fails to process, it will be marked as failed in the database and can be retried later
