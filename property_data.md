# property_data.py

## Overview

`property_data.py` is a specialized web scraping script for automatically extracting property information from the Telekom Contractor Portal (`glasfaser.telekom.de/auftragnehmerportal-ui`). The script handles authentication, navigation, and extraction of property data, including owner information, from multiple pages.

## Key Features

- **Multi-Session Parallelization**: Runs multiple browser sessions in parallel to speed up data extraction
- **Robust Authentication**: Handles login with username/password and OTP (One-Time Password) verification
- **Retry Mechanism**: Implements intelligent retries for extracting owner information when initial attempts fail
- **Exploration Protocol Downloads**: Downloads exploration PDF files and tracks which have already been downloaded
- **Database Storage**: Persists extracted data to an SQLite database with change tracking
- **Comprehensive Logging**: Maintains detailed logs of all operations and errors

## Workflow Diagram

The following diagram illustrates the overall workflow of the script, including the retry mechanism for owner information extraction:

```mermaid
flowchart TD
    A[Start Script] --> B[Initialize Multiple Browser Sessions]
    B --> C[Login to Telekom Portal]
    C --> D[Set Search Parameters]
    D --> E[Divide Pages Among Sessions]
    
    E --> F[For Each Page in Session's Range]
    F --> G[Extract Property List]
    G --> H[For Each Property in Page]
    
    H --> I[Open Property Detail]
    I --> J[Navigate to Owner Tab]
    J --> K[Extract Owner Information]
    
    K --> L{Owner Info\nExtracted?}
    L -->|Yes| M[Process Exploration Data]
    L -->|No| N{Retry\nNeeded?}
    
    N -->|No| M
    N -->|Yes| O[Retry Extraction\n(Up to 2 times)]
    O --> P{Retry\nSuccessful?}
    P -->|Yes| M
    P -->|No, Max Retries| M
    
    M --> Q[Save Property to Database]
    Q --> R{More Properties\non Page?}
    R -->|Yes| H
    R -->|No| S{More Pages\nin Range?}
    
    S -->|Yes| F
    S -->|No| T[Close Session]
    T --> U[End Script]
```

## Prerequisites

### Environment Variables
The script requires the following environment variables:
- `TELEKOM_USERNAME`: Your Telekom portal username
- `TELEKOM_PASSWORD`: Your Telekom portal password
- `TELEKOM_OTP_SECRET`: Secret key for generating OTP codes (optional, but recommended)

### Python Dependencies
```
asyncio
logging
os
urllib.parse
hmac
hashlib
json
struct
tabulate
aiosqlite
rich
pyotp
playwright
dotenv
```

## Installation

1. Install Python 3.7 or newer
2. Install required packages using `uv`:
   ```bash
   uv pip install asyncio aiosqlite rich pyotp python-dotenv playwright tabulate
   ```
   
   Or alternatively, create and activate a virtual environment with `uv`:
   ```bash
   uv venv
   source .venv/bin/activate  # On Linux/macOS
   # OR
   .venv\Scripts\activate     # On Windows
   uv pip install asyncio aiosqlite rich pyotp python-dotenv playwright tabulate
   ```
3. Install Playwright browsers:
   ```bash
   playwright install
   ```
4. Create a `.env` file in the same directory with the required credentials:
   ```
   TELEKOM_USERNAME=your_username
   TELEKOM_PASSWORD=your_password
   TELEKOM_OTP_SECRET=your_otp_secret
   ```

## Usage

Run the script using `uv` from the command line:

```bash
uv run property_data.py
```

Or if you've activated a virtual environment created with `uv`:

```bash
python property_data.py
```

The script will:
1. Initialize multiple browser sessions
2. Log into the Telekom portal in each session
3. Set search parameters (currently hardcoded to "Bad Sooden-Allendorf, Stadt")
4. Divide the result pages among sessions
5. Extract property data from all pages, with retries for owner information
6. Save the data to an SQLite database named `extraction.db`

## Key Components

### Classes

- `IBTPropertySearchSession`: Base class for handling browser interaction with the Telekom portal
- `RobustIBTPropertySearchSession`: Extended class with improved OTP handling
- `CustomTOTP`: Extended TOTP implementation that supports SHA512 for OTP generation

### Main Functions

- `extract_search_results`: Extracts property data from search results with retry mechanism
- `process_property`: Processes a single property's detailed information
- `extract_ownership`: Extracts owner information from a property's details page
- `download_exploration_pdf`: Downloads exploration protocol PDFs
- `save_page_data_to_db`: Saves extracted data to the SQLite database
- `process_page_range`: Processes a range of result pages
- `main`: Main execution function that coordinates the multi-session extraction

## Retry Mechanism for Owner Information

The script implements a specialized retry mechanism for owner information extraction:

1. If owner extraction fails (but not due to a missing owner table), the script will retry up to 2 more times
2. During each retry, it:
   - Reopens the property detail page
   - Navigates to the owner tab
   - Attempts to extract the owner information again
3. If successful on retry, it marks the recovery in the status
4. If all retries fail, it proceeds with the available data

This approach increases the completeness of extracted owner data by addressing temporary extraction failures before moving to the next page of results.

## Database Schema

The script creates an SQLite database with the following schema for the `property_data` table:

| Column | Type | Description |
|--------|------|-------------|
| fol_id | TEXT | Property ID (Primary Key) |
| session_id | INTEGER | Session ID that processed this property |
| page | INTEGER | Page number where the property was found |
| street | TEXT | Street name |
| house_number | TEXT | House number |
| house_appendix | TEXT | House number appendix |
| owner_name | TEXT | Property owner's name |
| owner_email | TEXT | Property owner's email |
| owner_mobile | TEXT | Property owner's mobile number |
| owner_landline | TEXT | Property owner's landline number |
| status | TEXT | Status message or extraction notes |
| exploration | TEXT | Exploration date |
| exploration_pdf | TEXT | Path to downloaded exploration PDF |
| au | TEXT | Accommodation Units |
| bu | TEXT | Business Units |
| nvt_area | TEXT | NVT Area |
| data_hash | TEXT | Hash of the property data for change detection |
| changed_flag | INTEGER | Flag indicating if data has changed (0/1) |
| last_updated | TIMESTAMP | Timestamp of last update |

## Logs

The script creates log files:
- `ibt_search.log`: Main log file (rotated when it reaches 10MB)

## Troubleshooting

- **Login Issues**: Check that your credentials in the `.env` file are correct
- **OTP Failures**: Verify that your OTP secret is correct and properly formatted
- **Extraction Errors**: Examine the log files for specific error messages
- **Slow Performance**: Consider adjusting the number of sessions (`num_sessions`) based on your system's capabilities

## Limitations

- Currently, the search area is hardcoded to "Bad Sooden-Allendorf, Stadt"
- The number of pages to process is set to 49
- The script uses a headless browser by default, which may cause issues with some CAPTCHAs or security measures

## Further Development

Potential improvements include:
- Command-line arguments for search parameters
- Better error recovery for session failures
- Improved PDF handling and metadata extraction
- Export functionality to CSV/Excel formats