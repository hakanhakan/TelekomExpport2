Below is an example of a comprehensive README.md file that explains the project, its structure, usage, and recent changes (including the fact that functionality from ibt_property_search.py is now merged into property_data.py and that the project uses uv for package management):

# TelekomExpport2 Scraper

TelekomExpport2 is a multi-session web scraper that logs into the Telekom IBT Order Portal, handles OTP-based authentication robustly, and extracts detailed property and owner data from the search results. The extracted data is saved to an SQLite database and can be reviewed in a tabulated format.

> **Note:** Functionality previously maintained in `ibt_property_search.py` has been merged into `property_data.py`. You can now remove the `ibt_property_search.py` file.

## Features

- **Robust OTP Handling:** Uses a custom OTP input routine with automatic retries for authentication.
- **Concurrent Sessions:** Runs multiple sessions concurrently to speed up the extraction process.
- **Property & Owner Data Extraction:** Extracts property details (e.g., FoL-ID, street, house number, house number appendix) as well as owner information (name, email, mobile, and landline).  
- **Integrated Package Management:** Uses **uv** for package management.
- **Data Storage:** Saves all extracted data into an SQLite database (`extraction.db`).

## Requirements

- Python 3.12+
- [Playwright](https://playwright.dev/python/)
- [aiosqlite](https://pypi.org/project/aiosqlite/)
- [rich](https://pypi.org/project/rich/)
- [tabulate](https://pypi.org/project/tabulate/)
- **uv** (for package management; install via pip if not already installed: `pip install uv`)

## Setup

1. **Clone the Repository:**

   ```bash
   git clone <repository_url>
   cd TelekomExpport2

	2.	Install Dependencies with uv:
Ensure you have uv installed globally. Then, install all required packages with:

uv install


	3.	Configure Environment Variables:
Set the following environment variables (you can use your shell configuration or a .env file):
	•	TELEKOM_USERNAME: Your Telekom username.
	•	TELEKOM_PASSWORD: Your Telekom password.
	•	TELEKOM_OTP_SECRET: Your OTP secret (used for generating the one-time passwords).

Usage

Run the main script using uv:

uv run debug_multiple_robust_otp.py

The script will:
	•	Launch multiple sessions.
	•	Log in to the Telekom portal using robust OTP handling.
	•	Extract property data across multiple pages.
	•	Save the extracted data into extraction.db.

Code Structure
	•	debug_multiple_robust_otp.py:
The main script that initializes multiple sessions, performs OTP-based login, extracts property data, and manages page navigation and concurrent processing.
	•	property_data.py:
Contains functions and classes for processing property and owner data. This module now includes functionality originally from ibt_property_search.py, so that file is no longer necessary.
	•	ibt_property_search.py:
(Deprecated) The code in this file has been merged into property_data.py for a streamlined codebase.

How It Works
	1.	Login & OTP Handling:
	•	The script uses a robust OTP routine that waits for the OTP input field, fills it with a freshly generated code (using pyotp and a custom TOTP implementation supporting SHA512), and submits the form.
	•	It retries OTP input a specified number of times if the field does not verify the correct value.
	2.	Data Extraction:
	•	Once logged in, the scraper navigates the property search results.
	•	For each property row, it extracts static data (e.g., FoL-ID, street, house number) and then clicks to view property details.
	•	The process_property function handles the extraction of owner details from the property’s Owner tab. It uses a try/except block to gracefully handle cases where the owner table does not appear within a timeout. In such cases, a status message is recorded in an extra column in the database.
	3.	Pagination:
	•	The scraper navigates through pages by clicking the next button (or directly selecting a page if possible) until all assigned pages are processed.
	4.	Database Storage:
	•	Extracted data is saved in an SQLite database (extraction.db) with columns for session ID, page, FoL-ID, street, house number, house appendix, owner details, and an execution status message (filled only if an error occurs during extraction).

Logging & Error Handling
	•	Detailed logging is provided for every session, including OTP submission, data extraction for each property, and any errors (e.g., timeouts when waiting for elements).
	•	When an element (like the property tab view or owner table) is not found within the specified timeout, a warning is logged and an error status is recorded in the database.

Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your improvements.

License

This project is licensed under the MIT License.

Acknowledgments
	•	The project leverages Playwright for robust web automation.
	•	The custom OTP handling extends the functionality of pyotp to support SHA512.
	•	Thanks to the developers and the community behind the libraries used in this project.
	•	Package management is streamlined with uv to simplify dependency management and deployment.

You can save this content as `README.md` in your project root. Adjust any URLs or additional details as necessary.