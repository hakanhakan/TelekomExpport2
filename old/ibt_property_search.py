#!/usr/bin/env python3
"""
Telekom IBT Property Search Module
This module provides functionality for searching and extracting property information
from the Telekom IBT portal.
"""

import asyncio
import argparse
import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from playwright.async_api import async_playwright, Browser, Page
from rich.console import Console
from rich.table import Table
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import json
import time
import pyotp
import urllib.parse
import hmac
import hashlib
import struct
from logging.handlers import RotatingFileHandler

load_dotenv()

# Global variables
shutdown_requested = False  # Used for graceful shutdown
exit_key_pressed = False
save_events_before_exit = False

def setup_logging(debug=False, quiet=False):
    logger = logging.getLogger()
    logger.handlers.clear()

    # File handler (always verbose)
    file_handler = RotatingFileHandler(
        'ibt_search.log',
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    file_handler.setLevel(logging.DEBUG)

    # Console handler configuration
    console_handler = logging.StreamHandler()
    if quiet:
        console_handler.setLevel(logging.CRITICAL)
        console_handler.setFormatter(logging.Formatter('%(message)s'))
    elif debug:
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    else:
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(message)s'))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)

# Initialize Rich console
console = Console()

class PropertyData(BaseModel):
    """Data model for property information"""
    property_id: str
    address: Optional[str] = None
    postal_code: Optional[str] = None
    city: Optional[str] = None
    status: Optional[str] = None
    owner_name: Optional[str] = None
    owner_email: Optional[str] = None
    owner_mobile: Optional[str] = None
    owner_phone: Optional[str] = None
    is_decision_maker: bool = False
    owner_details_loaded: bool = False
    additional_fields: dict = {}

class CustomTOTP(pyotp.TOTP):
    """Custom TOTP implementation that supports SHA512"""
    
    def __init__(self, s, digits=6, digest='sha1', name=None, issuer=None, interval=30):
        """Initialize the TOTP object with custom digest support"""
        self.secret = s
        self.digits = digits
        self.digest = digest.lower()  # Support sha1, sha256, sha512
        self.name = name or 'TOTP'
        self.issuer = issuer
        self.interval = interval
    
    def generate_otp(self, input):
        """Generate the OTP using the specified digest algorithm"""
        if self.digest == 'sha1':
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', input), hashlib.sha1)
        elif self.digest == 'sha256':
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', input), hashlib.sha256)
        elif self.digest == 'sha512':
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', input), hashlib.sha512)
        else:
            # Default to SHA1 if unknown digest
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', input), hashlib.sha1)
        
        hmac_hash = bytearray(hasher.digest())
        offset = hmac_hash[-1] & 0x0F
        code = ((hmac_hash[offset] & 0x7F) << 24 |
                (hmac_hash[offset + 1] & 0xFF) << 16 |
                (hmac_hash[offset + 2] & 0xFF) << 8 |
                (hmac_hash[offset + 3] & 0xFF))
        code = code % 10 ** self.digits
        return code

    def now(self):
        """Generate the current time OTP"""
        timecode = int(time.time()) // self.interval
        return self.generate_code(timecode)
    
    def generate_code(self, input):
        """Generate a code using the digit count"""
        result = self.generate_otp(input)
        return str(result).zfill(self.digits)

class IBTPropertySearchSession:
    """Handles individual browser session for property searching"""
    def __init__(self, username: str, password: str, session_id: int, headless=False):
        self.username = username
        self.password = password
        self.session_id = session_id
        self.headless = headless
        self.base_url = "https://glasfaser.telekom.de/auftragnehmerportal-ui"
        self.login_url = f"{self.base_url}/order/ibtorder/search?a-cid=58222"
        self.search_url = f"{self.base_url}/property/search"
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.context = None
        self.playwright = None
        self.download_dir = Path("downloads")
        self.download_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(f"Session {self.session_id}")
        self.otp_secret = None
        # Load OTP secret from environment variable if available
        self.otp_secret = os.getenv("TELEKOM_OTP_SECRET")
        logging.info(f"Session {self.session_id}: Loaded OTP secret from environment: {self.otp_secret is not None}")
        
    async def init_browser(self):
        """Initialize browser instance"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
            


    async def login(self) -> bool:
        """Handle login process"""
        try:
            await self.page.goto(self.login_url)
            
            try:
                username_field = await self.page.wait_for_selector('input[name="username"]', timeout=5000)
                if username_field:
                    logging.info(f"Session {self.session_id}: Login page detected, entering credentials")
                    await username_field.fill(self.username)
                    password_field = await self.page.wait_for_selector('input[name="password"]', timeout=5000)
                    if password_field:
                        await password_field.fill(self.password)
                    else:
                        logging.error(f"Session {self.session_id}: Password field not found")
                        return False
                    
                    # Look for the specific "Anmelden" button and click it
                    anmelden_button = await self.page.wait_for_selector('input#kc-login[value="Anmelden"]', timeout=5000)
                    if anmelden_button:
                        logging.info(f"Session {self.session_id}: Clicking 'Anmelden' button")
                        await anmelden_button.click()
                    else:
                        # Fallback to the generic submit button if the specific button is not found
                        logging.info(f"Session {self.session_id}: 'Anmelden' button not found, using generic submit button")
                        await self.page.click('button[type="submit"]')
                    
                    await self.page.wait_for_timeout(5000)
                    
                    current_url = self.page.url
                    if "authenticate" in current_url:
                        console.print(f"[yellow]OTP required for session {self.session_id}.[/yellow]")
                        
                        try:
                            # Wait for the OTP radio button to be visible
                            otp_radio_button = await self.page.wait_for_selector('input#kc-otp-credential-2[type="radio"]', timeout=5000)
                            if otp_radio_button:
                                logging.info(f"Session {self.session_id}: Clicking OTP radio button with ID 'kc-otp-credential-2'")
                                await otp_radio_button.click()
                                await self.page.wait_for_timeout(1000)  # Short wait after clicking
                            else:
                                logging.info(f"Session {self.session_id}: OTP radio button not found")
                            
                            # If we have an OTP secret, generate and enter the OTP code
                            if self.otp_secret:
                                logging.info(f"Session {self.session_id}: Using OTP secret: {self.otp_secret[:15]}...")
                                # Generate OTP code
                                otp_code = self.generate_otp_code(self.otp_secret)
                                if otp_code:
                                    logging.info(f"Session {self.session_id}: Generated OTP code: {otp_code}")
                                    
                                    # Wait for the OTP input field to be visible
                                    otp_input = await self.page.wait_for_selector('input#otp[name="otp"]', timeout=5000)
                                    if otp_input:
                                        logging.info(f"Session {self.session_id}: Filling OTP code in input field")
                                        await otp_input.fill(otp_code)
                                        
                                        # Look for the submit button and click it
                                        submit_button = await self.page.wait_for_selector('input[type="submit"]', timeout=5000)
                                        if submit_button:
                                            logging.info(f"Session {self.session_id}: Clicking OTP submit button")
                                            await submit_button.click()
                                        else:
                                            logging.info(f"Session {self.session_id}: OTP submit button not found")
                                    else:
                                        logging.info(f"Session {self.session_id}: OTP input field not found")
                                else:
                                    logging.error(f"Session {self.session_id}: Failed to generate OTP code")
                            else:
                                console.print("[yellow]No OTP secret provided. Please enter the OTP manually in the browser.[/yellow]")
                        except Exception as e:
                            logging.error(f"Session {self.session_id}: Error handling OTP: {str(e)}")
                        
                        console.print("[yellow]Waiting for OTP verification to complete...[/yellow]")
                        
                        max_wait = 120
                        for _ in range(max_wait):
                            current_url = self.page.url
                            if "authenticate" not in current_url:
                                logging.info(f"Session {self.session_id}: OTP verification completed")
                                break
                            await self.page.wait_for_timeout(1000)
                        else:
                            logging.error(f"Session {self.session_id}: OTP verification timed out")
                            return False
                    else:
                        logging.info(f"Session {self.session_id}: No OTP required")
                    
                    console.print("[yellow]Please complete the OTP verification in the browser window.[/yellow]")
                    console.print("[yellow]The script will continue once you're logged in.[/yellow]")
                    
                    max_wait = 120
                    for _ in range(max_wait):
                        current_url = self.page.url
                        if "authenticate" not in current_url:
                            logging.info(f"Session {self.session_id}: OTP verification completed")
                            break
                        await self.page.wait_for_timeout(1000)
                    else:
                        logging.error(f"Session {self.session_id}: OTP verification timed out")
                        return False
            except Exception as e:
                logging.info(f"Session {self.session_id}: Already logged in or login page not as expected: {str(e)}")
            
            await self.page.wait_for_timeout(5000)
            
            current_url = self.page.url
            logging.info(f"Session {self.session_id}: Current URL after login process: {current_url}")
            
            if "order/ibtorder/search" in current_url:
                logging.info(f"Session {self.session_id}: Successfully logged in")
                await self.page.goto(self.search_url)
                await self.page.wait_for_timeout(5000)
                
                console.print(f"[green]Successfully logged in session {self.session_id} and navigated to property search![/green]")
                return True
            else:
                logging.error(f"Session {self.session_id}: Failed to reach the expected page after login")
                return False
            
        except Exception as e:
            logging.error(f"Login failed for session {self.session_id}: {str(e)}")
            return False

    def generate_otp_code(self, otp_secret):
        """Generate an OTP code from the given secret"""
        if not otp_secret:
            logging.error(f"Session {self.session_id}: No OTP secret provided")
            return None
        
        logging.info(f"Session {self.session_id}: Attempting to generate OTP code from secret: {otp_secret[:15]}...")
        
        try:
            # Check if the input is already a otpauth:// URL
            if otp_secret.startswith('otpauth://'):
                logging.info(f"Session {self.session_id}: Processing otpauth:// URL")
                # Parse the otpauth URL
                parsed_url = urllib.parse.urlparse(otp_secret)
                query_params = dict(urllib.parse.parse_qsl(parsed_url.query))
                
                # Extract the secret from the query parameters
                secret = query_params.get('secret')
                if not secret:
                    logging.error(f"Session {self.session_id}: No secret found in otpauth URL")
                    return None
                
                logging.info(f"Session {self.session_id}: Extracted secret: {secret[:5]}...")
                
                # Get algorithm if specified
                algorithm = query_params.get('algorithm', 'SHA1')
                logging.info(f"Session {self.session_id}: Using algorithm: {algorithm}")
                
                # Get digits if specified
                digits = int(query_params.get('digits', 6))
                logging.info(f"Session {self.session_id}: Using digits: {digits}")
                
                # Get period if specified
                period = int(query_params.get('period', 30))
                logging.info(f"Session {self.session_id}: Using period: {period}")
                
                # Generate the OTP code
                if algorithm.upper() == 'SHA512':
                    logging.info(f"Session {self.session_id}: Using custom TOTP implementation for SHA512")
                    totp = CustomTOTP(secret, digits=digits, digest='sha512', interval=period)
                else:
                    # Use standard PyOTP for other algorithms
                    totp = pyotp.TOTP(secret, digits=digits, digest=algorithm.lower(), interval=period)
                
                otp_code = totp.now()
                logging.info(f"Session {self.session_id}: Generated OTP code: {otp_code}")
                return otp_code
            # Special case for format like "totp/Telekom:hakan%40ekerfiber.com?secret=..."
            elif 'secret=' in otp_secret:
                logging.info(f"Session {self.session_id}: Processing partial otpauth URL")
                # Extract the secret parameter
                secret_param = otp_secret.split('secret=')[1].split('&')[0]
                logging.info(f"Session {self.session_id}: Extracted secret: {secret_param[:5]}...")
                
                # Extract other parameters if available
                digits = 6
                if 'digits=' in otp_secret:
                    digits_str = otp_secret.split('digits=')[1].split('&')[0]
                    digits = int(digits_str)
                    logging.info(f"Session {self.session_id}: Using digits: {digits}")
                
                algorithm = 'SHA1'
                if 'algorithm=' in otp_secret:
                    algorithm = otp_secret.split('algorithm=')[1].split('&')[0]
                    logging.info(f"Session {self.session_id}: Using algorithm: {algorithm}")
                
                period = 30
                if 'period=' in otp_secret:
                    period_str = otp_secret.split('period=')[1].split('&')[0]
                    period = int(period_str)
                    logging.info(f"Session {self.session_id}: Using period: {period}")
                
                # Generate the OTP code
                if algorithm.upper() == 'SHA512':
                    logging.info(f"Session {self.session_id}: Using custom TOTP implementation for SHA512")
                    totp = CustomTOTP(secret_param, digits=digits, digest='sha512', interval=period)
                else:
                    # Use standard PyOTP for other algorithms
                    totp = pyotp.TOTP(secret_param, digits=digits, digest=algorithm.lower(), interval=period)
                
                otp_code = totp.now()
                logging.info(f"Session {self.session_id}: Generated OTP code: {otp_code}")
                return otp_code
            else:
                logging.info(f"Session {self.session_id}: Using direct secret")
                # Try to use the string directly as a secret
                totp = pyotp.TOTP(otp_secret)
                otp_code = totp.now()
                logging.info(f"Session {self.session_id}: Generated OTP code: {otp_code}")
                return otp_code
        except Exception as e:
            logging.error(f"Session {self.session_id}: Error generating OTP code: {str(e)}")
            return None

    async def set_search_criteria(self, area: str, creation_date_start: str = None, creation_date_end: str = None) -> bool:
        """Set the search criteria for property search"""
        try:
            logging.info(f"Session {self.session_id} setting search criteria")
            
            await self.page.screenshot(path=f"before_criteria_session_{self.session_id}.png")
            logging.info(f"Session {self.session_id}: Saved screenshot before setting criteria")
            
            await self.page.goto(self.search_url, wait_until="networkidle")
            await self.page.wait_for_timeout(5000)
            
            # Set the area/city using the extracted JavaScript functions
            area_set = await self.page.evaluate("""(area) => {
                function interactWithAreaInput(selector, value) {
                    let input;
                    if (typeof selector === 'string') {
                        input = document.querySelector(selector);
                    } else {
                        input = selector;
                    }
                    if (!input) return false;
                    input.value = '';
                    input.focus();
                    input.value = value;
                    ['change', 'input', 'focus', 'blur', 'keyup'].forEach(eventType => {
                        const event = new Event(eventType, { bubbles: true });
                        input.dispatchEvent(event);
                    });
                    return true;
                }
                
                const areaSelectors = [
                    '#searchCriteriaForm\\\\:vvmArea_input',
                    'input[name="searchCriteriaForm:vvmArea_input"]',
                    'input[id*="gigaArea"]',
                    'input[id*="area"]'
                ];
                
                for (const selector of areaSelectors) {
                    if (interactWithAreaInput(selector, area)) {
                        return true;
                    }
                }
                return false;
            }""", area)
            
            if area_set:
                logging.info(f"Session {self.session_id}: Set area '{area}' successfully")
            else:
                logging.error(f"Session {self.session_id}: Failed to set area")
                return False

            # Set number of results to 2500
            await self.page.evaluate(r"""() => {
                const hiddenInput = document.querySelector('#searchCriteriaForm\\:nrOfResults_input');
                if (hiddenInput) {
                    hiddenInput.value = "2500";
                    const event = new Event('change', { bubbles: true });
                    hiddenInput.dispatchEvent(event);
                    return true;
                }
                return false;
            }""")
            
            await self.page.wait_for_timeout(1000)
            
            # Click search button
            search_button = await self.page.query_selector('#searchCriteriaForm\\:searchButton')
            if search_button:
                await search_button.click()
                logging.info(f"Session {self.session_id}: Clicked search button")
            else:
                logging.error(f"Session {self.session_id}: Could not find search button")
                return False
            
            await self.page.wait_for_timeout(5000)
            
            # Take a screenshot after search
            await self.page.screenshot(path=f"after_search_session_{self.session_id}.png")
            
            return True
            
        except Exception as e:
            logging.error(f"Session {self.session_id}: Error setting search criteria: {str(e)}")
            logging.error(traceback.format_exc())
            return False

    async def search_by_area(self, area) -> bool:
        """Search for properties by area"""
        try:
            # Wait for the area search field to be visible
            area_input = await self.page.wait_for_selector('input[name="searchCriteriaForm:vvmArea_input"]', timeout=10000)
            if not area_input:
                self.logger.error("Area search input field not found")
                return False
                
            # Clear the field and type the area
            await area_input.click()
            await area_input.fill("")
            await area_input.type(area, delay=100)
            
            # Wait a bit for suggestions to appear
            await asyncio.sleep(1)

            # Set number of results to 2500
            dropdown = await self.page.wait_for_selector('#searchCriteriaForm\\:nrOfResults', timeout=10000)
            if not dropdown:
                self.logger.error("Number of results dropdown not found")
                return False

            # Click the dropdown to open it
            await dropdown.click()
            await asyncio.sleep(0.5)

            # Select 2500 option
            option_2500 = await self.page.wait_for_selector('li[data-label="2500"]', timeout=10000)
            if not option_2500:
                self.logger.error("2500 option not found in dropdown")
                return False
            await option_2500.click()
            await asyncio.sleep(0.5)

            # Click the search button
            search_button = await self.page.wait_for_selector('#searchCriteriaForm\\:searchButton', timeout=5000)
            if not search_button:
                self.logger.error("Search button not found")
                return False
                
            await search_button.click()
            
            # Wait for results to load
            await self.page.wait_for_selector('#searchResultForm\\:propertySearchSRT', timeout=20000)
            await asyncio.sleep(2)  # Give the page a moment to fully load
            
            # After successful search, ensure the Excel file is downloaded with retries
            download_attempts = 0
            excel_file = None
            while download_attempts < 3 and not excel_file:
                excel_file = await self.download_search_results_excel()
                if excel_file:
                    self.logger.info(f"Successfully downloaded search results to: {excel_file}")
                else:
                    self.logger.error(f"Download attempt {download_attempts + 1} failed.")
                    download_attempts += 1
                    await asyncio.sleep(2)
            if not excel_file:
                self.logger.error("Failed to download search results Excel file after 3 attempts")
                return False
            
            self.logger.info(f"Successfully searched for area: {area}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error searching by area: {str(e)}")
            return False

    async def extract_properties_from_results(self) -> List[PropertyData]:
        """Extract all properties from the search results"""
        properties = []
        try:
            logging.info(f"Session {self.session_id}: Waiting for search results to be visible")
            
            # Take a screenshot of the current page state for debugging
            await self.page.screenshot(path=f"search_results_before_{self.session_id}.png")
            
            # First approach: Try to find the table directly
            table_selectors = [
                '#searchResultForm\\:propertySearchSRT_data',
                '#searchResultForm\\:propertySRT_data',
                '.ui-datatable-data',
                'table[id$="_data"]',
                'table.ui-datatable-data'
            ]
            
            table_selector = None
            for selector in table_selectors:
                try:
                    logging.info(f"Session {self.session_id}: Looking for table with selector: {selector}")
                    table = await self.page.query_selector(selector)
                    if table:
                        table_selector = selector
                        logging.info(f"Session {self.session_id}: Found table with selector: {selector}")
                        break
                except Exception as e:
                    logging.warning(f"Session {self.session_id}: Error finding table with selector {selector}: {str(e)}")
            
            # If we have a table selector, use it to find rows
            if table_selector:
                row_selectors = [
                    f"{table_selector} tr",
                    '#searchResultForm\\:propertySearchSRT_data tr',
                    '#searchResultForm\\:propertySRT_data tr',
                    '.ui-datatable-data tr',
                    'table tr'  # Most generic selector
                ]
            else:
                # Fallback to generic row selectors
                row_selectors = [
                    '#searchResultForm\\:propertySearchSRT_data tr',
                    '#searchResultForm\\:propertySRT_data tr',
                    '.ui-datatable-data tr',
                    'table tr'  # Most generic selector
                ]
            
            rows = []
            for selector in row_selectors:
                try:
                    logging.info(f"Session {self.session_id}: Looking for rows with selector: {selector}")
                    found_rows = await self.page.query_selector_all(selector)
                    if found_rows and len(found_rows) > 0:
                        rows = found_rows
                        logging.info(f"Session {self.session_id}: Found {len(rows)} rows with selector: {selector}")
                        break
                except Exception as e:
                    logging.warning(f"Session {self.session_id}: Error finding rows with selector {selector}: {str(e)}")
            
            if not rows or len(rows) == 0:
                logging.error(f"Session {self.session_id}: No rows found in search results")
                # Save the page HTML for debugging
                html_content = await self.page.content()
                with open(f"search_results_page_html_{self.session_id}.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                logging.info(f"Session {self.session_id}: Saved page HTML to search_results_page_html_{self.session_id}.html")
                return properties
            
            logging.info(f"Session {self.session_id}: Processing {len(rows)} rows in search results")
            
            # Process all rows starting from index 0
            for i in range(0, len(rows)):
                row = rows[i]
                try:
                    # Extract all cells from the row
                    cells = await row.query_selector_all('td')
                    
                    if len(cells) < 5:
                        logging.warning(f"Session {self.session_id}: Row {i} has fewer than 5 cells: {len(cells)}")
                        continue
                    
                    # Log the text content of each cell for debugging
                    cell_contents = []
                    for j, cell in enumerate(cells):
                        try:
                            # Try multiple approaches to get text content
                            # 1. Try inner_text first
                            cell_text = await cell.inner_text()
                            
                            # 2. If empty, try to get text from span elements
                            if not cell_text.strip():
                                spans = await cell.query_selector_all('span')
                                span_texts = []
                                for span in spans:
                                    span_text = await span.inner_text()
                                    if span_text.strip():
                                        span_texts.append(span_text.strip())
                                if span_texts:
                                    cell_text = ' '.join(span_texts)
                            
                            # 3. If still empty, try to get text content via JavaScript
                            if not cell_text.strip():
                                cell_text = await cell.evaluate('el => el.textContent')
                            
                            cell_contents.append(cell_text.strip())
                            logging.info(f"Session {self.session_id}: Row {i}, Cell {j} content: {cell_text.strip()}")
                        except Exception as cell_error:
                            logging.error(f"Session {self.session_id}: Error getting text from cell {j}: {str(cell_error)}")
                            cell_contents.append("")
                    
                    # Log all cell contents for debugging
                    logging.info(f"Session {self.session_id}: Row {i} has {len(cell_contents)} cells with content: {cell_contents}")
                    
                    # Skip rows that don't have enough content
                    if not any(cell_contents) or all(not c for c in cell_contents):
                        logging.warning(f"Session {self.session_id}: Row {i} has no content, skipping")
                        continue
                    
                    # Create property data dictionary
                    property_data = {}
                    
                    # Map cell contents to property fields
                    field_mapping = {
                        26: 'property_id',  # Using column 26 for the FOL ID/property ID
                        1: 'address',
                        2: 'postal_code',
                        3: 'city',
                        4: 'status'
                    }
                    
                    # Also store the first column value as an additional field for debugging
                    if len(cell_contents) > 0 and cell_contents[0]:
                        property_data['additional_fields'] = {'first_column_value': cell_contents[0]}
                    
                    for idx, field in field_mapping.items():
                        if idx < len(cell_contents) and cell_contents[idx]:
                            property_data[field] = cell_contents[idx]
                    
                    # Ensure we have a property ID
                    if 'property_id' not in property_data or not property_data['property_id']:
                        logging.warning(f"Session {self.session_id}: Row {i} has no property ID, skipping")
                        continue
                    
                    # Create PropertyData object
                    try:
                        property_obj = PropertyData(**property_data)
                        properties.append(property_obj)
                        logging.info(f"Session {self.session_id}: Added property {property_obj.property_id}")
                    except Exception as model_error:
                        logging.error(f"Session {self.session_id}: Error creating PropertyData object: {str(model_error)}")
                        continue
                    
                except Exception as row_error:
                    logging.error(f"Session {self.session_id}: Error processing row {i}: {str(row_error)}")
                    continue
            
            logging.info(f"Session {self.session_id}: Extracted {len(properties)} properties from search results")
            
            # If we didn't find any properties, save a screenshot and HTML for debugging
            if not properties:
                logging.warning(f"Session {self.session_id}: No properties extracted from search results")
                await self.page.screenshot(path=f"search_results_no_properties_{self.session_id}.png")
                html_content = await self.page.content()
                with open(f"search_results_no_properties_{self.session_id}.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
            
            return properties
            
        except Exception as e:
            logging.error(f"Session {self.session_id}: Failed to extract properties from results: {str(e)}")
            logging.error(f"Session {self.session_id}: Exception traceback: {traceback.format_exc()}")
            # Take a screenshot of the error state
            try:
                await self.page.screenshot(path=f"search_results_error_{self.session_id}.png")
                html_content = await self.page.content()
                with open(f"search_results_error_{self.session_id}.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                logging.info(f"Session {self.session_id}: Saved error screenshot and HTML")
            except Exception as screenshot_error:
                logging.error(f"Session {self.session_id}: Error saving screenshot: {str(screenshot_error)}")
            return properties

    async def open_property_details(self, row_index: int) -> bool:
        """Navigate to property details page by clicking on the eye icon in search results"""
        try:
            # Construct the selector for the eye icon using the row index
            eye_icon_selector = f"#searchResultForm\\:propertySearchSRT\\:{row_index}\\:viewSelectedRowItem"
            self.logger.info(f"Session {self.session_id}: Opening property details for row {row_index}")
            self.logger.info(f"Session {self.session_id}: Looking for eye icon with selector: {eye_icon_selector}")
            
            # Check if the eye icon exists
            count = await self.page.locator(eye_icon_selector).count()
            if count == 0:
                # Try alternative selectors
                alt_selectors = [
                    f"#searchResultForm\\:propertySearchSRT\\:{row_index}\\:viewSelectedRowItem_link",
                    f"#searchResultForm\\:propertySearchSRT\\:{row_index}\\:viewSelectedRowItem span",
                    f"#searchResultForm\\:propertySearchSRT\\:{row_index}\\:viewSelectedRowItem i",
                    f"#searchResultForm\\:propertySearchSRT_data tr:nth-child({row_index+1}) td:last-child a",
                    f"#searchResultForm\\:propertySearchSRT_data tr:nth-child({row_index+1}) a[id*='viewSelectedRowItem']"
                ]
                
                for alt_selector in alt_selectors:
                    alt_count = await self.page.locator(alt_selector).count()
                    if alt_count > 0:
                        self.logger.info(f"Session {self.session_id}: Found eye icon with alternative selector: {alt_selector}")
                        eye_icon_selector = alt_selector
                        count = alt_count
                        break
                
                if count == 0:
                    # Last resort: try to find any clickable element in the last cell of the row
                    js_result = await self.page.evaluate(f"""() => {{
                        const row = document.querySelector('#searchResultForm\\\\:propertySearchSRT_data tr:nth-child({row_index+1})');
                        if (!row) return false;
                        
                        const lastCell = row.cells[row.cells.length - 1];
                        if (!lastCell) return false;
                        
                        const clickableElements = lastCell.querySelectorAll('a, button, [role="button"], .ui-clickable');
                        if (clickableElements.length > 0) {{
                            clickableElements[0].click();
                            return true;
                        }}
                        return false;
                    }}""")
                    
                    if js_result:
                        self.logger.info(f"Session {self.session_id}: Clicked eye icon using JavaScript")
                        await self.page.wait_for_timeout(2000)  # Wait for navigation
                        return True
                    
                    self.logger.error(f"Eye icon not found with any selector for row {row_index}")
                    return False
            
            # Take screenshot before clicking eye icon for debugging
            await self.page.screenshot(path=f"before_eye_click_session_{self.session_id}_row_{row_index}.png")
            # Click on the eye icon
            await self.page.locator(eye_icon_selector).click()
            self.logger.info(f"Clicked eye icon with selector: {eye_icon_selector}")
            # Take screenshot after clicking eye icon for debugging
            await self.page.screenshot(path=f"after_eye_click_session_{self.session_id}_row_{row_index}.png")
            await self.page.wait_for_timeout(2000)  # Wait for navigation
            return True
            
        except Exception as e:
            self.logger.exception(f"Session {self.session_id}: Error opening property details: {e}")
            return False

    async def extract_owner_data(self, page):
        """Extract owner details from the property owner table"""
        try:
            owner_table = page.locator('css=.owner-details-section')
            if not owner_table:
                logging.error("Owner section not found using selector '.owner-details-section'")
                return None
            
            logging.debug("Starting owner information extraction")
            
            # Add selector debugging
            owner_section = await page.query_selector('css=.owner-details-section')
            if not owner_section:
                logging.error("Owner section not found using selector '.owner-details-section'")
                return None
                
            logging.debug(f"Raw owner HTML:\n{await owner_section.inner_html()}")
            
            # Add field extraction debugging
            owner_data = {}
            fields = {
                'name': '.owner-name',
                'address': '.owner-address',
                'contact': '.contact-info',
                'email': '.owner-email',
                'mobile': '.owner-mobile',
                'phone': '.owner-phone',
                'isDecisionMaker': '.owner-decision-maker'
            }
            
            for field, selector in fields.items():
                element = await owner_section.query_selector(selector)
                if element:
                    value = await element.inner_text()
                    owner_data[field] = value.strip()
                    logging.debug(f"Extracted {field}: {owner_data[field]}")
                else:
                    logging.warning(f"Selector {selector} not found for {field}")
                    
            logging.debug(f"Parsed owner data: {json.dumps(owner_data, indent=2)}")
            return owner_data
            
        except Exception as e:
            logging.error(f"Owner extraction failed: {str(e)}", exc_info=True)
            return None

    async def process_property(self, page, property_id):
        # ... existing code ...
        owner_data = await self.extract_owner_data(page)
        if owner_data:
            logging.info(f"Session {self.session_id}: Extracted owner details - {owner_data}")
            # Add to your data storage
            self.property_data['owner'] = owner_data
        # ... rest of existing code ...

    async def get_property_details_with_owner(self, property_id: str) -> Optional[PropertyData]:
        """Get complete property details including owner information"""
        try:
            # First create a basic PropertyData object
            property_data = PropertyData(property_id=property_id)
            
            # Open property details page
            details_opened = await self.open_property_details(property_id)
            if not details_opened:
                logging.error(f"Session {self.session_id}: Failed to open property details for ID {property_id}")
                return None
            
            # Extract owner information
            owner_data = await self.extract_owner_data(self.page)
            if owner_data:
                property_data.owner_name = owner_data.get('name')
                property_data.owner_email = owner_data.get('email')
                property_data.owner_mobile = owner_data.get('mobile')
                property_data.owner_phone = owner_data.get('phone')
                property_data.is_decision_maker = owner_data.get('isDecisionMaker', False)
                property_data.owner_details_loaded = True
            
            return property_data
            
        except Exception as e:
            logging.error(f"Session {self.session_id}: Error getting property details with owner for ID {property_id}: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    async def paginate_and_extract_all(self):
        """Paginate through search results and extract properties with owner information from all pages."""
        all_properties = []
        page_num = 1
        
        while True:
            logging.info(f"Session {self.session_id}: Processing page {page_num}")
            
            # Extract properties from current page
            properties = await self.extract_properties_from_results()
            
            if properties:
                logging.info(f"Session {self.session_id}: Found {len(properties)} properties on page {page_num}")
                
                # Extract owner information for properties on this page
                properties_with_owners = await self.extract_owner_information_for_all_properties(properties)
                
                # Display the table with properties and owner information for current page
                owner_table = Table(title=f"Properties with Owner Information - Page {page_num} (Session {self.session_id})")
                owner_table.add_column("Property ID")
                owner_table.add_column("Owner Name")
                owner_table.add_column("Owner Email")
                owner_table.add_column("Owner Mobile")
                owner_table.add_column("Owner Phone")
                owner_table.add_column("Is Decision Maker")
                owner_table.add_column("Details Loaded")
                
                for prop in properties_with_owners:
                    owner_table.add_row(
                        prop.property_id,
                        prop.owner_name or "",
                        prop.owner_email or "",
                        prop.owner_mobile or "",
                        prop.owner_phone or "",
                        "✓" if prop.is_decision_maker else "✗",
                        "✓" if prop.owner_details_loaded else "✗"
                    )
                
                console.print(owner_table)
                
                # Add properties from this page to our collected list
                all_properties.extend(properties_with_owners)
                logging.info(f"Session {self.session_id}: Total properties collected so far: {len(all_properties)}")
            else:
                logging.info(f"Session {self.session_id}: No properties found on page {page_num}.")
            
            # Look for the next button that's not disabled
            next_button = await self.page.query_selector("a.ui-paginator-next:not(.ui-state-disabled)")
            if not next_button:
                logging.info(f"Session {self.session_id}: Next button not available or disabled. End of pagination.")
                break
            
            logging.info(f"Session {self.session_id}: Navigating to page {page_num + 1}.")
            await next_button.click()
            await self.page.wait_for_timeout(2000)  # Wait for page to load
            page_num += 1
        
        logging.info(f"Session {self.session_id}: Pagination complete. Extracted a total of {len(all_properties)} properties with owner information.")
        return all_properties
        
    async def extract_owner_information_for_all_properties(self, properties: List[PropertyData]) -> List[PropertyData]:
        """Extract owner information for all properties in the list"""
        properties_with_owners = []
        total_properties = len(properties)
        
        logging.info(f"Session {self.session_id}: Extracting owner information for {total_properties} properties")
        
        # Create a mapping of property IDs to their row indices
        property_id_to_row_index = {}
        
        # Get all rows in the table
        rows = await self.page.locator("#searchResultForm\\:propertySearchSRT_data tr").all()
        
        for row_index, row in enumerate(rows):
            try:
                # Get the property ID from column 26 (index 26)
                cells = await row.locator("td").all()
                if len(cells) > 26:
                    property_id_cell = cells[26]
                    property_id = await property_id_cell.text_content()
                    property_id = property_id.strip() if property_id else ""
                    if property_id:
                        property_id_to_row_index[property_id] = row_index
                        logging.info(f"Session {self.session_id}: Mapped property ID {property_id} to row index {row_index}")
            except Exception as e:
                logging.error(f"Session {self.session_id}: Error mapping property ID to row index: {str(e)}")
        
        for index, property_data in enumerate(properties):
            logging.info(f"Session {self.session_id}: Processing property {index+1}/{total_properties}: {property_data.property_id}")
            
            # Log additional fields if available
            if hasattr(property_data, 'additional_fields') and property_data.additional_fields:
                logging.info(f"Session {self.session_id}: Property additional fields: {property_data.additional_fields}")
            
            try:
                # Open property details page - use row index (0-based) for the selector
                # The eye icon selectors in the table are 0-based, not 1-based
                details_opened = await self.open_property_details(property_id_to_row_index.get(property_data.property_id, 0))

                if not details_opened:
                    logging.error(f"Session {self.session_id}: Failed to open details for property {property_data.property_id}")
                    properties_with_owners.append(property_data)  # Add the original property data without owner info
                    continue
                
                # Extract owner information
                owner_data = await self.extract_owner_data(self.page)
                if owner_data:
                    property_data.owner_name = owner_data.get('name')
                    property_data.owner_email = owner_data.get('email')
                    property_data.owner_mobile = owner_data.get('mobile')
                    property_data.owner_phone = owner_data.get('phone')
                    property_data.is_decision_maker = owner_data.get('isDecisionMaker', False)
                    property_data.owner_details_loaded = True
                properties_with_owners.append(property_data)
                
                # Go back to search results
                await self.page.go_back()
                
                # Add a small delay to avoid overwhelming the server
                await self.page.wait_for_timeout(1000)
                
            except Exception as e:
                logging.error(f"Session {self.session_id}: Error processing property {property_data.property_id}: {str(e)}")
                logging.error(traceback.format_exc())
                properties_with_owners.append(property_data)  # Add the original property data without owner info
        
        return properties_with_owners

    async def download_search_results_excel(self):
        """Download search results as Excel file"""
        try:
            logging.info(f"Session {self.session_id}: Downloading search results Excel file (always download in first session)")
            
            # Wait for the search results to be visible
            await self.page.wait_for_selector('#searchResultForm\\:propertySearchSRT', state='visible', timeout=30000)
            await asyncio.sleep(1)
            
            # Look for export/download buttons
            download_button = None
            
            # Try different selectors that might be used for the download button
            selectors = [
                '#searchResultForm\\:propertySearchSRT\\:exportPropertiesData',  # Specific export button
                'a.btn.btn-default.btn-ico:has(.fa-file-excel-o)',  # Button with Excel icon
                '#searchCriteriaForm\\:exportButton',
                'button[id*="export"]',
                'button[id*="download"]',
                'a[id*="export"]',
                'a[id*="download"]',
                'button:has-text("Export")',
                'button:has-text("Download")',
                'a:has-text("Export")',
                'a:has-text("Download")'
            ]
            
            for selector in selectors:
                try:
                    button = await self.page.query_selector(selector)
                    if button:
                        download_button = button
                        logging.info(f"Session {self.session_id}: Found download button with selector: {selector}")
                        break
                except Exception as e:
                    logging.debug(f"Session {self.session_id}: Error finding selector {selector}: {str(e)}")
                    continue
            
            if not download_button:
                # If we couldn't find the button with selectors, try to find it by evaluating JavaScript
                button_found = await self.page.evaluate("""() => {
                    const findButtonByText = (text) => {
                        const elements = Array.from(document.querySelectorAll('button, a'));
                        for (const el of elements) {
                            if (el.textContent && el.textContent.toLowerCase().includes(text.toLowerCase())) {
                                el.id = el.id || 'dynamic-found-button-' + Date.now();
                                return el.id;
                            }
                        }
                        return null;
                    };
                    
                    return findButtonByText('export') || findButtonByText('download') || findButtonByText('excel');
                }""")
                
                if button_found:
                    download_button = await self.page.query_selector(f"#{button_found}")
                    logging.info(f"Session {self.session_id}: Found download button with dynamic search: {button_found}")
            
            if not download_button:
                logging.error(f"Session {self.session_id}: Could not find download button")
                return None
            
            # Set up download event
            download_path = None
            async with self.page.expect_download() as download_info:
                await download_button.click()
                logging.info(f"Session {self.session_id}: Clicked download button")
                
                # Wait for the download to start
                download = await download_info.value
                
                # Get suggested filename
                suggested_filename = download.suggested_filename
                logging.info(f"Session {self.session_id}: Download started with suggested filename: {suggested_filename}")
                
                # Create a timestamped filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                if '.' in suggested_filename:
                    base, ext = suggested_filename.rsplit('.', 1)
                    new_filename = f"{base}_{timestamp}.{ext}"
                else:
                    new_filename = f"{suggested_filename}_{timestamp}.xlsx"
                
                # Save to the downloads directory
                download_path = self.download_dir / new_filename
                await download.save_as(download_path)
                logging.info(f"Session {self.session_id}: Download completed and saved to {download_path}")
            
            if not download_button:
                # Try to trigger the JSF export function directly
                try:
                    jsf_triggered = await self.page.evaluate("""() => {
                        if (typeof mojarra !== 'undefined' && typeof mojarra.jsfcljs === 'function') {
                            const form = document.getElementById('searchResultForm');
                            if (form) {
                                mojarra.jsfcljs(form, {
                                    'searchResultForm:propertySearchSRT:exportPropertiesData': 'searchResultForm:propertySearchSRT:exportPropertiesData'
                                }, '');
                                return true;
                            }
                        }
                        return false;
                    }""")
                    if jsf_triggered:
                        logging.info(f"Session {self.session_id}: Successfully triggered JSF export function")
                except Exception as e:
                    logging.error(f"Session {self.session_id}: Failed to trigger JSF export: {str(e)}")
            
            return str(download_path)
            
        except Exception as e:
            logging.error(f"Session {self.session_id}: Error downloading Excel file: {str(e)}")
            logging.error(traceback.format_exc())
            return None


    async def close(self):
        """Close the browser session"""
        try:
            if self.page:
                try:
                    await self.page.close()
                    self.page = None
                except Exception as e:
                    logging.error(f"Session {self.session_id}: Error closing page: {str(e)}")
            
            if self.context:
                try:
                    await self.context.close()
                    self.context = None
                except Exception as e:
                    logging.error(f"Session {self.session_id}: Error closing context: {str(e)}")
            
            if self.browser:
                try:
                    await self.browser.close()
                    self.browser = None
                except Exception as e:
                    logging.error(f"Session {self.session_id}: Error closing browser: {str(e)}")
            
            if self.playwright:
                try:
                    await self.playwright.stop()
                    self.playwright = None
                except Exception as e:
                    logging.error(f"Session {self.session_id}: Error stopping playwright: {str(e)}")
                    
            logging.info(f"Session {self.session_id}: Browser session closed")
        except Exception as e:
            logging.error(f"Session {self.session_id}: Error during close: {str(e)}")
            console.print(f"[red]Error closing browser: {str(e)}[/red]")
class IBTSessionPoolManager:
    """Manages multiple IBTPropertySearchSession instances for parallel processing"""
    def __init__(self, session_count=3, username=None, password=None, headless=True, otp_secret=None):
        """Initialize the session pool manager
        
        Args:
            session_count: Number of parallel sessions to create
            username: Telekom IBT username
            password: Telekom IBT password
            headless: Whether to run browsers in headless mode
            otp_secret: OTP secret for authentication
        """
        self.session_count = session_count
        self.username = username or os.getenv("TELEKOM_USERNAME")
        self.password = password or os.getenv("TELEKOM_PASSWORD")
        self.headless = headless
        self.otp_secret = otp_secret or os.getenv("TELEKOM_OTP_SECRET")
        
        self.sessions = []
        self.work_queue = asyncio.Queue()
        self.result_lock = asyncio.Lock()
        self.results = []
        self.logger = logging.getLogger("SessionPool")
        
        # Statistics and monitoring
        self.processed_count = 0
        self.error_count = 0
        self.start_time = None
        self.active_properties = {}  # Track which session is processing which property
    
    async def initialize_sessions(self):
        """Initialize all browser sessions and log in"""
        self.logger.info(f"Initializing {self.session_count} sessions")
        self.start_time = time.time()
        
        # Create and initialize all sessions
        for i in range(self.session_count):
            try:
                session = IBTPropertySearchSession(
                    username=self.username,
                    password=self.password,
                    session_id=i,
                    headless=self.headless
                )
                if self.otp_secret:
                    session.otp_secret = self.otp_secret
                
                self.logger.info(f"Initializing browser for session {i}")
                await session.init_browser()
                
                self.logger.info(f"Logging in with session {i}")
                login_success = await session.login()
                
                if login_success:
                    self.logger.info(f"Session {i} logged in successfully")
                    self.sessions.append(session)
                else:
                    self.logger.error(f"Failed to log in with session {i}")
            except Exception as e:
                self.logger.error(f"Error initializing session {i}: {str(e)}")
        
        self.logger.info(f"Successfully initialized {len(self.sessions)} sessions")
        return len(self.sessions) > 0
    
    async def distribute_work(self, properties):
        """Add all properties to the work queue for processing
        
        Args:
            properties: List of PropertyData objects to process
        """
        self.logger.info(f"Adding {len(properties)} properties to work queue")
        for prop in properties:
            await self.work_queue.put(prop)
    
    async def worker(self, session):
        """Worker function that processes properties from the queue
        
        Args:
            session: IBTPropertySearchSession instance to use for processing
        """
        session_id = session.session_id
        self.logger.info(f"Starting worker for session {session_id}")
        
        while not self.work_queue.empty():
            try:
                # Get next property from queue
                property_data = await self.work_queue.get()
                property_id = property_data.property_id
                
                # Track which session is working on which property
                self.active_properties[property_id] = session_id
                
                self.logger.info(f"Session {session_id} processing property {property_id}")
                
                # Get owner information for the property
                owner_data = await session.extract_owner_data(session.page)
                if owner_data:
                    property_data.owner_name = owner_data.get('name')
                    property_data.owner_email = owner_data.get('email')
                    property_data.owner_mobile = owner_data.get('mobile')
                    property_data.owner_phone = owner_data.get('phone')
                    property_data.is_decision_maker = owner_data.get('isDecisionMaker', False)
                    property_data.owner_details_loaded = True
                
                # Store the result with lock to avoid race conditions
                async with self.result_lock:
                    self.results.append(property_data)
                    self.processed_count += 1
                    
                    # Log progress every 10 properties
                    if self.processed_count % 10 == 0:
                        elapsed = time.time() - self.start_time
                        rate = self.processed_count / elapsed if elapsed > 0 else 0
                        self.logger.info(f"Processed {self.processed_count} properties. "
                                         f"Rate: {rate:.2f} properties/second")
                
                # Remove from active properties
                del self.active_properties[property_id]
                
                # Mark task as done
                self.work_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Error in session {session_id} processing property: {str(e)}")
                traceback.print_exc()
                
                # Increment error counter
                async with self.result_lock:
                    self.error_count += 1
                
                # Put property back in queue for retry if queue not too large
                # This ensures properties aren't lost due to session errors
                if self.work_queue.qsize() < 1000:  # Limit retries if too many errors
                    await self.work_queue.put(property_data)
                
                # Clear active property tracking
                if property_id in self.active_properties:
                    del self.active_properties[property_id]
                
                # Mark task as done
                self.work_queue.task_done()
                
                # Small delay to avoid hammering the server if there are persistent errors
                await asyncio.sleep(1)
    
    async def process_properties(self, properties):
        """Process all properties using parallel sessions
        
        Args:
            properties: List of PropertyData objects to process
            
        Returns:
            List of processed PropertyData objects with owner information
        """
        if not self.sessions:
            initialized = await self.initialize_sessions()
            if not initialized:
                self.logger.error("Failed to initialize any sessions")
                return []
        
        # Reset counters
        self.processed_count = 0
        self.error_count = 0
        self.results = []
        self.start_time = time.time()
        
        # Add all properties to work queue
        await self.distribute_work(properties)
        
        # Create worker tasks
        workers = [self.worker(session) for session in self.sessions]
        
        # Run all workers in parallel
        self.logger.info(f"Starting {len(workers)} worker tasks")
        await asyncio.gather(*workers)
        
        # Wait for queue to be completely processed
        await self.work_queue.join()
        
        # Log final statistics
        elapsed = time.time() - self.start_time
        self.logger.info(f"Completed processing {self.processed_count} properties in {elapsed:.2f} seconds")
        self.logger.info(f"Average rate: {self.processed_count / elapsed if elapsed > 0 else 0:.2f} properties/second")
        self.logger.info(f"Error count: {self.error_count}")
        
        return self.results
    
    async def search_and_process_area(self, area):
        """Search for properties in an area and process them all
        
        Args:
            area: Area to search for properties
            
        Returns:
            List of processed PropertyData objects with owner information
        """
        if not self.sessions:
            await self.initialize_sessions()
        
        # Use the first session to perform the search
        if not self.sessions:
            self.logger.error("No sessions available")
            return []
        
        search_session = self.sessions[0]
        
        # Perform search
        self.logger.info(f"Searching for area: {area}")
        search_success = await search_session.set_search_criteria(area)
        
        if not search_success:
            self.logger.error(f"Search failed for area: {area}")
            return []
        
        # Extract properties from search results
        properties = await search_session.extract_properties_from_results()
        self.logger.info(f"Found {len(properties)} properties in area {area}")
        
        if not properties:
            self.logger.warning(f"No properties found in area {area}")
            return []
        
        # Process all properties in parallel
        return await self.process_properties(properties)
    
    async def close(self):
        """Close all sessions"""
        for session in self.sessions:
            await session.close()
        self.sessions = []

# Helper function to map Excel data with extracted owner information
def merge_with_excel_data(properties_with_owners, excel_file_path):
    """Merge extracted owner information with Excel data
    
    Args:
        properties_with_owners: List of PropertyData objects with owner information
        excel_file_path: Path to Excel file downloaded from IBT
        
    Returns:
        DataFrame with merged data
    """
    import pandas as pd
    
    # Create a dictionary mapping property IDs to owner information
    owner_info_dict = {}
    for prop in properties_with_owners:
        owner_info_dict[prop.property_id] = {
            "owner_name": prop.owner_name,
            "owner_email": prop.owner_email,
            "owner_mobile": prop.owner_mobile,
            "owner_phone": prop.owner_phone,
            "is_decision_maker": prop.is_decision_maker
        }
    
    # Load Excel file
    df = pd.read_excel(excel_file_path)
    
    # Determine which column contains property IDs (may vary based on Excel structure)
    # Try common column names
    id_column = None
    for col in ["property_id", "Property ID", "ID", "FOL ID", "FOL-ID"]:
        if col in df.columns:
            id_column = col
            break
    
    if not id_column:
        # Try to find column by analyzing content
        for col in df.columns:
            if "ID" in col or "id" in col:
                id_column = col
                break
    
    if not id_column:
        raise ValueError("Could not determine property ID column in Excel file")
    
    # Add owner information columns
    df["Owner Name"] = df[id_column].map(lambda x: owner_info_dict.get(str(x), {}).get("owner_name"))
    df["Owner Email"] = df[id_column].map(lambda x: owner_info_dict.get(str(x), {}).get("owner_email"))
    df["Owner Mobile"] = df[id_column].map(lambda x: owner_info_dict.get(str(x), {}).get("owner_mobile"))
    df["Owner Phone"] = df[id_column].map(lambda x: owner_info_dict.get(str(x), {}).get("owner_phone"))
    df["Is Decision Maker"] = df[id_column].map(lambda x: owner_info_dict.get(str(x), {}).get("is_decision_maker"))
    
    # Add a column indicating whether owner information was found
    df["Owner Info Found"] = df[id_column].map(lambda x: str(x) in owner_info_dict)
    
    return df

def save_properties_to_file(properties: List[PropertyData], filename: str):
    """Save properties to a file in JSON format"""
    try:
        # Convert properties to dictionaries
        properties_data = []
        for prop in properties:
            prop_dict = {
                "property_id": prop.property_id,
                "address": prop.address,
                "postal_code": prop.postal_code,
                "city": prop.city,
                "status": prop.status,
                "owner_name": prop.owner_name,
                "owner_email": prop.owner_email,
                "owner_mobile": prop.owner_mobile,
                "owner_phone": prop.owner_phone,
                "is_decision_maker": prop.is_decision_maker,
                "owner_details_loaded": prop.owner_details_loaded,
                "additional_fields": prop.additional_fields
            }
            properties_data.append(prop_dict)
        
        # Save to file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(properties_data, f, indent=2)
        
        logging.info(f"Saved {len(properties)} properties to {filename}")
        return True
    except Exception as e:
        logging.error(f"Error saving properties to file: {str(e)}")
        logging.error(traceback.format_exc())
        return False

def save_to_csv(properties: List[PropertyData], filename: str):
    """Save property data to CSV file"""
    try:
        import csv
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Define CSV headers
            fieldnames = [
                'property_id', 'address', 'postal_code', 'city', 'status',
                'owner_name', 'owner_email', 'owner_mobile', 'owner_phone',
                'is_decision_maker', 'owner_details_loaded'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write property data
            for prop in properties:
                writer.writerow({
                    "property_id": prop.property_id,
                    "address": prop.address,
                    "postal_code": prop.postal_code,
                    "city": prop.city,
                    "status": prop.status,
                    "owner_name": prop.owner_name,
                    "owner_email": prop.owner_email,
                    "owner_mobile": prop.owner_mobile,
                    "owner_phone": prop.owner_phone,
                    "is_decision_maker": prop.is_decision_maker,
                    "owner_details_loaded": prop.owner_details_loaded
                })
                
        logging.info(f"Results saved to {filename}")
        return True
    except Exception as e:
        logging.error(f"Error saving results to CSV: {str(e)}")
        console.print(f"[bold red]Error saving results to CSV: {str(e)}[/bold red]")
        return False

class IBTPropertySearcher:
    """Main class for searching properties in the IBT portal"""
    def __init__(self, playwright, headless=False, session_id=0, otp_secret=None):
        load_dotenv()
        self.username = os.getenv("TELEKOM_USERNAME")
        self.password = os.getenv("TELEKOM_PASSWORD")
        self.session = None
        self.headless = headless
        self.playwright = playwright
        self.session_id = session_id
        self.otp_secret = otp_secret
        
    async def init(self) -> bool:
        """Initialize the property searcher"""
        try:
            self.session = IBTPropertySearchSession(
                username=self.username,
                password=self.password,
                session_id=self.session_id,
                headless=self.headless
            )
            if self.otp_secret is not None:
                self.session.otp_secret = self.otp_secret
            await self.session.init_browser()
            return True
        except Exception as e:
            console.print(f"[red]Error initializing session: {str(e)}[/red]")
            return False
    
    async def login(self) -> bool:
        """Login to the IBT portal"""
        if not self.session:
            console.print("[red]Session not initialized[/red]")
            return False
            
        return await self.session.login()
        
    async def navigate_to_property_search(self) -> bool:
        """Navigate to the property search page"""
        if not self.session:
            console.print("[red]Session not initialized[/red]")
            return False
            
        return await self.session.navigate_to_property_search()
        
    async def search_by_area(self, area) -> bool:
        """Search for properties by area"""
        if not self.session:
            console.print("[red]Session not initialized[/red]")
            return False
            
        return await self.session.search_by_area(area)

    async def get_property_details_with_owner(self, property_id: str) -> Optional[PropertyData]:
        """Get property details with owner information"""
        if not self.session:
            logging.error("Session not initialized")
            return None
            
        return await self.session.get_property_details_with_owner(property_id)

    async def extract_owner_information_for_all_properties(self, properties: List[PropertyData]) -> List[PropertyData]:
        """Extract owner information for all properties in the list"""
        if not self.session:
            logging.error("Session not initialized")
            return properties
            
        return await self.session.extract_owner_information_for_all_properties(properties)

    async def close(self):
        """Close the browser"""
        if self.session:
            await self.session.close()
            self.session = None

def keyboard_listener(exit_key):
    """Listen for keyboard input to trigger exit"""
    global exit_key_pressed
    
    console.print(f"[yellow]Keyboard listener started. Press '{exit_key}' to save events and exit.[/yellow]")
    
    while not shutdown_requested and not exit_key_pressed:
        try:
            # Read a single character from stdin without requiring Enter
            import tty
            import termios
            import sys
            
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setraw(sys.stdin.fileno())
                ch = sys.stdin.read(1)
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            
            if ch == exit_key:
                console.print(f"[yellow]Exit key '{exit_key}' pressed. Saving events and exiting...[/yellow]")
                exit_key_pressed = True
                # Also set save_events_before_exit to ensure events are saved
                global save_events_before_exit
                save_events_before_exit = True
        except Exception:
            # Just continue if there's an error
            pass
        
        # Small sleep to prevent CPU hogging
        time.sleep(0.1)

async def main():
    """Main entry point for the script with parallel session support"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Search for properties in the IBT portal with parallel sessions")
    parser.add_argument("--area", help="Area to search for properties")
    parser.add_argument("--property-id", help="Property ID to get details for")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument("--output", help="Output file for search results")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--quiet", action="store_true", help="Suppress non-essential output")
    parser.add_argument("--session-count", type=int, default=3, help="Number of parallel sessions to use")
    parser.add_argument("--otp-secret", type=str, help="OTP secret for authentication (otpauth:// URL)")
    parser.add_argument("--excel-file", help="Path to Excel file to merge results with")
    args = parser.parse_args()

    # Setup logging
    setup_logging(debug=args.debug, quiet=args.quiet)
    
    # Create session pool manager
    pool_manager = IBTSessionPoolManager(
        session_count=args.session_count,
        headless=args.headless,
        otp_secret=args.otp_secret
    )
    
    try:
        # Initialize sessions
        initialized = await pool_manager.initialize_sessions()
        if not initialized:
            console.print("[red]Failed to initialize any sessions. Exiting...[/red]")
            return
        
        console.print(f"[green]Successfully initialized {len(pool_manager.sessions)} sessions![/green]")
        
        # Process based on command line arguments
        if args.area:
            console.print(f"[blue]Searching for area: {args.area}[/blue]")
            
            # Use the search_and_process_area method that handles both search and processing
            properties_with_owners = await pool_manager.search_and_process_area(args.area)
            
            if not properties_with_owners:
                console.print("[red]No properties found or processing failed[/red]")
            else:
                console.print(f"[green]Successfully processed {len(properties_with_owners)} properties with owner information[/green]")
                
                # Display results in table
                display_properties_table(properties_with_owners)
                
                # Save raw properties to file if output is specified
                if args.output:
                    output_path = args.output
                    if not output_path.endswith('.json') and not output_path.endswith('.csv'):
                        output_path += '.csv'  # Default to CSV
                    
                    if output_path.endswith('.json'):
                        save_properties_to_file(properties_with_owners, output_path)
                    else:
                        save_to_csv(properties_with_owners, output_path)
                    
                    console.print(f"[green]Properties saved to {output_path}[/green]")
                
                # Merge with Excel file if specified
                if args.excel_file and os.path.exists(args.excel_file):
                    try:
                        merged_df = merge_with_excel_data(properties_with_owners, args.excel_file)
                        
                        # Save merged data to Excel
                        output_excel = args.output.replace('.csv', '').replace('.json', '') + '_merged.xlsx'
                        merged_df.to_excel(output_excel, index=False)
                        console.print(f"[green]Merged data saved to {output_excel}[/green]")
                        
                    except Exception as e:
                        console.print(f"[red]Error merging with Excel file: {str(e)}[/red]")
                        traceback.print_exc()
        
        elif args.property_id:
            console.print(f"[blue]Getting owner information for property: {args.property_id}[/blue]")
            
            # For single property, use the first session
            if not pool_manager.sessions:
                console.print("[red]No sessions available[/red]")
                return
                
            session = pool_manager.sessions[0]
            
            # Create basic PropertyData object
            property_data = PropertyData(property_id=args.property_id)
            
            # Get owner information
            owner_data = await session.extract_owner_data(session.page)
            if owner_data:
                property_data.owner_name = owner_data.get('name')
                property_data.owner_email = owner_data.get('email')
                property_data.owner_mobile = owner_data.get('mobile')
                property_data.owner_phone = owner_data.get('phone')
                property_data.is_decision_maker = owner_data.get('isDecisionMaker', False)
                property_data.owner_details_loaded = True
            
            if property_data.owner_details_loaded:
                # Display results
                display_owner_information(property_data)
                
                # Save to output file if specified
                if args.output:
                    if args.output.endswith('.json'):
                        save_properties_to_file([property_data], args.output)
                    else:
                        save_to_csv([property_data], args.output)
                    console.print(f"[green]Property saved to {args.output}[/green]")
            else:
                console.print("[red]Failed to get owner information for property[/red]")
        
        else:
            console.print("[yellow]No area or property ID specified. Please enter search criteria manually or use --area/--property-id options.[/yellow]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        traceback.print_exc()
    
    finally:
        # Close sessions
        console.print("[yellow]Closing all sessions...[/yellow]")
        await pool_manager.close()
        console.print("[green]All sessions closed[/green]")

def display_properties_table(properties):
    """Display properties in a nicely formatted table"""
    owner_table = Table(title="Properties with Owner Information")
    owner_table.add_column("Property ID")
    owner_table.add_column("Address")
    owner_table.add_column("City")
    owner_table.add_column("Owner Name")
    owner_table.add_column("Owner Email")
    owner_table.add_column("Owner Mobile")
    owner_table.add_column("Is Decision Maker")
    
    for prop in properties:
        owner_table.add_row(
            prop.property_id,
            prop.address or "",
            prop.city or "",
            prop.owner_name or "",
            prop.owner_email or "",
            prop.owner_mobile or "",
            "✓" if prop.is_decision_maker else "✗"
        )
    
    console.print(owner_table)

def display_owner_information(property_data):
    """Display detailed owner information for a single property"""
    console.print("\n[bold green]Owner Information:[/bold green]")
    owner_table = Table(show_header=True, header_style="bold cyan")
    owner_table.add_column("Property ID")
    owner_table.add_column("Owner Name")
    owner_table.add_column("Owner Email")
    owner_table.add_column("Owner Mobile")
    owner_table.add_column("Owner Phone")
    owner_table.add_column("Is Decision Maker")
    
    owner_table.add_row(
        property_data.property_id,
        property_data.owner_name or "",
        property_data.owner_email or "",
        property_data.owner_mobile or "",
        property_data.owner_phone or "",
        "✓" if property_data.is_decision_maker else "✗"
    )
    
    console.print(owner_table)
    
    # Display additional fields if available
    if hasattr(property_data, 'additional_fields') and property_data.additional_fields:
        if 'owner_tab_content' in property_data.additional_fields:
            console.print("\n[bold yellow]Raw Owner Tab Content:[/bold yellow]")
            console.print(property_data.additional_fields['owner_tab_content'])
            
# Global variable for signal handler
searcher = None

if __name__ == "__main__":
    asyncio.run(main())
