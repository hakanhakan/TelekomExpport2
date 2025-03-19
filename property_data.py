#!/usr/bin/env python3
import asyncio
import logging
import os
import time
import urllib.parse
import hmac
import hashlib
import json
import struct
from tabulate import tabulate
import aiosqlite
from rich.console import Console
import pyotp
from playwright.async_api import async_playwright, Browser, Page
from typing import Optional
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from pathlib import Path

load_dotenv()

console = Console()

# Add new function calculate_hash after imports
def calculate_hash(property_data):
    """
    Calculate a SHA256 hash for the given property data.
    property_data: tuple or list of values representing key fields, for example:
                  [fol_id, street, house_number, house_appendix, owner_name, owner_email, owner_mobile, owner_landline, status]
    """
    # Convert data to JSON string with sorted keys for consistent ordering.
    data_str = json.dumps(property_data, sort_keys=True)
    return hashlib.sha256(data_str.encode('utf-8')).hexdigest()

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

# --------------------------------------------------
# Robust OTP Input Helper and Custom TOTP Class
# --------------------------------------------------
class CustomTOTP(pyotp.TOTP):
    """Custom TOTP implementation that supports SHA512."""
    def __init__(self, s, digits=6, digest='sha1', interval=30):
        self.secret = s
        self.digits = digits
        self.digest = digest.lower()  # supports sha1, sha256, sha512
        self.interval = interval

    def generate_otp(self, counter):
        if self.digest == 'sha1':
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', counter), hashlib.sha1)
        elif self.digest == 'sha256':
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', counter), hashlib.sha256)
        elif self.digest == 'sha512':
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', counter), hashlib.sha512)
        else:
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', counter), hashlib.sha1)
        hmac_hash = bytearray(hasher.digest())
        offset = hmac_hash[-1] & 0x0F
        code = ((hmac_hash[offset] & 0x7F) << 24 |
                (hmac_hash[offset + 1] & 0xFF) << 16 |
                (hmac_hash[offset + 2] & 0xFF) << 8 |
                (hmac_hash[offset + 3] & 0xFF))
        return code % (10 ** self.digits)

    def now(self):
        counter = int(time.time()) // self.interval
        code = self.generate_otp(counter)
        return str(code).zfill(self.digits)


async def robust_otp_input(page, otp_secret, otp_input_selector, otp_submit_selector, max_retries=3):
    """
    Waits for the OTP input field, clears it, fills in the OTP code (generated from otp_secret),
    verifies its value, and then clicks the submit button.
    """
    def generate_otp_code(secret):
        # Process otpauth:// URLs
        if secret.startswith("otpauth://"):
            parsed_url = urllib.parse.urlparse(secret)
            query_params = dict(urllib.parse.parse_qsl(parsed_url.query))
            secret_value = query_params.get('secret')
            algorithm = query_params.get('algorithm', 'SHA1')
            digits = int(query_params.get('digits', 6))
            period = int(query_params.get('period', 30))
            if algorithm.upper() == 'SHA512':
                totp = CustomTOTP(secret_value, digits=digits, digest='sha512', interval=period)
            else:
                totp = pyotp.TOTP(secret_value, digits=digits, digest=algorithm.lower(), interval=period)
            return totp.now()
        else:
            totp = pyotp.TOTP(secret)
            return totp.now()

    otp_code = generate_otp_code(otp_secret)
    for attempt in range(max_retries):
        try:
            otp_field = await page.wait_for_selector(otp_input_selector, timeout=10000)
            await otp_field.fill("")
            await otp_field.type(otp_code, delay=50)
            await asyncio.sleep(0.5)
            current_value = await otp_field.input_value()
            if current_value.strip() == otp_code:
                logging.info(f"OTP correctly filled with {otp_code} on attempt {attempt + 1}")
                break
            else:
                logging.warning(f"Attempt {attempt + 1}: Field value '{current_value}' does not match '{otp_code}'")
        except Exception as e:
            logging.warning(f"OTP fill attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(1)
    else:
        logging.error("Failed to fill OTP input field after maximum retries.")
        return False

    try:
        submit_button = await page.wait_for_selector(otp_submit_selector, timeout=5000)
        await submit_button.click()
        logging.info("Clicked OTP submit button.")
    except Exception as e:
        logging.error(f"OTP submit button not found: {e}")
        return False

    return True

# --------------------------------------------------
# IBT Property Search Session Base Class
# --------------------------------------------------
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
                                logging.info(f"Session {self.session_id}: Using OTP secret")
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
        
        logging.info(f"Session {self.session_id}: Attempting to generate OTP code from secret")
        
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
                
                logging.info(f"Session {self.session_id}: Extracted secret")
                
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
                logging.info(f"Session {self.session_id}: Extracted secret")
                
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

# --------------------------------------------------
# Subclass to Override Login for Robust OTP Handling
# --------------------------------------------------
class RobustIBTPropertySearchSession(IBTPropertySearchSession):
    async def login(self) -> bool:
        """Override login method to use robust OTP input."""
        try:
            await self.page.goto(self.login_url)
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

                anmelden_button = await self.page.wait_for_selector('input#kc-login[value="Anmelden"]', timeout=5000)
                if anmelden_button:
                    logging.info(f"Session {self.session_id}: Clicking 'Anmelden' button")
                    await anmelden_button.click()
                else:
                    logging.info(f"Session {self.session_id}: 'Anmelden' button not found, using generic submit")
                    await self.page.click('button[type="submit"]')

                await self.page.wait_for_timeout(5000)
                current_url = self.page.url

                if "authenticate" in current_url:
                    console.print(f"[yellow]OTP required for session {self.session_id}.[/yellow]")
                    try:
                        otp_radio_button = await self.page.wait_for_selector('input#kc-otp-credential-2[type="radio"]', timeout=5000)
                        if otp_radio_button:
                            logging.info(f"Session {self.session_id}: Clicking OTP radio button")
                            await otp_radio_button.click()
                            await self.page.wait_for_timeout(1000)
                    except Exception as e:
                        logging.warning(f"Session {self.session_id}: OTP radio button error: {e}")

                    # Re-read OTP secret for this session to generate a fresh OTP code.
                    self.otp_secret = os.getenv("TELEKOM_OTP_SECRET")
                    if self.otp_secret:
                        logging.info(f"Session {self.session_id}: Using OTP secret.")
                        success = await robust_otp_input(
                            self.page,
                            self.otp_secret,
                            otp_input_selector='input#otp[name="otp"]',
                            otp_submit_selector='input[type="submit"]'
                        )
                        if not success:
                            return False
                    else:
                        console.print("[yellow]No OTP secret provided. Please enter OTP manually in the browser.[/yellow]")

                    console.print("[yellow]Waiting for OTP verification to complete...[/yellow]")
                    # Wait for OTP verification (retrying if needed)
                    for _ in range(12):
                        if "authenticate" not in self.page.url:
                            logging.info(f"Session {self.session_id}: OTP verification completed")
                            break
                        await self.page.wait_for_timeout(1000)
                    else:
                        logging.warning(f"Session {self.session_id}: OTP verification did not complete after first try.")

                    # If still on authentication page, retry a few times.
                    max_otp_attempts = 3
                    attempts = 0
                    while "authenticate" in self.page.url and attempts < max_otp_attempts:
                        logging.info(f"Session {self.session_id}: OTP verification failed, retrying new OTP attempt {attempts + 1}")
                        await self.page.wait_for_timeout(2000)
                        # Re-read OTP secret here as well if you expect it might change
                        self.otp_secret = os.getenv("TELEKOM_OTP_SECRET")
                        success = await robust_otp_input(
                            self.page,
                            self.otp_secret,
                            otp_input_selector='input#otp[name="otp"]',
                            otp_submit_selector='input[type="submit"]'
                        )
                        if not success:
                            logging.error(f"Session {self.session_id}: Failed to re-fill OTP on attempt {attempts + 1}")
                            return False
                        await self.page.wait_for_timeout(5000)
                        attempts += 1

                    if "authenticate" in self.page.url:
                        logging.error(f"Session {self.session_id}: OTP verification ultimately failed after {attempts} attempts")
                        return False
                else:
                    logging.info(f"Session {self.session_id}: No OTP required")
            await self.page.wait_for_timeout(5000)
            current_url = self.page.url
            logging.info(f"Session {self.session_id}: Current URL after login: {current_url}")

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
            logging.error(f"Login failed for session {self.session_id}: {e}")
            return False

# --------------------------------------------------
# Ownership Extraction
# --------------------------------------------------
async def extract_ownership(page):
    """
    Extract owner data from the Owner tab.
    Returns the decision maker's data or None if not found.
    """
    try:
        await page.wait_for_selector("#processPageForm\\:propertyTabView\\:propertyOwnerTable_data", timeout=10000)
    except Exception as e:
        logging.warning(f"Owner table did not appear: {e}")
        return None  # Return None if the table isn’t found

    owner_rows = await page.query_selector_all("#processPageForm\\:propertyTabView\\:propertyOwnerTable_data tr")
    decision_owner = None

    for row in owner_rows:
        decision_elem = await row.query_selector("td:last-child span.fa-check[title='Decision Maker']")
        if decision_elem:
            tds = await row.query_selector_all("td")
            if len(tds) >= 4:
                name_span = await tds[0].query_selector("span")
                email_span = await tds[1].query_selector("span")
                mobile_span = await tds[2].query_selector("span")
                landline_span = await tds[3].query_selector("span")

                name = (await name_span.inner_text()).strip() if name_span else ""
                email = (await email_span.inner_text()).strip() if email_span else ""
                mobile = (await mobile_span.inner_text()).strip() if mobile_span else ""
                landline = (await landline_span.inner_text()).strip() if landline_span else ""
                decision_owner = [name, email, mobile, landline]
            break
    return decision_owner

# --------------------------------------------------
# Property-Level Extraction
# --------------------------------------------------
async def process_property(session, ri):
    # Define the selector for the eye icon.
    eye_selector = f"xpath=//tr[@data-ri='{ri}']//a[contains(@id, 'viewSelectedRowItem')]"
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Click the eye icon.
            eye_link = await session.page.wait_for_selector(eye_selector, timeout=5000)
            await eye_link.click()
            logging.info(f"[Session {session.session_id}] Clicked eye icon for data-ri {ri} (Attempt {attempt + 1})")
            # Wait a short time to allow the detail view to open.
            await session.page.wait_for_timeout(2000)
            # Wait for the property detail view to appear.
            await session.page.wait_for_selector("#processPageForm\\:propertyTabView", timeout=15000)
            logging.info(f"[Session {session.session_id}] Property tab view appeared for data-ri {ri} on attempt {attempt + 1}")
            break  # Exit loop if successful.
        except Exception as e:
            logging.warning(f"[Session {session.session_id}] Attempt {attempt + 1} to open detail page for data-ri {ri} failed: {e}")
            if attempt == max_retries - 1:
                msg = f"Property tab view still did not appear for data-ri {ri} after {max_retries} attempts: {e}"
                logging.error(f"[Session {session.session_id}] {msg}")
                return None, msg  # Return error status after final attempt.
            else:
                logging.info(f"[Session {session.session_id}] Refreshing page before retrying attempt {attempt + 2}")
                await session.page.reload()
                await asyncio.sleep(3)
    
    # Continue with the rest of the process if the detail view is successfully opened.
    owner_tab_selector = "xpath=//*[@id='processPageForm:propertyTabView']/ul/li[4]/a"
    try:
        owner_tab = await session.page.wait_for_selector(owner_tab_selector, timeout=5000)
        await owner_tab.click()
        logging.info(f"[Session {session.session_id}] Clicked on Owner tab (data-ri {ri})")
    except Exception as e:
        msg = f"Owner tab not found for data-ri {ri}: {e}"
        logging.error(f"[Session {session.session_id}] {msg}")
        return None, msg

    try:
        await session.page.wait_for_selector("#processPageForm\\:propertyTabView\\:propertyOwnerTable_data", timeout=10000)
        owner_data = await extract_ownership(session.page)
        status_msg = ""
    except Exception as e:
        msg = f"Owner table not found for data-ri {ri}: {e}"
        logging.warning(f"[Session {session.session_id}] {msg}")
        owner_data = None
        status_msg = msg

    # --- NEW: Extract Exploration Data ---
    exploration_date = ""
    exploration_pdf_ref = ""
    try:
        exploration_button = await session.page.query_selector("#processPageForm\\:explorationProtocol")
        if exploration_button:
            # Check if the button is enabled.
            disabled_attr = await exploration_button.get_attribute("disabled")
            aria_disabled = await exploration_button.get_attribute("aria-disabled")
            if disabled_attr or (aria_disabled and aria_disabled.lower() == "true"):
                logging.info(f"[Session {session.session_id}] Exploration protocol button is disabled. Skipping download.")
            else:
                logging.info(f"[Session {session.session_id}] Exploration protocol button found and enabled.")
                download_future = session.page.wait_for_event("download", timeout=10000)
                await exploration_button.click()
                download = await download_future
                pdf_filename = download.suggested_filename
                exploration_folder = Path("exploration_protocols")
                exploration_folder.mkdir(exist_ok=True)
                destination_path = exploration_folder / pdf_filename
                await download.save_as(str(destination_path))
                exploration_pdf_ref = str(destination_path)
        else:
            logging.info(f"[Session {session.session_id}] Exploration protocol button not found.")
    except Exception as e:
        logging.warning(f"[Session {session.session_id}] Error handling exploration protocol: {e}")

    try:
        exploration_date_elem = await session.page.query_selector("#processPageForm\\:explorationAgreementDate")
        if exploration_date_elem:
            exploration_date = (await exploration_date_elem.inner_text()).strip()
            logging.info(f"[Session {session.session_id}] Extracted exploration date: {exploration_date}")
    except Exception as e:
        logging.warning(f"[Session {session.session_id}] Exploration agreement date not found: {e}")
    # --- END NEW SECTION ---

    # Close the detail page.
    close_selector = "#processPageForm\\:j_idt340 span"
    try:
        close_button = await session.page.wait_for_selector(close_selector, timeout=5000)
    except Exception:
        close_selector = "xpath=//*[@id='processPageForm:j_idt341']/span"
        close_button = await session.page.wait_for_selector(close_selector, timeout=5000)
    await close_button.click()
    logging.info(f"[Session {session.session_id}] Closed detail page (data-ri {ri})")
    await session.page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    return owner_data, status_msg, exploration_date, exploration_pdf_ref

# --------------------------------------------------
# Page Extraction
# --------------------------------------------------
async def extract_search_results(session):
    """
    Extract static data from the current page's table rows.
    For each row, retrieve its "data-ri" attribute and basic property details,
    then call process_property for that specific row.
    Returns a list of rows:
    [FoL-ID, Street, House number, House number Appendix, Owner Name, Owner Email, Owner Mobile, Owner Landline, Status].
    """
    try:
        await session.page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    except Exception as e:
        logging.error(f"[Session {session.session_id}] Search results table not found: {e}")
        return []

    rows = await session.page.query_selector_all("#searchResultForm\\:propertySearchSRT_data tr")
    extracted_data = []
    row_data_cache = []

    for row in rows:
        ri = await row.get_attribute("data-ri")
        fol_elem = await row.query_selector("span[title='FoL-Id']")
        street_elem = await row.query_selector("span[title='Street']")
        house_elem = await row.query_selector("span[title='House number']")
        appendix_elem = await row.query_selector("span[title='House number Appndix']")

        fol_id = (await fol_elem.inner_text()).strip() if fol_elem else ""
        street = (await street_elem.inner_text()).strip() if street_elem else ""
        house_number = (await house_elem.inner_text()).strip() if house_elem else ""
        house_appendix = (await appendix_elem.inner_text()).strip() if appendix_elem else ""
        row_data_cache.append((ri, fol_id, street, house_number, house_appendix))

    for ri, fol_id, street, house_number, house_appendix in row_data_cache:
        # process_property now returns a tuple: (owner_info, status_msg, exploration_date, exploration_pdf_ref)
        owner_info, status_msg, exploration_date, exploration_pdf_ref = await process_property(session, ri)
        if owner_info:
            combined = [fol_id, street, house_number, house_appendix] + owner_info + [status_msg, exploration_date, exploration_pdf_ref]
        else:
            combined = [fol_id, street, house_number, house_appendix, "", "", "", "", status_msg, "", ""]
        extracted_data.append(combined)

    return extracted_data

# --------------------------------------------------
# Database Functions
# --------------------------------------------------
async def save_page_data_to_db(session_id, page_number, data):
    async with aiosqlite.connect("extraction.db") as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS property_data (
                fol_id TEXT PRIMARY KEY,
                session_id INTEGER,
                page INTEGER,
                street TEXT,
                house_number TEXT,
                house_appendix TEXT,
                owner_name TEXT,
                owner_email TEXT,
                owner_mobile TEXT,
                owner_landline TEXT,
                status TEXT,
                exploration TEXT,
                exploration_pdf TEXT,
                data_hash TEXT,
                changed_flag INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

        # Check existing schema and add missing columns if necessary.
        async with db.execute("PRAGMA table_info(property_data)") as cursor:
            columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]
        if "exploration" not in column_names:
            await db.execute("ALTER TABLE property_data ADD COLUMN exploration TEXT")
        if "exploration_pdf" not in column_names:
            await db.execute("ALTER TABLE property_data ADD COLUMN exploration_pdf TEXT")
        await db.commit()

        for row in data:
            new_hash = calculate_hash(row)
            await db.execute("""
                INSERT INTO property_data 
                (fol_id, session_id, page, street, house_number, house_appendix, owner_name, owner_email, owner_mobile, owner_landline, status, exploration, exploration_pdf, data_hash, changed_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(fol_id) DO UPDATE SET
                    street = excluded.street,
                    house_number = excluded.house_number,
                    house_appendix = excluded.house_appendix,
                    owner_name = excluded.owner_name,
                    owner_email = excluded.owner_email,
                    owner_mobile = excluded.owner_mobile,
                    owner_landline = excluded.owner_landline,
                    status = excluded.status,
                    exploration = excluded.exploration,
                    exploration_pdf = excluded.exploration_pdf,
                    data_hash = CASE WHEN property_data.data_hash <> excluded.data_hash THEN excluded.data_hash ELSE property_data.data_hash END,
                    changed_flag = CASE WHEN property_data.data_hash <> excluded.data_hash THEN 1 ELSE 0 END,
                    last_updated = CURRENT_TIMESTAMP
            """, (row[0], session_id, page_number, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], new_hash))
        await db.commit()

# --------------------------------------------------
# Page Navigation Helpers
# --------------------------------------------------
async def click_next_page(session):
    next_selector = "#searchResultForm\\:propertySearchSRT_paginator_top > a.ui-paginator-next > span"
    try:
        next_button = await session.page.wait_for_selector(next_selector, timeout=5000)
        await next_button.click()
        logging.info(f"[Session {session.session_id}] Clicked next button.")
        await asyncio.sleep(3)
        await session.page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    except Exception as e:
        logging.error(f"[Session {session.session_id}] Failed to click next page: {e}")
        raise

async def go_to_page_by_clicking_number(session, page_number):
    try:
        link_selector = f"xpath=//*[@id='searchResultForm:propertySearchSRT_paginator_top']/span[1]/a[text()='{page_number}']"
        page_link = await session.page.wait_for_selector(link_selector, timeout=5000)
        await page_link.click()
        logging.info(f"[Session {session.session_id}] Clicked page label {page_number}.")
        await asyncio.sleep(3)
        await session.page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    except Exception as e:
        logging.warning(f"[Session {session.session_id}] Could not directly click page {page_number}: {e}")
        current_page = 1
        while current_page < page_number:
            await click_next_page(session)
            current_page += 1

# --------------------------------------------------
# Range Processing
# --------------------------------------------------
async def process_page_range(session, start_page, end_page):
    if start_page > 1:
        await go_to_page_by_clicking_number(session, start_page)

    for page_number in range(start_page, end_page + 1):
        logging.info(f"[Session {session.session_id}] Extracting data from page {page_number}")
        page_data = await extract_search_results(session)
        await save_page_data_to_db(session.session_id, page_number, page_data)
        logging.info(f"[Session {session.session_id}] Saved data for page {page_number}")

        if page_number < end_page:
            await click_next_page(session)

# --------------------------------------------------
# Main & Multi-Session
# --------------------------------------------------
async def main():
    setup_logging(debug=True)

    total_pages = 51     # Adjust total pages for your test.
    num_sessions = 1    # Number of concurrent sessions.
    pages_per_session = total_pages // num_sessions

    sessions = []
    # Create and log in each session using our robust subclass.
    for i in range(num_sessions):
        s = RobustIBTPropertySearchSession(
            username=os.getenv("TELEKOM_USERNAME"),
            password=os.getenv("TELEKOM_PASSWORD"),
            session_id=i,
            headless=False  # Set to True for headless mode.
        )
        s.otp_secret = os.getenv("TELEKOM_OTP_SECRET")
        await s.init_browser()
        if not await s.login():
            logging.error(f"Session {i} login failed.")
            continue
        logging.info(f"Successfully logged in session {i}")
        sessions.append(s)

    if not sessions:
        logging.error("No sessions available. Exiting.")
        return

    # Set the same search criteria for all sessions.
    for s in sessions:
        area = "Bad Sooden-Allendorf, Stadt"
        logging.info(f"[Session {s.session_id}] Setting search criteria for area: {area}")
        try:
            area_input = await s.page.wait_for_selector("[id='searchCriteriaForm:vvmArea_input']", timeout=10000)
            await area_input.click()
            await area_input.fill("")
            await area_input.type(area, delay=50)
            await area_input.dispatch_event("input")
            suggestion_panel_selector = "#searchCriteriaForm\\:vvmArea_panel"
            await s.page.wait_for_selector(suggestion_panel_selector, timeout=5000)
            suggestion = await s.page.wait_for_selector(f"{suggestion_panel_selector} li", timeout=5000)
            await suggestion.click()
        except Exception as e:
            logging.error(f"[Session {s.session_id}] Failed area input: {e}")

        # Set number of results.
        try:
            dropdown = await s.page.wait_for_selector("xpath=//*[@id='searchCriteriaForm:nrOfResults']/div[3]/span", timeout=5000)
            await dropdown.click()
            option = await s.page.wait_for_selector("#searchCriteriaForm\\:nrOfResults_6", timeout=5000)
            await option.click()
            logging.info(f"[Session {s.session_id}] Set number of results to 2500 (option index 6).")
        except Exception as e:
            logging.error(f"[Session {s.session_id}] Failed to set number of results: {e}")

        # Click the search button.
        try:
            search_btn = await s.page.wait_for_selector("#searchCriteriaForm\\:searchButton", timeout=10000)
            await search_btn.click()
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"[Session {s.session_id}] Could not click search: {e}")

    # Distribute page ranges among sessions.
    tasks = []
    for i, s in enumerate(sessions):
        start_page = i * pages_per_session + 1
        if i == num_sessions - 1:
            end_page = total_pages
        else:
            end_page = (i + 1) * pages_per_session

        logging.info(f"[Session {s.session_id}] Assigned pages {start_page} to {end_page}")
        tasks.append(process_page_range(s, start_page, end_page))

    await asyncio.gather(*tasks)

    # (Optional) Print combined results from the SQLite database.
    async with aiosqlite.connect("extraction.db") as db:
        async with db.execute("SELECT * FROM property_data") as cursor:
            all_rows = await cursor.fetchall()
            headers = [
                "session_id", "page", "fol_id", "street", "house_number",
                "house_appendix", "owner_name", "owner_email", "owner_mobile", "owner_landline"
            ]
            print(tabulate(all_rows, headers=headers, tablefmt="pretty"))

    # Close all sessions.
    for s in sessions:
        await s.close()
    logging.info("All sessions closed.")

if __name__ == "__main__":
    asyncio.run(main())
