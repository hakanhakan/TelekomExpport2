#!/usr/bin/env python3
"""
Telekom Order Export Automation Tool
This script automates the extraction of order details from the Telekom supplier portal.
"""

import asyncio
import argparse
import csv
import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from playwright.async_api import async_playwright, Browser, Page
from rich.console import Console
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telekom_export.log'),
        logging.StreamHandler()
    ]
)

# Initialize Rich console
console = Console()

class OrderData(BaseModel):
    """Data model for order information"""
    # Standard fields
    external_order_id: Optional[str] = None
    order_id: Optional[str] = None
    order_status: Optional[str] = None
    order_type: Optional[str] = None
    customer_name: Optional[str] = None
    customer_type: Optional[str] = None
    customer_order_reference: Optional[str] = None
    street: Optional[str] = None
    house_number: Optional[str] = None
    postal_code: Optional[str] = None
    city: Optional[str] = None
    appointment_status: Optional[str] = None
    installation_due_date: Optional[str] = None
    kls_id: Optional[str] = None
    fol_id: Optional[str] = None
    building_type: Optional[str] = None
    accommodation_units: Optional[str] = None
    build_up_agreement: Optional[str] = None
    construction_type: Optional[str] = None
    project_id: Optional[str] = None
    customer_email: Optional[str] = None
    customer_phone: Optional[str] = None
    customer_mobile: Optional[str] = None
    carrier_name: str = "Deutsche Telekom AG"
    # Dynamic fields dictionary to store additional fields
    additional_fields: dict = {}

class BrowserSession:
    """Handles individual browser session and its state"""
    def __init__(self, username: str, password: str, session_id: int, headless=False, recording_mode=False):
        self.username = username
        self.password = password
        self.session_id = session_id
        self.headless = headless
        self.recording_mode = recording_mode
        self.base_url = "https://glasfaser.telekom.de/auftragnehmerportal-ui"
        self.login_url = f"{self.base_url}/order/ibtorder/search?a-cid=58222"
        self.search_url = f"{self.base_url}/property/search"
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.download_dir = Path("downloads")
        self.download_dir.mkdir(exist_ok=True)
        
    async def init_browser(self):
        """Initialize browser instance"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=self.headless)
        self.page = await self.browser.new_page()
        
        # Set up event recording if enabled
        if self.recording_mode:
            logging.info(f"Session {self.session_id}: Recording mode enabled - events will be logged")
            await self.setup_event_recording()
        
    async def login(self) -> bool:
        """Handle login process"""
        try:
            # Start with the login URL
            await self.page.goto(self.login_url)
            
            # Check if we're already logged in by looking for username/password fields
            try:
                username_field = await self.page.wait_for_selector('input[name="username"]', timeout=5000)
                if username_field:
                    # We need to log in
                    logging.info(f"Session {self.session_id}: Login page detected, entering credentials")
                    await self.page.fill('input[name="username"]', self.username)
                    await self.page.fill('input[name="password"]', self.password)
                    await self.page.click('button[type="submit"]')
                    
                    # Wait for OTP page or successful login
                    await self.page.wait_for_timeout(5000)
                    
                    # Check if we're on the OTP page
                    current_url = self.page.url
                    if "authenticate" in current_url:
                        console.print(f"[yellow]OTP required for session {self.session_id}.[/yellow]")
                        console.print("[yellow]Please complete the OTP verification in the browser window.[/yellow]")
                        console.print("[yellow]The script will continue once you're logged in.[/yellow]")
                        
                        # Wait for the user to complete OTP verification (max 2 minutes)
                        max_wait = 120  # seconds
                        for _ in range(max_wait):
                            current_url = self.page.url
                            if "authenticate" not in current_url:
                                logging.info(f"Session {self.session_id}: OTP verification completed")
                                break
                            await self.page.wait_for_timeout(1000)  # Check every second
                        else:
                            logging.error(f"Session {self.session_id}: OTP verification timed out")
                            return False
            except Exception as e:
                logging.info(f"Session {self.session_id}: Already logged in or login page not as expected: {str(e)}")
            
            # Wait for authentication to complete
            await self.page.wait_for_timeout(5000)
            
            # Check current URL to determine if we're logged in
            current_url = self.page.url
            logging.info(f"Session {self.session_id}: Current URL after login process: {current_url}")
            
            # If we're on the order search page, we're logged in
            if "order/ibtorder/search" in current_url:
                logging.info(f"Session {self.session_id}: Successfully logged in, now navigating to property search page")
                
                # Take a screenshot of the current page
                await self.page.screenshot(path=f"order_search_page_session_{self.session_id}.png")
                
                # Navigate to the property search page
                await self.page.goto(self.search_url)
                await self.page.wait_for_timeout(5000)  # Give it time to load
                
                # Take a screenshot of the property search page
                await self.page.screenshot(path=f"property_search_page_session_{self.session_id}.png")
                
                console.print(f"[green]Successfully logged in session {self.session_id} and navigated to property search![/green]")
                return True
            else:
                logging.error(f"Session {self.session_id}: Failed to reach the expected page after login")
                return False
            
        except Exception as e:
            logging.error(f"Login failed for session {self.session_id}: {str(e)}")
            return False
            
    async def extract_order_details(self, order_id: str) -> Optional[OrderData]:
        """Extract detailed information from the order details page"""
        try:
            logging.info(f"Session {self.session_id} starting extraction for order {order_id}")
            
            # Wait for the search results table to be visible
            await self.page.wait_for_selector('#searchResultForm\\:orderSRT_data', state='visible', timeout=30000)
            logging.info("Search results table is visible")
            
            # Find the row containing our order ID
            found = False
            data_ri = None
            
            while not found:
                # Find all rows in the table
                rows = await self.page.query_selector_all('#searchResultForm\\:orderSRT_data tr')
                
                for row in rows:
                    order_id_cell = await row.query_selector('td:nth-child(2) span')
                    if order_id_cell:
                        cell_text = await order_id_cell.inner_text()
                        if cell_text.strip() == str(order_id):
                            data_ri = await row.get_attribute('data-ri')
                            found = True
                            break
                
                if found:
                    break
                    
                # If order not found on current page, try to go to next page
                next_button = await self.page.query_selector('#searchResultForm\\:orderSRT_paginator_bottom .ui-paginator-next:not(.ui-state-disabled)')
                if not next_button:
                    logging.error(f"Session {self.session_id} could not find row for order {order_id} and no more pages available")
                    return None
                    
                logging.info("Session {self.session_id} order not found on current page, moving to next page")
                await next_button.click()
                await self.page.wait_for_selector('#searchResultForm\\:orderSRT_data', state='visible')
                await self.page.wait_for_timeout(2000)
            
            if not data_ri:
                logging.error(f"Session {self.session_id} could not find data-ri attribute for order {order_id}")
                return None
                
            logging.info(f"Session {self.session_id} found order {order_id} with data-ri {data_ri}")
            
            # Initialize order data
            data = OrderData(external_order_id=order_id)
            
            # Click the eye icon and wait for details to load
            eye_icon_selector = f'#searchResultForm\\:orderSRT\\:{data_ri}\\:j_idt240\\:0\\:viewSelectedRowItem'
            eye_icon = await self.page.wait_for_selector(eye_icon_selector, timeout=5000)
            
            if not eye_icon:
                logging.error(f"Session {self.session_id} could not find eye icon for order {order_id}")
                return None
            
            await eye_icon.click()
            logging.info("Session {self.session_id} clicked eye icon, waiting for details page")
            
            # Wait for the details page to load completely
            await self.page.wait_for_selector('.ui-panelgrid-cell', state='visible', timeout=30000)
            await self.page.wait_for_timeout(2000)  # Give the page a moment to fully load
            
            # Field mappings for known fields
            field_mapping = {
                'External Order Id': 'external_order_id',
                'Order Id': 'order_id',
                'Order Status': 'order_status',
                'Order Type': 'order_type',
                'Name': 'customer_name',
                'Customer Type': 'customer_type',
                'Customer Order Reference': 'customer_order_reference',
                'Street': 'street',
                'House Number': 'house_number',
                'Postal Code': 'postal_code',
                'City': 'city',
                'Appointment Status': 'appointment_status',
                'Installation Due Date': 'installation_due_date',
                'KLS ID': 'kls_id',
                'FOL ID': 'fol_id',
                'Building Type': 'building_type',
                'Accommodation Units': 'accommodation_units',
                'Build Up Agreement': 'build_up_agreement',
                'Construction Type': 'construction_type',
                'Project ID': 'project_id',
                'Email': 'customer_email',
                'Phone': 'customer_phone',
                'Mobile': 'customer_mobile'
            }
            
            # Get all table rows
            detail_rows = await self.page.query_selector_all('tr.ui-widget-content')
            
            # Process each row
            for row in detail_rows:
                field_name, value = await self.extract_field_value(row)
                
                if field_name:
                    # Map to standard fields if known
                    model_field = field_mapping.get(field_name)
                    if model_field and hasattr(data, model_field):
                        if value is not None:  # Only set the value if it's not None
                            setattr(data, model_field, value)
                            logging.info(f"Session {self.session_id} extracted {field_name}: {value}")
                    else:
                        # Store unknown fields in additional_fields
                        if field_name and value is not None:
                            data.additional_fields[field_name] = value
                            logging.info(f"Session {self.session_id} stored additional field {field_name}: {value}")
            
            # Try to download exploration protocol
            pdf_path = await self.download_exploration_protocol(order_id)
            if pdf_path:
                logging.info(f"Session {self.session_id}: Exploration protocol saved to {pdf_path}")
            
            return data

        except Exception as e:
            logging.error(f"Session {self.session_id} failed to extract details for order {order_id}: {str(e)}")
            return None
        finally:
            # Ensure we always try to close the detail page
            await self.close_detail_page()

    async def extract_property_details(self, property_id: str) -> Optional[dict]:
        """Extract detailed information from the property details page"""
        try:
            logging.info(f"Session {self.session_id} starting extraction for property {property_id}")
            
            # Wait for the search results table to be visible
            await self.page.wait_for_selector('#searchResultForm\\:propertySRT_data', state='visible', timeout=30000)
            logging.info("Property search results table is visible")
            
            # Find the row containing our property ID
            found = False
            data_ri = None
            
            while not found:
                # Find all rows in the table
                rows = await self.page.query_selector_all('#searchResultForm\\:propertySRT_data tr')
                
                for row in rows:
                    property_id_cell = await row.query_selector('td:nth-child(1) span')
                    if property_id_cell:
                        cell_text = await property_id_cell.inner_text()
                        if cell_text.strip() == str(property_id):
                            data_ri = await row.get_attribute('data-ri')
                            found = True
                            break
                
                if found:
                    break
                    
                # If property not found on current page, try to go to next page
                next_button = await self.page.query_selector('#searchResultForm\\:propertySRT_paginator_bottom .ui-paginator-next:not(.ui-state-disabled)')
                if not next_button:
                    logging.error(f"Session {self.session_id} could not find row for property {property_id} and no more pages available")
                    return None
                    
                logging.info(f"Session {self.session_id} property not found on current page, moving to next page")
                await next_button.click()
                await self.page.wait_for_selector('#searchResultForm\\:propertySRT_data', state='visible')
                await self.page.wait_for_timeout(2000)
            
            if not data_ri:
                logging.error(f"Session {self.session_id} could not find data-ri attribute for property {property_id}")
                return None
                
            logging.info(f"Session {self.session_id} found property {property_id} with data-ri {data_ri}")
            
            # Click the eye icon to view property details
            eye_icon_selector = f'#searchResultForm\\:propertySRT\\:{data_ri}\\:viewSelectedRowItem'
            eye_icon = await self.page.wait_for_selector(eye_icon_selector, timeout=5000)
            
            if not eye_icon:
                logging.error(f"Session {self.session_id} could not find eye icon for property {property_id}")
                return None
            
            await eye_icon.click()
            logging.info(f"Session {self.session_id} clicked eye icon, waiting for property details page")
            
            # Wait for the details page to load completely
            await self.page.wait_for_selector('.ui-panelgrid-cell', state='visible', timeout=30000)
            await self.page.wait_for_timeout(2000)  # Give the page a moment to fully load
            
            # Initialize property data dictionary
            property_data = {
                'property_id': property_id,
                'additional_fields': {}
            }
            
            # Get all table rows
            detail_rows = await self.page.query_selector_all('tr.ui-widget-content')
            
            # Process each row
            for row in detail_rows:
                field_name, value = await self.extract_field_value(row)
                
                if field_name and value:
                    # Store all fields in the additional_fields dictionary
                    property_data['additional_fields'][field_name] = value
                    logging.info(f"Session {self.session_id} extracted property field {field_name}: {value}")
            
            return property_data
            
        except Exception as e:
            logging.error(f"Session {self.session_id} failed to extract details for property {property_id}: {str(e)}")
            return None
        finally:
            # Ensure we always try to close the detail page
            await self.close_detail_page()

    async def extract_field_value(self, row) -> tuple[Optional[str], Optional[str]]:
        """Extract field name and value from a row, handling both visible and hidden elements."""
        try:
            # Try to get the label first
            label_elem = await row.query_selector('td:first-child label')
            if not label_elem:
                return None, None
                
            # Get field name from title or text
            field_name = await label_elem.get_attribute('title')
            if not field_name:
                field_name = await label_elem.inner_text()
            
            # Try to get the value from span
            value_elem = await row.query_selector('td:nth-child(2) span')
            if not value_elem:
                return field_name, None
            
            # Get the actual value (inner text) first
            value = await value_elem.inner_text()
            if not value or value.strip() == "":
                # If no inner text, try title attribute
                value = await value_elem.get_attribute('title')
            
            # If the value matches the field name exactly (case-insensitive), treat it as empty
            if value and value.strip().lower() == field_name.lower():
                return field_name, None
            
            return field_name, value.strip() if value else None
            
        except Exception as e:
            logging.warning(f"Session {self.session_id} failed to extract field: {str(e)}")
            return None, None

    async def is_exploration_button_active(self) -> bool:
        """Check if the exploration protocol button is active."""
        try:
            button = await self.page.query_selector('#processPageForm\\:explorationProtocol')
            if not button:
                return False
            
            # Get button attributes
            button_class = await button.get_attribute('class') or ''
            button_disabled = await button.get_attribute('disabled')
            aria_disabled = await button.get_attribute('aria-disabled')
            
            # Button is active if:
            # 1. No disabled attribute
            # 2. No ui-state-disabled class
            # 3. aria-disabled is "false"
            return (
                button_disabled is None and
                'ui-state-disabled' not in button_class and
                aria_disabled == 'false'
            )
        except Exception as e:
            logging.error(f"Session {self.session_id}: Error checking exploration button state: {str(e)}")
            return False

    async def close_detail_page(self):
        """Close the detail page and return to search results."""
        try:
            # Try to find and click the close button
            close_button = await self.page.wait_for_selector(
                '#page-header-form\\:closeCioDetailsPage',
                timeout=5000
            )
            if close_button:
                await close_button.click()
                # Wait for the details overlay to disappear and search results to be visible again
                await self.page.wait_for_selector('#searchResultForm\\:orderSRT_data', state='visible', timeout=10000)
                logging.info(f"Session {self.session_id} successfully closed details page")
                return True
        except Exception as e:
            logging.warning(f"Session {self.session_id} error closing detail page: {str(e)}")
            try:
                # Fallback: try to go back
                await self.page.go_back()
                await self.page.wait_for_selector('#searchResultForm\\:orderSRT_data', state='visible', timeout=10000)
                return True
            except Exception as back_error:
                logging.error(f"Session {self.session_id} failed to go back: {str(back_error)}")
                return False

    async def download_exploration_protocol(self, order_id: str) -> Optional[Path]:
        """
        Download the exploration protocol PDF if available.
        Returns the path to the downloaded file if successful, None otherwise.
        """
        try:
            # Check if the exploration protocol button is active
            if not await self.is_exploration_button_active():
                logging.info(f"Session {self.session_id}: Exploration protocol not available for order {order_id}")
                return None

            # Set up download path
            pdf_path = self.download_dir / f"{order_id}.pdf"
            
            # Start waiting for download before clicking
            async with self.page.expect_download(timeout=10000) as download_info:
                # Click the exploration protocol button
                button = await self.page.wait_for_selector(
                    '#processPageForm\\:explorationProtocol:not([disabled]):not(.ui-state-disabled)',
                    state='visible',
                    timeout=5000
                )
                
                if not button:
                    logging.info(f"Session {self.session_id}: Exploration protocol button not clickable for order {order_id}")
                    return None
                    
                await button.click()
                
                try:
                    # Wait for the download to start
                    download = await download_info.value
                    
                    # Wait for the download to complete and save the file
                    await download.save_as(pdf_path)
                    
                    logging.info(f"Session {self.session_id}: Successfully downloaded exploration protocol for order {order_id}")
                    return pdf_path
                except Exception as download_error:
                    logging.error(f"Session {self.session_id}: Download failed for order {order_id}: {str(download_error)}")
                    return None

        except Exception as e:
            logging.error(f"Session {self.session_id}: Failed to download exploration protocol for order {order_id}: {str(e)}")
            return None

    async def set_search_criteria(self, area: str, creation_date_start: str = None, creation_date_end: str = None):
        """Set the search criteria to get maximum results"""
        try:
            logging.info(f"Session {self.session_id} setting search criteria")
            
            # Take a screenshot before setting criteria
            await self.page.screenshot(path=f"before_criteria_session_{self.session_id}.png")
            logging.info(f"Session {self.session_id}: Saved screenshot before setting criteria")
            
            # Navigate to property search page
            logging.info(f"Session {self.session_id}: Navigating to property search page")
            await self.page.goto(self.search_url, wait_until="networkidle")
            
            # Wait for the page to load
            await self.page.wait_for_timeout(5000)
            
            # Set the area/city
            try:
                # Look for location area field using the exact selectors from the HTML
                area_selectors = [
                    '#searchCriteriaForm\\:vvmArea_input',
                    'input[name="searchCriteriaForm:vvmArea_input"]',
                    'input[aria-labelledby="searchCriteriaForm:vvmAreaLabel"]',
                    'input[title="Giga Area"]',
                    'input[placeholder*="Giga Area"]',
                    'input[aria-label*="Giga Area"]',
                    '#searchCriteriaForm\\:gigaArea',
                    'input[name="searchCriteriaForm:gigaArea"]',
                    'input[id*="gigaArea"]',
                    '#searchCriteriaForm\\:area_completed',
                    'input[name="searchCriteriaForm:area_completed"]',
                    'input[id*="area"]',
                    'input[id*="location"]'
                ]
                
                area_field = None
                for selector in area_selectors:
                    try:
                        logging.info(f"Session {self.session_id}: Trying to find area field with selector: {selector}")
                        field = await self.page.query_selector(selector)
                        if field:
                            area_field = field
                            logging.info(f"Session {self.session_id}: Found area field with selector: {selector}")
                            break
                    except Exception as e:
                        logging.warning(f"Session {self.session_id}: Error finding area field with selector {selector}: {str(e)}")
                
                if area_field:
                    # Clear and fill the area field
                    await area_field.click()
                    await area_field.fill("")
                    await self.page.wait_for_timeout(500)
                    
                    # Fill the area and wait for suggestions
                    await area_field.fill(area)
                    await self.page.wait_for_timeout(2000)
                    
                    # Press Tab to accept the suggestion
                    await area_field.press("Tab")
                    logging.info(f"Session {self.session_id}: Set area '{area}' using direct Playwright interaction")
                else:
                    logging.warning(f"Session {self.session_id}: Could not find area field with direct Playwright interaction")
                    # Fall back to JavaScript approach
                    area_set = await self.page.evaluate("""(area) => {
                        function interactWithAreaInput(selector, value) {
                            let input;
                            
                            // Handle both string selectors and element references
                            if (typeof selector === 'string') {
                                input = document.querySelector(selector);
                            } else {
                                input = selector;
                            }
                            
                            if (!input) return false;
                            
                            // Clear existing value first
                            input.value = '';
                            
                            // Focus on the element
                            input.focus();
                            
                            // Set the value
                            input.value = value;
                            
                            // Trigger multiple events to ensure the change is registered
                            ['change', 'input', 'blur', 'keyup'].forEach(eventType => {
                                const event = new Event(eventType, { bubbles: true });
                                input.dispatchEvent(event);
                            });
                            
                            return true;
                        }
                        
                        // Try multiple selectors
                        const areaSelectors = [
                            '#searchCriteriaForm\\\\:vvmArea_input',
                            'input[name="searchCriteriaForm:vvmArea_input"]',
                            'input[aria-labelledby="searchCriteriaForm:vvmAreaLabel"]',
                            'input[title="Giga Area"]',
                            'input[placeholder*="Giga Area"]',
                            'input[aria-label*="Giga Area"]',
                            '#searchCriteriaForm\\\\:gigaArea',
                            'input[name="searchCriteriaForm:gigaArea"]',
                            'input[id*="gigaArea"]',
                            '#searchCriteriaForm\\\\:area_completed',
                            'input[name="searchCriteriaForm:area_completed"]',
                            'input[id*="area"]',
                            'input[id*="location"]'
                        ];
                        
                        for (const selector of areaSelectors) {
                            if (interactWithAreaInput(selector, area)) {
                                console.log('Successfully set area with selector:', selector);
                                return true;
                            }
                        }
                        
                        // Last resort: try to identify the field by its label or nearby text
                        const allInputs = document.querySelectorAll('input[type="text"]');
                        for (const input of allInputs) {
                            const id = input.id || '';
                            const name = input.name || '';
                            const title = input.getAttribute('title') || '';
                            
                            if (id.toLowerCase().includes('vvm') || 
                                name.toLowerCase().includes('vvm') ||
                                title.toLowerCase().includes('giga area')) {
                                if (interactWithAreaInput(input, area)) {
                                    console.log('Set area using identified input:', input.id || input.name);
                                    return true;
                                }
                            }
                        }
                        
                        return false;
                    }""", area)
                    
                    if area_set:
                        logging.info(f"Session {self.session_id}: Successfully set area '{area}' with JavaScript fallback")
                    else:
                        logging.warning(f"Session {self.session_id}: Failed to set area with JavaScript fallback")
            except Exception as e:
                logging.error(f"Session {self.session_id}: Error setting area: {str(e)}")
                logging.error(traceback.format_exc())
            
            # Wait for any UI updates
            await self.page.wait_for_timeout(2000)
            
            # Set creation date start field if provided
            if creation_date_start:
                try:
                    # Look for creation date start field using various selectors
                    creation_date_start_selectors = [
                        '#searchCriteriaForm\\:creationDateBegin_input',
                        'input[name="searchCriteriaForm:creationDateBegin_input"]',
                        'input[id*="creationDateBegin"]',
                        'input[id*="creation"][id*="start"]',
                        'input.hasDatepicker'
                    ]
                    
                    date_start_field = None
                    for selector in creation_date_start_selectors:
                        try:
                            logging.info(f"Session {self.session_id}: Trying to find creation date start field with selector: {selector}")
                            field = await self.page.query_selector(selector)
                            if field:
                                date_start_field = field
                                logging.info(f"Session {self.session_id}: Found creation date start field with selector: {selector}")
                                break
                        except Exception as e:
                            logging.warning(f"Session {self.session_id}: Error finding creation date start field with selector {selector}: {str(e)}")
                    
                    if date_start_field:
                        # Clear and fill the date field
                        await date_start_field.click()
                        await date_start_field.fill("")
                        await self.page.wait_for_timeout(500)
                        
                        # Fill the date
                        await date_start_field.fill(creation_date_start)
                        await date_start_field.press("Tab")  # Tab out to trigger any blur events
                        logging.info(f"Session {self.session_id}: Set creation date start '{creation_date_start}' using direct Playwright interaction")
                    else:
                        logging.warning(f"Session {self.session_id}: Could not find creation date start field with direct Playwright interaction")
                        # Fall back to JavaScript approach
                        date_set = await self.page.evaluate("""(date) => {
                            // Function to properly interact with a date input
                            function interactWithDateInput(selector, value) {
                                let input;
                                
                                // Handle both string selectors and element references
                                if (typeof selector === 'string') {
                                    input = document.querySelector(selector);
                                } else {
                                    input = selector;
                                }
                                
                                if (!input) return false;
                                
                                // Clear existing value first
                                input.value = '';
                                
                                // Focus on the element
                                input.focus();
                                
                                // Set the value
                                input.value = value;
                                
                                // Trigger multiple events to ensure the change is registered
                                ['change', 'input', 'blur', 'keyup'].forEach(eventType => {
                                    const event = new Event(eventType, { bubbles: true });
                                    input.dispatchEvent(event);
                                });
                                
                                return true;
                            }
                            
                            // Try multiple selectors for the date input
                            const dateSelectors = [
                                '#searchCriteriaForm\\\\:creationDateBegin_input',
                                'input[name="searchCriteriaForm:creationDateBegin_input"]',
                                'input[id*="creationDateBegin"]',
                                'input[id*="creation"][id*="start"]',
                                'input.hasDatepicker'
                            ];
                            
                            // Try specific selectors first
                            for (const selector of dateSelectors) {
                                if (interactWithDateInput(selector, date)) {
                                    console.log('Successfully set creation date start with selector:', selector);
                                    return true;
                                }
                            }
                            
                            // Last resort: try any date input
                            const dateInputs = document.querySelectorAll('input.hasDatepicker');
                            for (const input of dateInputs) {
                                const id = input.id || '';
                                const name = input.name || '';
                                
                                if (id.toLowerCase().includes('creation') || name.toLowerCase().includes('creation')) {
                                    if (interactWithDateInput(input, date)) {
                                        console.log('Set creation date start using identified date input:', input.id || input.name);
                                        return true;
                                    }
                                }
                            }
                            
                            return false;
                        }""", creation_date_start)
                        
                        if date_set:
                            logging.info(f"Session {self.session_id}: Successfully set creation date start '{creation_date_start}' with JavaScript fallback")
                        else:
                            logging.warning(f"Session {self.session_id}: Failed to set creation date start with JavaScript fallback")
                except Exception as e:
                    logging.error(f"Session {self.session_id}: Error setting creation date start: {str(e)}")
                    logging.error(traceback.format_exc())
                
                # Wait for any UI updates
                await self.page.wait_for_timeout(1000)
            
            # Set creation date end field if provided
            if creation_date_end:
                try:
                    # Look for creation date end field using various selectors
                    creation_date_end_selectors = [
                        '#searchCriteriaForm\\:creationDateEnd_input',
                        'input[name="searchCriteriaForm:creationDateEnd_input"]',
                        'input[id*="creationDateEnd"]',
                        'input[id*="creation"][id*="end"]',
                        'input.hasDatepicker'
                    ]
                    
                    date_end_field = None
                    for selector in creation_date_end_selectors:
                        try:
                            logging.info(f"Session {self.session_id}: Trying to find creation date end field with selector: {selector}")
                            field = await self.page.query_selector(selector)
                            if field:
                                date_end_field = field
                                logging.info(f"Session {self.session_id}: Found creation date end field with selector: {selector}")
                                break
                        except Exception as e:
                            logging.warning(f"Session {self.session_id}: Error finding creation date end field with selector {selector}: {str(e)}")
                    
                    if date_end_field:
                        # Clear and fill the date field
                        await date_end_field.click()
                        await date_end_field.fill("")
                        await self.page.wait_for_timeout(500)
                        
                        # Fill the date
                        await date_end_field.fill(creation_date_end)
                        await date_end_field.press("Tab")  # Tab out to trigger any blur events
                        logging.info(f"Session {self.session_id}: Set creation date end '{creation_date_end}' using direct Playwright interaction")
                    else:
                        logging.warning(f"Session {self.session_id}: Could not find creation date end field with direct Playwright interaction")
                        # Fall back to JavaScript approach
                        date_set = await self.page.evaluate("""(date) => {
                            // Function to properly interact with a date input
                            function interactWithDateInput(selector, value) {
                                let input;
                                
                                // Handle both string selectors and element references
                                if (typeof selector === 'string') {
                                    input = document.querySelector(selector);
                                } else {
                                    input = selector;
                                }
                                
                                if (!input) return false;
                                
                                // Clear existing value first
                                input.value = '';
                                
                                // Focus on the element
                                input.focus();
                                
                                // Set the value
                                input.value = value;
                                
                                // Trigger multiple events to ensure the change is registered
                                ['change', 'input', 'blur', 'keyup'].forEach(eventType => {
                                    const event = new Event(eventType, { bubbles: true });
                                    input.dispatchEvent(event);
                                });
                                
                                return true;
                            }
                            
                            // Try multiple selectors for the date input
                            const dateSelectors = [
                                '#searchCriteriaForm\\\\:creationDateEnd_input',
                                'input[name="searchCriteriaForm:creationDateEnd_input"]',
                                'input[id*="creationDateEnd"]',
                                'input[id*="creation"][id*="end"]',
                                'input.hasDatepicker'
                            ];
                            
                            // Try specific selectors first
                            for (const selector of dateSelectors) {
                                if (interactWithDateInput(selector, date)) {
                                    console.log('Successfully set creation date end with selector:', selector);
                                    return true;
                                }
                            }
                            
                            // Last resort: try any date input
                            const dateInputs = document.querySelectorAll('input.hasDatepicker');
                            for (const input of dateInputs) {
                                const id = input.id || '';
                                const name = input.name || '';
                                
                                if ((id.toLowerCase().includes('creation') && id.toLowerCase().includes('end')) || 
                                    (name.toLowerCase().includes('creation') && name.toLowerCase().includes('end'))) {
                                    if (interactWithDateInput(input, date)) {
                                        console.log('Set creation date end using identified date input:', input.id || input.name);
                                        return true;
                                    }
                                }
                            }
                            
                            return false;
                        }""", creation_date_end)
                        
                        if date_set:
                            logging.info(f"Session {self.session_id}: Successfully set creation date end '{creation_date_end}' with JavaScript fallback")
                        else:
                            logging.warning(f"Session {self.session_id}: Failed to set creation date end with JavaScript fallback")
                except Exception as e:
                    logging.error(f"Session {self.session_id}: Error setting creation date end: {str(e)}")
                    logging.error(traceback.format_exc())
                
                # Wait for any UI updates
                await self.page.wait_for_timeout(1000)
            
            # Set number of results field to 2500
            try:
                logging.info(f"Session {self.session_id}: Attempting to set number of results by simulating manual interaction")
                
                # Step 1: Find and click the dropdown label to open it (based on DOM events)
                dropdown_label_selector = '#searchCriteriaForm\\:nrOfResults_label'
                dropdown_label = await self.page.query_selector(dropdown_label_selector)
                
                if not dropdown_label:
                    logging.warning(f"Session {self.session_id}: Could not find dropdown label with selector: {dropdown_label_selector}")
                    # Fall back to the original selector
                    dropdown_selector = '#searchCriteriaForm\\:nrOfResults'
                    dropdown = await self.page.query_selector(dropdown_selector)
                    
                    if dropdown:
                        await dropdown.click()
                        logging.info(f"Session {self.session_id}: Clicked dropdown to open it")
                        await self.page.wait_for_timeout(1000)
                else:
                    # Click the dropdown label to open it
                    await dropdown_label.click()
                    logging.info(f"Session {self.session_id}: Clicked dropdown label to open it")
                    await self.page.wait_for_timeout(1000)  # Wait for dropdown to open
                
                # Step 2: Find the dropdown panel that appears
                panel_selector = '#searchCriteriaForm\\:nrOfResults_panel'
                panel = await self.page.query_selector(panel_selector)
                
                if not panel:
                    # Try alternative selectors for the panel
                    alternative_panel_selectors = [
                        '#searchCriteriaForm\\:nrOfResults_items',
                        '.ui-selectonemenu-panel:not(.ui-helper-hidden)',
                        'div[id$="_panel"]:not(.ui-helper-hidden)'
                    ]
                    
                    for selector in alternative_panel_selectors:
                        panel = await self.page.query_selector(selector)
                        if panel:
                            logging.info(f"Session {self.session_id}: Found dropdown panel with selector: {selector}")
                            break
                
                if not panel:
                    logging.warning(f"Session {self.session_id}: Could not find dropdown panel")
                else:
                    # Step 3: Find and click the 2500 option in the panel
                    option_selectors = [
                        'li[data-label="2500"]',
                        '#searchCriteriaForm\\:nrOfResults_6',  # Based on the HTML, this is the 7th option (0-indexed)
                        'li:contains("2500")'
                    ]
                    
                    option = None
                    for selector in option_selectors:
                        try:
                            opt = await panel.query_selector(selector)
                            if opt:
                                option = opt
                                logging.info(f"Session {self.session_id}: Found option 2500 with selector: {selector}")
                                break
                        except Exception as e:
                            logging.warning(f"Session {self.session_id}: Error finding option with selector {selector}: {str(e)}")
                    
                    if option:
                        # Click the 2500 option
                        await option.click()
                        logging.info(f"Session {self.session_id}: Clicked option 2500")
                        await self.page.wait_for_timeout(1000)  # Wait for selection to register
                        
                        # Verify the selection was made
                        label_text = await self.page.evaluate("""() => {
                            const label = document.querySelector('#searchCriteriaForm\\\\:nrOfResults_label');
                            return label ? label.textContent.trim() : null;
                        }""")
                        
                        # Also check the hidden input value which is more reliable
                        hidden_input_value = await self.page.evaluate("""() => {
                            const hiddenInput = document.querySelector('#searchCriteriaForm\\\\:nrOfResults_input');
                            return hiddenInput ? hiddenInput.value : null;
                        }""")
                        
                        if label_text == "2500" or hidden_input_value == "2500":
                            logging.info(f"Session {self.session_id}: Successfully set number of results to 2500 (verified)")
                        else:
                            logging.warning(f"Session {self.session_id}: Label text: {label_text}, Hidden input value: {hidden_input_value}")
                    else:
                        logging.warning(f"Session {self.session_id}: Could not find option 2500 in dropdown panel")
                        
                        # Try to find all available options for debugging
                        options = await panel.query_selector_all('li')
                        option_texts = []
                        for opt in options:
                            text = await opt.text_content()
                            option_texts.append(text.strip())
                        
                        logging.info(f"Session {self.session_id}: Available options: {option_texts}")
                        
                        # Try clicking any option that contains "2500"
                        for opt in options:
                            text = await opt.text_content()
                            if "2500" in text:
                                await opt.click()
                                logging.info(f"Session {self.session_id}: Clicked option containing 2500: {text}")
                                await self.page.wait_for_timeout(1000)
                                break
                
                # If the above fails, try a JavaScript fallback
                if not dropdown_label or not panel or not option:
                    logging.info(f"Session {self.session_id}: Trying JavaScript fallback for dropdown interaction")
                    
                    # Use JavaScript to simulate the exact same manual interaction observed in DOM events
                    js_result = await self.page.evaluate(r"""() => {
                        try {
                            // Step 1: Find and click the dropdown label
                            const dropdownLabel = document.querySelector('#searchCriteriaForm\\:nrOfResults_label');
                            if (!dropdownLabel) return { success: false, message: "Dropdown label not found" };
                            
                            // Click to open the dropdown
                            dropdownLabel.click();
                            console.log("Clicked dropdown label");
                            
                            // Wait a bit for the panel to appear
                            setTimeout(() => {
                                // Step 2: Find the option with ID that worked in DOM events
                                const option = document.querySelector('#searchCriteriaForm\\:nrOfResults_6');
                                if (!option) {
                                    console.log("Option not found by ID");
                                    
                                    // Try alternative selectors
                                    const options = document.querySelectorAll('.ui-selectonemenu-items li');
                                    let found = false;
                                    
                                    for (const opt of options) {
                                        if (opt.textContent.includes('2500')) {
                                            opt.click();
                                            found = true;
                                            console.log("Clicked option by text content");
                                            break;
                                        }
                                    }
                                    
                                    if (!found) {
                                        // Direct value setting as last resort
                                        const hiddenInput = document.querySelector('#searchCriteriaForm\\:nrOfResults_input');
                                        if (hiddenInput) {
                                            hiddenInput.value = "2500";
                                            
                                            // Trigger change event
                                            const event = new Event('change', { bubbles: true });
                                            hiddenInput.dispatchEvent(event);
                                            
                                            console.log("Set value directly on hidden input");
                                            return { success: true, message: "Set value directly on hidden input" };
                                        }
                                    }
                                    
                                    return { success: found, message: found ? "Clicked option by text" : "Option not found" };
                                }
                                
                                // Click the option
                                option.click();
                                console.log("Clicked option by ID");
                                return { success: true, message: "Clicked option by ID" };
                            }, 500);
                            
                            return { success: true, message: "Started dropdown interaction" };
                        } catch (e) {
                            return { success: false, message: e.toString() };
                        }
                    }""")
                    
                    if js_result.get('success'):
                        logging.info(f"Session {self.session_id}: JavaScript fallback: {js_result.get('message')}")
                        await self.page.wait_for_timeout(1000)  # Wait for JS to complete
                    else:
                        logging.warning(f"Session {self.session_id}: JavaScript fallback failed: {js_result.get('message')}")
            except Exception as e:
                logging.error(f"Session {self.session_id}: Error setting number of results: {str(e)}")
                logging.error(traceback.format_exc())
            
            # Final verification and recovery attempt if all previous methods failed
            final_hidden_input_value = await self.page.evaluate("""() => {
                const hiddenInput = document.querySelector('#searchCriteriaForm\\\\:nrOfResults_input');
                return hiddenInput ? hiddenInput.value : null;
            }""")
            
            if final_hidden_input_value != "2500":
                logging.warning(f"Session {self.session_id}: All dropdown selection methods failed. Attempting direct value injection.")
                
                # Direct value injection as last resort
                try:
                    await self.page.evaluate("""() => {
                        const hiddenInput = document.querySelector('#searchCriteriaForm\\\\:nrOfResults_input');
                        if (hiddenInput) {
                            // Set the value directly
                            hiddenInput.value = "2500";
                            
                            // Trigger change events
                            const changeEvent = new Event('change', { bubbles: true });
                            hiddenInput.dispatchEvent(changeEvent);
                            
                            // Update the label text for visual consistency
                            const label = document.querySelector('#searchCriteriaForm\\\\:nrOfResults_label');
                            if (label) {
                                label.textContent = "2500";
                            }
                            
                            console.log("Direct value injection completed");
                            return true;
                        }
                        return false;
                    }""")
                    
                    logging.info(f"Session {self.session_id}: Completed direct value injection recovery attempt")
                except Exception as e:
                    logging.error(f"Session {self.session_id}: Error in direct value injection: {str(e)}")
            
            # Wait for any UI updates
            await self.page.wait_for_timeout(1000)
            
            # Take a screenshot after setting criteria but before search
            await self.page.screenshot(path=f"after_criteria_session_{self.session_id}.png")
            logging.info(f"Session {self.session_id}: Saved screenshot after setting criteria")
            
            # Try direct Playwright interaction for the search button
            try:
                # Look for search button using various selectors
                button_selectors = [
                    '#searchCriteriaForm\\:searchButton',
                    'button[type="submit"]',
                    'input[type="submit"]',
                    'button.ui-button',
                    '.ui-button'
                ]
                
                search_button = None
                for selector in button_selectors:
                    try:
                        logging.info(f"Session {self.session_id}: Trying to find search button with selector: {selector}")
                        buttons = await self.page.query_selector_all(selector)
                        if buttons:
                            # Try to find a button with search-related text
                            for button in buttons:
                                text = await button.text_content()
                                if text and ('search' in text.lower() or 'suchen' in text.lower() or 'submit' in text.lower()):
                                    search_button = button
                                    logging.info(f"Session {self.session_id}: Found search button with text: {text}")
                                    break
                            
                            # If no specific search text found, use the first button
                            if not search_button and buttons:
                                search_button = buttons[0]
                                logging.info(f"Session {self.session_id}: Using first button as search button")
                                break
                    except Exception as e:
                        logging.warning(f"Session {self.session_id}: Error finding search button with selector {selector}: {str(e)}")
                
                if search_button:
                    # Click the search button
                    await search_button.click()
                    logging.info(f"Session {self.session_id}: Clicked search button using direct Playwright interaction")
                else:
                    logging.warning(f"Session {self.session_id}: Could not find search button with direct Playwright interaction")
            except Exception as e:
                logging.error(f"Session {self.session_id}: Error clicking search button with direct Playwright: {str(e)}")
                logging.error(traceback.format_exc())
            
            # Wait for search results to load
            await self.page.wait_for_timeout(5000)
            
            # Check for results
            results_count = await self.page.evaluate(r"""() => {
                // Try to find results count display
                const resultCountSelectors = [
                    '.ui-datatable-header',
                    '.result-count',
                    '.search-results-header',
                    'span:contains("Results")',
                    'div:contains("Results")'
                ];
                
                for (const selector of resultCountSelectors) {
                    const element = document.querySelector(selector);
                    if (element) {
                        const text = element.textContent || '';
                        // Try to extract a number from the text
                        const match = text.match(/(\d+)/);
                        if (match) {
                            return parseInt(match[1], 10);
                        }
                        return text; // If no number found, return the text
                    }
                }
                
                return null;
            }""")
            
            if results_count:
                logging.info(f"Session {self.session_id}: Search returned approximately {results_count} results")
            else:
                logging.warning(f"Session {self.session_id}: Could not determine number of search results")
            
            # Take a screenshot of search results
            await self.page.screenshot(path=f"search_results_session_{self.session_id}.png")
            logging.info(f"Session {self.session_id}: Saved screenshot of search results")
            
            return True
            
        except Exception as e:
            logging.error(f"Session {self.session_id}: Error setting search criteria: {str(e)}")
            logging.error(traceback.format_exc())
            return False

    async def wait_for_excel_download(self) -> Optional[str]:
        """Wait for Excel file to be downloaded"""
        try:
            logging.info(f"Session {self.session_id} preparing to download Excel")
            
            # Wait for the export button to be visible
            export_button = await self.page.wait_for_selector('#searchResultForm\\:orderSRT\\:exportData', timeout=10000)
            if not export_button:
                logging.error(f"Session {self.session_id} export button not found")
                return None
            
            # Create downloads directory
            downloads_dir = Path("downloads")
            downloads_dir.mkdir(exist_ok=True)
            
            # Start download
            async with self.page.expect_download(timeout=10000) as download_info:
                # Click the export button
                button = await self.page.wait_for_selector(
                    '#searchResultForm\\:orderSRT\\:exportData:not([disabled]):not(.ui-state-disabled)',
                    state='visible',
                    timeout=5000
                )
                
                if not button:
                    logging.info(f"Session {self.session_id}: Export button not clickable")
                    return None
                    
                await button.click()
                
                try:
                    # Wait for the download to start
                    download = await download_info.value
                    
                    # Wait for the download to complete and save the file
                    await download.save_as(downloads_dir / download.suggested_filename)
                    
                    logging.info(f"Session {self.session_id}: Successfully downloaded Excel")
                    return str(downloads_dir / download.suggested_filename)
                except Exception as download_error:
                    logging.error(f"Session {self.session_id}: Download failed: {str(download_error)}")
                    return None

        except Exception as e:
            logging.error(f"Session {self.session_id}: Failed to download Excel: {str(e)}")
            return None

    async def extract_properties_from_results(self) -> List[dict]:
        """Extract all properties from the search results"""
        properties = []
        try:
            # Wait for the search results table to be visible
            await self.page.wait_for_selector('#searchResultForm\\:propertySRT_data', state='visible', timeout=30000)
            
            page_num = 1
            has_next_page = True
            
            while has_next_page:
                logging.info(f"Session {self.session_id} processing property results page {page_num}")
                
                # Get all rows in the current page
                rows = await self.page.query_selector_all('#searchResultForm\\:propertySRT_data tr')
                
                for row in rows:
                    property_data = {}
                    
                    # Extract data from each cell
                    cells = await row.query_selector_all('td')
                    if len(cells) >= 5:  # Ensure we have enough cells
                        # Extract property ID from first cell
                        id_cell = await cells[0].query_selector('span')
                        if id_cell:
                            property_id = await id_cell.inner_text()
                            property_data['property_id'] = property_id.strip()
                        
                        # Extract address from second cell
                        address_cell = await cells[1].query_selector('span')
                        if address_cell:
                            address = await address_cell.inner_text()
                            property_data['address'] = address.strip()
                        
                        # Extract postal code from third cell
                        postal_cell = await cells[2].query_selector('span')
                        if postal_cell:
                            postal = await postal_cell.inner_text()
                            property_data['postal_code'] = postal.strip()
                        
                        # Extract city from fourth cell
                        city_cell = await cells[3].query_selector('span')
                        if city_cell:
                            city = await city_cell.inner_text()
                            property_data['city'] = city.strip()
                        
                        # Extract status from fifth cell
                        status_cell = await cells[4].query_selector('span')
                        if status_cell:
                            status = await status_cell.inner_text()
                            property_data['status'] = status.strip()
                        
                        # Add to properties list if we have a property ID
                        if 'property_id' in property_data:
                            properties.append(property_data)
                
                # Check if there's a next page
                next_button = await self.page.query_selector('#searchResultForm\\:propertySRT_paginator_bottom .ui-paginator-next:not(.ui-state-disabled)')
                if next_button:
                    await next_button.click()
                    await self.page.wait_for_selector('#searchResultForm\\:propertySRT_data', state='visible')
                    await self.page.wait_for_timeout(2000)  # Wait for page to load
                    page_num += 1
                else:
                    has_next_page = False
            
            logging.info(f"Session {self.session_id} extracted {len(properties)} properties from search results")
            return properties
            
        except Exception as e:
            logging.error(f"Session {self.session_id} failed to extract properties from results: {str(e)}")
            return properties

    async def close(self):
        """Close the browser session"""
        if self.browser:
            await self.browser.close()

    async def setup_event_recording(self):
        """Set up JavaScript event recording to capture user interactions"""
        logging.info(f"Session {self.session_id}: Setting up event recording")
        
        # Add JavaScript to record DOM events
        await self.page.add_init_script("""
            window.recordedEvents = [];
            
            function recordEvent(event) {
                const target = event.target;
                const targetInfo = {
                    tagName: target.tagName,
                    id: target.id,
                    className: target.className,
                    type: target.type,
                    name: target.name,
                    value: target.value,
                    checked: target.checked,
                    textContent: target.textContent ? target.textContent.trim().substring(0, 50) : null
                };
                
                // Get CSS selector
                function getSelector(el) {
                    if (!el) return '';
                    if (el.id) return '#' + el.id;
                    
                    let path = el.tagName.toLowerCase();
                    if (el.className) {
                        const classes = el.className.split(/\\s+/).filter(c => c);
                        if (classes.length > 0) {
                            path += '.' + classes.join('.');
                        }
                    }
                    
                    return path;
                }
                
                const eventInfo = {
                    type: event.type,
                    timestamp: new Date().toISOString(),
                    target: targetInfo,
                    selector: getSelector(target),
                    pageUrl: window.location.href
                };
                
                // Add event-specific properties
                if (event.type === 'click' || event.type === 'mousedown' || event.type === 'mouseup') {
                    eventInfo.x = event.clientX;
                    eventInfo.y = event.clientY;
                } else if (event.type === 'change' || event.type === 'input') {
                    eventInfo.value = event.target.value;
                }
                
                window.recordedEvents.push(eventInfo);
                console.log('EVENT_RECORDED: ' + JSON.stringify(eventInfo));
            }
            
            // Events to record
            const eventsToRecord = [
                'click', 'mousedown', 'mouseup', 
                'change', 'input', 'focus', 'blur',
                'submit'
            ];
            
            // Add event listeners
            eventsToRecord.forEach(eventType => {
                document.addEventListener(eventType, recordEvent, true);
            });
            
            console.log('Event recording initialized');
        """)
        
        # Set up console message handler
        self.page.on("console", self.handle_console_message)
        logging.info(f"Session {self.session_id}: Event recording setup complete")
    
    async def handle_console_message(self, msg):
        """Handle console messages, looking for recorded events"""
        text = msg.text
        if "EVENT_RECORDED:" in text:
            # Extract the JSON part
            json_str = text.split("EVENT_RECORDED:", 1)[1].strip()
            try:
                event_data = json.loads(json_str)
                # Log the event
                logging.info(f"Session {self.session_id}: DOM Event: {event_data['type']} on {event_data['selector']} ({event_data['target']['tagName']})")
                
                # Save to a separate event log file
                with open(f"dom_events_session_{self.session_id}.json", "a") as f:
                    f.write(json_str + "\n")
            except Exception as e:
                logging.error(f"Session {self.session_id}: Error processing event data: {str(e)}")

class TelekomExporter:
    def __init__(self, recording_mode=False, headless=False):
        load_dotenv()
        self.username = os.getenv("TELEKOM_USERNAME")
        self.password = os.getenv("TELEKOM_PASSWORD")
        self.num_sessions = 3  # Number of parallel browser sessions
        self.sessions: List[BrowserSession] = []
        self.all_fields = set()
        self.extracted_data = []
        self.area = "Bad Sooden-Allendorf, Stadt"  # Default area to search
        self.recording_mode = recording_mode
        self.headless = headless
        self.property_searcher = None  # Will be initialized when needed
        
    async def init_sessions(self):
        """Initialize multiple browser sessions"""
        # Initialize property searcher first
        from ibt_property_search import IBTPropertySearcher
        self.property_searcher = IBTPropertySearcher(recording_mode=self.recording_mode, headless=self.headless)
        if not await self.property_searcher.initialize():
            raise Exception("Failed to initialize property searcher")
            
        # Initialize other sessions for order processing
        for i in range(self.num_sessions):
            session = BrowserSession(
                username=self.username,
                password=self.password,
                session_id=i,
                headless=self.headless,
                recording_mode=self.recording_mode
            )
            await session.init_browser()
            if await session.login():
                self.sessions.append(session)
            else:
                await session.close()
                
        if not self.sessions:
            raise Exception("Failed to initialize any browser sessions")
        
        console.print(f"[green]Successfully initialized {len(self.sessions)} browser sessions[/green]")
        return True
    
    async def extract_all_properties(self):
        """Extract all properties from search results using the property searcher"""
        if not self.property_searcher:
            raise Exception("Property searcher not initialized")
            
        console.print("[yellow]Extracting properties from search results...[/yellow]")
        properties = await self.property_searcher.search_properties(self.area)
        console.print(f"[green]Found {len(properties)} properties[/green]")
        
        # Convert PropertyData objects to dictionaries and store
        self.extracted_data = []
        for prop in properties:
            prop_dict = prop.dict()
            # Flatten additional_fields into main dictionary
            prop_dict.update(prop_dict.pop('additional_fields', {}))
            self.extracted_data.append(prop_dict)
            self.all_fields.update(prop_dict.keys())
            
        return self.extracted_data
    
    async def process_properties_parallel(self, properties: List[dict]) -> None:
        """Process properties in parallel using multiple sessions"""
        console.print(f"[yellow]Processing {len(properties)} properties using {len(self.sessions)} parallel sessions[/yellow]")
        
        # Split properties among sessions
        property_ids = [p['property_id'] for p in properties]
        chunks = [property_ids[i::len(self.sessions)] for i in range(len(self.sessions))]
        
        # Create tasks for each session
        tasks = []
        for session, chunk in zip(self.sessions, chunks):
            tasks.append(self.process_property_chunk(session, chunk))
            
        # Run tasks concurrently
        await asyncio.gather(*tasks)
        
    async def process_property_chunk(self, session: BrowserSession, property_ids: List[str]) -> None:
        """Process a chunk of properties using a single session"""
        for property_id in property_ids:
            data = await session.extract_property_details(str(property_id))
            if data:
                # Find the existing property in extracted_data
                existing_property = next((p for p in self.extracted_data if p.get('property_id') == property_id), None)
                
                if existing_property:
                    # Update with detailed information
                    existing_property.update(data.get('additional_fields', {}))
                    self.all_fields.update(existing_property.keys())
                    console.print(f"[green]Session {session.session_id} successfully processed property {property_id}[/green]")
                else:
                    # Create new entry if not found
                    flat_data = {'property_id': property_id}
                    flat_data.update(data.get('additional_fields', {}))
                    self.all_fields.update(flat_data.keys())
                    self.extracted_data.append(flat_data)
                    console.print(f"[green]Session {session.session_id} successfully processed property {property_id}[/green]")
            else:
                console.print(f"[red]Session {session.session_id} failed to process property {property_id}[/red]")

    def save_to_csv(self, output_file: str) -> None:
        """Save extracted data to CSV with all fields as columns"""
        # Sort fields to ensure consistent column order
        fields = sorted(list(self.all_fields))
        
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                for data in self.extracted_data:
                    # Ensure all fields are present in each row
                    row = {field: data.get(field, '') for field in fields}
                    writer.writerow(row)
            print(f"Successfully saved data to {output_file}")
        except Exception as e:
            print(f"Error saving CSV: {str(e)}")

    async def wait_for_excel_download(self) -> Optional[str]:
        """Wait for Excel file to be downloaded after user clicks the export button"""
        try:
            console.print("[yellow]Please set your search criteria and click the Excel export button when ready...[/yellow]")
            
            # Create downloads directory
            downloads_dir = Path("downloads")
            downloads_dir.mkdir(exist_ok=True)
            
            # Wait for download to start
            async with self.sessions[0].page.expect_download(timeout=10000) as download_info:
                # Wait for user to click the export button
                input("Press Enter after clicking the export button...")
                download = await download_info.value
                
                # Wait for the download to complete
                path = downloads_dir / download.suggested_filename
                await download.save_as(path)
                console.print(f"[green]Successfully downloaded orders to {path}[/green]")
                return str(path)
        except Exception as e:
            logging.error(f"Failed to capture Excel download: {str(e)}")
            return None

    def parse_orders_excel(self, excel_path: str) -> List[str]:
        """Parse the Excel file to get Order IDs"""
        try:
            df = pd.read_excel(excel_path)
            if 'Order ID' not in df.columns:
                logging.error("Excel file does not contain 'Order ID' column")
                return []
            
            # Get unique Order IDs
            order_ids = df['Order ID'].unique().tolist()
            console.print(f"[green]Found {len(order_ids)} unique orders[/green]")
            return order_ids
        except Exception as e:
            logging.error(f"Failed to parse Excel file: {str(e)}")
            return []

    async def get_order_ids_from_excel(self, excel_path: str) -> List[str]:
        """Parse the Excel file to get Order IDs"""
        try:
            df = pd.read_excel(excel_path)
            if 'Order ID' not in df.columns:
                logging.error("Excel file does not contain 'Order ID' column")
                return []
            
            # Get unique Order IDs
            order_ids = df['Order ID'].unique().tolist()
            console.print(f"[green]Found {len(order_ids)} unique orders[/green]")
            return order_ids
        except Exception as e:
            logging.error(f"Failed to parse Excel file: {str(e)}")
            return []

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Telekom Property Export Tool")
    parser.add_argument("--area", type=str, help="Area to search for properties")
    parser.add_argument("--record", action="store_true", help="Enable recording mode to capture DOM events")
    parser.add_argument("--output", type=str, default="telekom_properties.csv", help="Output CSV file path")
    parser.add_argument("--headless", action="store_true", help="Run browsers in headless mode (invisible)")
    args = parser.parse_args()
    
    try:
        exporter = TelekomExporter(recording_mode=args.record, headless=args.headless)
        
        if args.area:
            exporter.area = args.area
        
        # Initialize sessions and set search criteria
        console.print("[yellow]Initializing browser sessions and setting search criteria...[/yellow]")
        await exporter.init_sessions()
        
        if args.record:
            # In recording mode, just keep the browser open for manual interaction
            console.print("[yellow]Recording mode enabled. DOM events will be logged to dom_events_session_X.json files.[/yellow]")
            console.print("[yellow]Please interact with the browser manually to solve the dropdown issue.[/yellow]")
            console.print("[yellow]Press Ctrl+C when done to exit.[/yellow]")
            
            try:
                # Keep the script running until user interrupts
                while True:
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                console.print("[yellow]Recording stopped by user.[/yellow]")
        else:
            # Extract all properties from search results
            properties = await exporter.extract_all_properties()
            if not properties:
                console.print("[red]No properties found in search results[/red]")
                return
                
            console.print(f"[green]Successfully retrieved {len(properties)} properties[/green]")
            if properties:
                console.print(f"First few properties: {properties[:3]}")
            
            # Process properties in parallel to get detailed information
            await exporter.process_properties_parallel(properties)
            
            # Generate timestamp for filenames
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save extracted data
            csv_file = f"{args.output.split('.')[0]}_{timestamp}.csv"
            exporter.save_to_csv(csv_file)
            console.print(f"[green]Saved property data to {csv_file}[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        logging.error(f"Error in main: {str(e)}")
        
    finally:
        # Ensure all sessions are closed
        if 'exporter' in locals() and hasattr(exporter, 'sessions'):
            for session in exporter.sessions:
                await session.close()
        console.print("[yellow]All browser sessions closed[/yellow]")

if __name__ == "__main__":
    asyncio.run(main())
