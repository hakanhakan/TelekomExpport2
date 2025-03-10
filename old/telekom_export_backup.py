#!/usr/bin/env python3
"""
Telekom Order Export Automation Tool
This script automates the extraction of order details from the Telekom supplier portal.
"""

import asyncio
import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from playwright.async_api import async_playwright, Browser, Page
from rich.console import Console
from pydantic import BaseModel
from dotenv import load_dotenv
import os

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
    def __init__(self, username: str, password: str, session_id: int):
        self.username = username
        self.password = password
        self.session_id = session_id
        self.base_url = "https://glasfaser.telekom.de/auftragnehmerportal-ui"
        self.search_url = f"{self.base_url}/order/ibtorder/search?a-cid=58222"
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        
    async def init_browser(self):
        """Initialize browser instance"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=False)
        self.page = await self.browser.new_page()
        
    async def login(self) -> bool:
        """Handle login process including OTP"""
        try:
            await self.page.goto(self.search_url)
            await self.page.fill('input[name="username"]', self.username)
            await self.page.fill('input[name="password"]', self.password)
            await self.page.click('button[type="submit"]')
            
            try:
                otp_input = await self.page.wait_for_selector('input[name="otp"]', timeout=5000)
                if otp_input:
                    console.print(f"[yellow]OTP required for session {self.session_id}. Please enter the code:[/yellow]")
                    otp = input(f"Enter OTP for session {self.session_id}: ")
                    await self.page.fill('input[name="otp"]', otp)
                    await self.page.click('button[type="submit"]')
            except:
                pass
                
            await self.page.wait_for_selector('#searchResultForm\\:orderSRT', timeout=30000)
            console.print(f"[green]Successfully logged in session {self.session_id}![/green]")
            return True
            
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
                        # Store in additional_fields if not a standard field and value exists
                        if value is not None:
                            data.additional_fields[field_name] = value
                            logging.info(f"Session {self.session_id} extracted {field_name}: {value}")
            
            # Go back to search results
            logging.info("Session {self.session_id} attempting to return to search results")
            try:
                close_button = await self.page.wait_for_selector('#page-header-form\\:closeCioDetailsPage', timeout=5000)
                if close_button:
                    await close_button.click()
                    # Wait for the details overlay to disappear and search results to be visible again
                    await self.page.wait_for_selector('#searchResultForm\\:orderSRT_data', state='visible', timeout=10000)
                    logging.info("Session {self.session_id} successfully closed details page")
                else:
                    logging.warning("Session {self.session_id} close button not found, using browser back")
                    await self.page.go_back()
                    await self.page.wait_for_selector('#searchResultForm\\:orderSRT_data', state='visible', timeout=10000)
            except Exception as e:
                logging.error(f"Session {self.session_id} error closing detail page: {str(e)}")
                # Try to recover by going back
                await self.page.go_back()
                await self.page.wait_for_selector('#searchResultForm\\:orderSRT_data', state='visible', timeout=10000)

            return data

        except Exception as e:
            logging.error(f"Session {self.session_id} failed to extract details for order {order_id}: {str(e)}")
            return None
            
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

    async def set_search_criteria(self):
        """Set the search criteria to get maximum results"""
        try:
            logging.info(f"Session {self.session_id} setting search criteria")
            
            # Wait for the search form to be visible and fully loaded
            await self.page.wait_for_selector('#searchCriteriaForm', timeout=30000)
            await self.page.wait_for_timeout(1000)  # Give the form a moment to fully initialize
            
            # Set number of results to 2500 using JavaScript since it's a PrimeFaces component
            await self.page.evaluate("""() => {
                document.querySelector('#searchCriteriaForm\\\\:nrOfResults_input').value = '2500';
                // Trigger change event to ensure PrimeFaces updates its internal state
                var event = new Event('change', { bubbles: true });
                document.querySelector('#searchCriteriaForm\\\\:nrOfResults_input').dispatchEvent(event);
            }""")
            await self.page.wait_for_timeout(500)
            
            # Click search button
            search_button = await self.page.wait_for_selector('#searchCriteriaForm\\:searchButton')
            if search_button:
                await search_button.click()
                # Wait for results to load
                await self.page.wait_for_selector('#searchResultForm\\:orderSRT_data', timeout=30000)
                logging.info(f"Session {self.session_id} search criteria applied successfully")
                return True
            
            return False
            
        except Exception as e:
            logging.error(f"Session {self.session_id} failed to set search criteria: {str(e)}")
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
            async with self.page.expect_download() as download_info:
                await export_button.click()
                download = await download_info.value
                
                # Save the file
                path = downloads_dir / download.suggested_filename
                await download.save_as(path)
                logging.info(f"Session {self.session_id} successfully downloaded Excel to {path}")
                return str(path)
                
        except Exception as e:
            logging.error(f"Session {self.session_id} failed to download Excel: {str(e)}")
            return None

    async def close(self):
        """Close the browser session"""
        if self.browser:
            await self.browser.close()

class TelekomExporter:
    def __init__(self):
        load_dotenv()
        self.username = os.getenv("TELEKOM_USERNAME")
        self.password = os.getenv("TELEKOM_PASSWORD")
        self.num_sessions = 3  # Number of parallel browser sessions
        self.sessions: List[BrowserSession] = []
        self.all_fields = set()
        self.extracted_data = []
        
    async def init_sessions(self):
        """Initialize multiple browser sessions"""
        for i in range(self.num_sessions):
            session = BrowserSession(self.username, self.password, i)
            await session.init_browser()
            if await session.login():
                # Set search criteria for each session
                if await session.set_search_criteria():
                    self.sessions.append(session)
                else:
                    await session.close()
            else:
                await session.close()
                
        if not self.sessions:
            raise Exception("Failed to initialize any browser sessions")
        
        # Download Excel file using the first session
        excel_file = await self.sessions[0].wait_for_excel_download()
        if not excel_file:
            raise Exception("Failed to download Excel file")
        
        return excel_file

    async def process_orders_parallel(self, order_ids: List[str]) -> None:
        """Process orders in parallel using multiple sessions"""
        console.print(f"[yellow]Processing {len(order_ids)} orders using {len(self.sessions)} parallel sessions[/yellow]")
        
        # Split orders among sessions
        chunks = [order_ids[i::len(self.sessions)] for i in range(len(self.sessions))]
        
        # Create tasks for each session
        tasks = []
        for session, chunk in zip(self.sessions, chunks):
            tasks.append(self.process_chunk(session, chunk))
            
        # Run tasks concurrently
        await asyncio.gather(*tasks)
        
    async def process_chunk(self, session: BrowserSession, order_ids: List[str]) -> None:
        """Process a chunk of orders using a single session"""
        for order_id in order_ids:
            data = await session.extract_order_details(str(order_id))
            if data:
                order_dict = data.model_dump()
                flat_dict = self.flatten_order_data(order_dict)
                flat_dict.update(data.additional_fields)
                self.all_fields.update(flat_dict.keys())
                self.extracted_data.append(flat_dict)
                console.print(f"[green]Session {session.session_id} successfully processed order {order_id}[/green]")
            else:
                console.print(f"[red]Session {session.session_id} failed to process order {order_id}[/red]")

    def flatten_order_data(self, order_dict: dict) -> dict:
        """Flatten nested dictionary and handle special types"""
        flat_dict = {}
        
        # Handle each field
        for key, value in order_dict.items():
            # Skip the additional_fields as we'll merge them directly
            if key == 'additional_fields':
                continue
                
            # Convert any special types to string
            if isinstance(value, (datetime,)):
                flat_dict[key] = value.isoformat()
            elif isinstance(value, (list, dict)):
                flat_dict[key] = str(value)
            else:
                flat_dict[key] = value
        
        return flat_dict

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

    def merge_with_excel(self, excel_file: str, output_file: str) -> None:
        """Merge extracted data with the original Excel file"""
        try:
            # Read the original Excel file
            df_excel = pd.read_excel(excel_file)
            
            # Convert extracted data to DataFrame
            df_extracted = pd.DataFrame(self.extracted_data)
            
            # Find the order ID column in the Excel file
            excel_order_id_col = None
            possible_columns = ['Order ID', 'OrderId', 'Order Id', 'order_id', 'Order_ID']
            for col in possible_columns:
                if col in df_excel.columns:
                    excel_order_id_col = col
                    break
            
            if not excel_order_id_col:
                print("Error: Could not find order ID column in Excel file")
                return
            
            # Rename the column to match our extracted data
            df_excel = df_excel.rename(columns={excel_order_id_col: 'order_id'})
            
            # Convert order_id to string in both DataFrames for proper matching
            df_excel['order_id'] = df_excel['order_id'].astype(str)
            if 'order_id' in df_extracted.columns:
                df_extracted['order_id'] = df_extracted['order_id'].astype(str)
            
            # Merge DataFrames based on order_id
            df_merged = pd.merge(
                df_excel,
                df_extracted,
                on='order_id',
                how='left',
                suffixes=('', '_extracted')
            )
            
            # Save merged data
            df_merged.to_excel(output_file, index=False)
            print(f"Successfully merged data and saved to {output_file}")
            
        except Exception as e:
            print(f"Error merging with Excel: {str(e)}")
            import traceback
            traceback.print_exc()

    async def wait_for_excel_download(self) -> Optional[str]:
        """Wait for Excel file to be downloaded after user clicks the export button"""
        try:
            console.print("[yellow]Please set your search criteria and click the Excel export button when ready...[/yellow]")
            
            # Create downloads directory if it doesn't exist
            downloads_dir = Path("downloads")
            downloads_dir.mkdir(exist_ok=True)
            
            # Wait for download to start
            async with self.sessions[0].page.expect_download() as download_info:
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
    try:
        exporter = TelekomExporter()
        
        # Initialize sessions and get Excel file
        console.print("[yellow]Initializing browser sessions and downloading Excel file...[/yellow]")
        excel_file = await exporter.init_sessions()
        console.print(f"[green]Successfully downloaded Excel file: {excel_file}[/green]")
        
        # Get order IDs from Excel
        order_ids = await exporter.get_order_ids_from_excel(excel_file)
        if not order_ids:
            console.print("[red]No order IDs found in Excel file[/red]")
            return
            
        console.print(f"[green]Successfully retrieved {len(order_ids)} order IDs[/green]")
        console.print(f"First few order IDs: {order_ids[:5]}")
        
        # Process orders in parallel
        await exporter.process_orders_parallel(order_ids)
        
        # Generate timestamp for filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save extracted data
        csv_file = f"telekom_orders_{timestamp}.csv"
        exporter.save_to_csv(csv_file)
        console.print(f"[green]Saved data to {csv_file}[/green]")
        
        # Merge with original Excel
        merged_file = f"telekom_orders_merged_{timestamp}.xlsx"
        exporter.merge_with_excel(excel_file, merged_file)
        console.print(f"[green]Merged data saved to {merged_file}[/green]")
        
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
