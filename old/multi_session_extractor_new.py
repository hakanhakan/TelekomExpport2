#!/usr/bin/env python3
"""
Multi-Session IBT Property Extraction System
This module provides functionality for extracting property information from the Telekom IBT portal
using multiple browser sessions in parallel for improved performance and resilience.
"""

import asyncio
import argparse
import logging
import traceback
import signal
import sqlite3
import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import random

from playwright.async_api import async_playwright, Browser, Page
from rich.console import Console
from rich.table import Table
from dotenv import load_dotenv

# Import the existing IBT property search module
from ibt_property_search import (
    IBTPropertySearchSession, 
    PropertyData, 
    setup_logging
)

# Global variables
shutdown_requested = False  # Used for graceful shutdown
console = Console()

class DatabaseManager:
    """Manages the SQLite database for checkpointing extraction progress"""
    
    def __init__(self, db_path: str):
        """Initialize the database manager"""
        self.db_path = db_path
        self.conn = None
        self.initialize_db()
        
    def initialize_db(self):
        """Create the database and tables if they don't exist"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            cursor = self.conn.cursor()
            
            # Create properties table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS properties (
                property_id TEXT PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                session_id INTEGER DEFAULT NULL,
                attempts INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error_message TEXT DEFAULT NULL,
                address TEXT DEFAULT NULL,
                postal_code TEXT DEFAULT NULL,
                city TEXT DEFAULT NULL,
                property_status TEXT DEFAULT NULL,
                owner_name TEXT DEFAULT NULL,
                owner_email TEXT DEFAULT NULL,
                owner_mobile TEXT DEFAULT NULL,
                owner_phone TEXT DEFAULT NULL,
                is_decision_maker INTEGER DEFAULT 0,
                owner_details_loaded INTEGER DEFAULT 0,
                additional_fields TEXT DEFAULT NULL
            )
            ''')
            
            # Create index on status for faster queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON properties(status)')
            
            # Create sessions table to track active sessions
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                session_id INTEGER PRIMARY KEY,
                status TEXT DEFAULT 'inactive',
                properties_processed INTEGER DEFAULT 0,
                properties_failed INTEGER DEFAULT 0,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            self.conn.commit()
            logging.info(f"Database initialized at {self.db_path}")
        except Exception as e:
            logging.error(f"Error initializing database: {str(e)}")
            raise
            
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            
    def initialize_properties(self, property_ids: List[str]):
        """Initialize the database with property IDs"""
        try:
            cursor = self.conn.cursor()
            
            # Check which property IDs already exist
            cursor.execute('SELECT property_id FROM properties')
            existing_ids = {row[0] for row in cursor.fetchall()}
            
            # Insert only new property IDs
            new_ids = [id for id in property_ids if id not in existing_ids]
            if new_ids:
                cursor.executemany(
                    'INSERT INTO properties (property_id, status) VALUES (?, ?)',
                    [(id, 'pending') for id in new_ids]
                )
                self.conn.commit()
                
            logging.info(f"Initialized {len(new_ids)} new properties in database")
            return len(new_ids)
        except Exception as e:
            logging.error(f"Error initializing properties: {str(e)}")
            self.conn.rollback()
            raise
            
    def register_session(self, session_id: int):
        """Register a session in the database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                'INSERT OR REPLACE INTO sessions (session_id, status, last_active) VALUES (?, ?, CURRENT_TIMESTAMP)',
                (session_id, 'active')
            )
            self.conn.commit()
            logging.info(f"Registered session {session_id}")
        except Exception as e:
            logging.error(f"Error registering session {session_id}: {str(e)}")
            self.conn.rollback()
            
    def update_session_status(self, session_id: int, status: str):
        """Update a session's status"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                'UPDATE sessions SET status = ?, last_active = CURRENT_TIMESTAMP WHERE session_id = ?',
                (status, session_id)
            )
            self.conn.commit()
            logging.info(f"Updated session {session_id} status to {status}")
        except Exception as e:
            logging.error(f"Error updating session {session_id} status: {str(e)}")
            self.conn.rollback()
            
    def increment_session_counter(self, session_id: int, counter_name: str):
        """Increment a session's counter (processed or failed)"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                f'UPDATE sessions SET {counter_name} = {counter_name} + 1, last_active = CURRENT_TIMESTAMP WHERE session_id = ?',
                (session_id,)
            )
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error incrementing session {session_id} counter {counter_name}: {str(e)}")
            self.conn.rollback()
            
    def get_next_batch(self, session_id: int, batch_size: int = 10) -> List[str]:
        """Get the next batch of pending properties for a session"""
        try:
            cursor = self.conn.cursor()
            
            # First, check for any properties that were assigned to this session but not completed
            cursor.execute(
                'SELECT property_id FROM properties WHERE session_id = ? AND status = "in_progress" LIMIT ?',
                (session_id, batch_size)
            )
            batch = [row[0] for row in cursor.fetchall()]
            
            # If we didn't get a full batch, get more pending properties
            if len(batch) < batch_size:
                remaining = batch_size - len(batch)
                cursor.execute(
                    'SELECT property_id FROM properties WHERE status = "pending" LIMIT ?',
                    (remaining,)
                )
                batch.extend([row[0] for row in cursor.fetchall()])
                
            return batch
        except Exception as e:
            logging.error(f"Error getting next batch for session {session_id}: {str(e)}")
            return []
            
    def mark_in_progress(self, property_id: str, session_id: int):
        """Mark a property as in-progress"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                'UPDATE properties SET status = "in_progress", session_id = ?, last_updated = CURRENT_TIMESTAMP WHERE property_id = ?',
                (session_id, property_id)
            )
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error marking property {property_id} as in-progress: {str(e)}")
            self.conn.rollback()
            
    def mark_completed(self, property_id: str, property_data: PropertyData):
        """Mark a property as completed and save its details"""
        try:
            cursor = self.conn.cursor()
            
            # Convert additional_fields to JSON string
            additional_fields_json = json.dumps(property_data.additional_fields) if property_data.additional_fields else None
            
            cursor.execute(
                '''
                UPDATE properties SET 
                    status = "completed", 
                    last_updated = CURRENT_TIMESTAMP,
                    address = ?,
                    postal_code = ?,
                    city = ?,
                    property_status = ?,
                    owner_name = ?,
                    owner_email = ?,
                    owner_mobile = ?,
                    owner_phone = ?,
                    is_decision_maker = ?,
                    owner_details_loaded = ?,
                    additional_fields = ?
                WHERE property_id = ?
                ''',
                (
                    property_data.address,
                    property_data.postal_code,
                    property_data.city,
                    property_data.status,
                    property_data.owner_name,
                    property_data.owner_email,
                    property_data.owner_mobile,
                    property_data.owner_phone,
                    1 if property_data.is_decision_maker else 0,
                    1 if property_data.owner_details_loaded else 0,
                    additional_fields_json,
                    property_id
                )
            )
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error marking property {property_id} as completed: {str(e)}")
            self.conn.rollback()
            
    def mark_failed(self, property_id: str, error_message: str):
        """Mark a property as failed"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                'UPDATE properties SET status = "failed", error_message = ?, attempts = attempts + 1, last_updated = CURRENT_TIMESTAMP WHERE property_id = ?',
                (error_message, property_id)
            )
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error marking property {property_id} as failed: {str(e)}")
            self.conn.rollback()
            
    def reset_stalled_properties(self, max_age_minutes: int = 30):
        """Reset properties that have been in-progress for too long"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                UPDATE properties 
                SET status = "pending", session_id = NULL 
                WHERE status = "in_progress" 
                AND datetime(last_updated, '+' || ? || ' minutes') < datetime('now')
                ''',
                (max_age_minutes,)
            )
            reset_count = cursor.rowcount
            self.conn.commit()
            if reset_count > 0:
                logging.info(f"Reset {reset_count} stalled properties")
            return reset_count
        except Exception as e:
            logging.error(f"Error resetting stalled properties: {str(e)}")
            self.conn.rollback()
            return 0
            
    def get_progress_stats(self) -> Dict:
        """Get progress statistics"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                FROM properties
                '''
            )
            row = cursor.fetchone()
            
            # Get session stats
            cursor.execute(
                '''
                SELECT 
                    session_id,
                    status,
                    properties_processed,
                    properties_failed,
                    datetime(last_active) as last_active
                FROM sessions
                '''
            )
            sessions = [
                {
                    'session_id': row[0],
                    'status': row[1],
                    'properties_processed': row[2],
                    'properties_failed': row[3],
                    'last_active': row[4]
                }
                for row in cursor.fetchall()
            ]
            
            return {
                'total': row[0],
                'pending': row[1],
                'in_progress': row[2],
                'completed': row[3],
                'failed': row[4],
                'sessions': sessions
            }
        except Exception as e:
            logging.error(f"Error getting progress stats: {str(e)}")
            return {
                'total': 0,
                'pending': 0,
                'in_progress': 0,
                'completed': 0,
                'failed': 0,
                'sessions': []
            }
            
    def export_to_excel(self, output_path: str) -> bool:
        """Export all completed properties to an Excel file"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                SELECT 
                    property_id,
                    address,
                    postal_code,
                    city,
                    property_status,
                    owner_name,
                    owner_email,
                    owner_mobile,
                    owner_phone,
                    is_decision_maker,
                    owner_details_loaded,
                    additional_fields
                FROM properties
                WHERE status = 'completed'
                '''
            )
            
            # Convert to list of dictionaries
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            data = []
            
            for row in rows:
                row_dict = dict(zip(columns, row))
                
                # Parse additional_fields JSON if it exists
                if row_dict['additional_fields']:
                    try:
                        additional_fields = json.loads(row_dict['additional_fields'])
                        # Flatten additional_fields for Excel
                        for key, value in additional_fields.items():
                            row_dict[f"additional_{key}"] = value
                    except Exception:
                        pass
                        
                # Convert boolean integers to strings for better readability
                row_dict['is_decision_maker'] = 'Yes' if row_dict['is_decision_maker'] == 1 else 'No'
                row_dict['owner_details_loaded'] = 'Yes' if row_dict['owner_details_loaded'] == 1 else 'No'
                
                data.append(row_dict)
                
            # Create DataFrame and export to Excel
            if data:
                df = pd.DataFrame(data)
                df.to_excel(output_path, index=False)
                logging.info(f"Exported {len(data)} properties to {output_path}")
                return True
            else:
                logging.warning("No completed properties to export")
                return False
                
        except Exception as e:
            logging.error(f"Error exporting to Excel: {str(e)}")
            return False
            
    def export_to_csv(self, output_path: str) -> bool:
        """Export all completed properties to a CSV file"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                SELECT 
                    property_id,
                    address,
                    postal_code,
                    city,
                    property_status,
                    owner_name,
                    owner_email,
                    owner_mobile,
                    owner_phone,
                    is_decision_maker,
                    owner_details_loaded
                FROM properties
                WHERE status = 'completed'
                '''
            )
            
            # Convert to list of dictionaries
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            data = []
            
            for row in rows:
                row_dict = dict(zip(columns, row))
                
                # Convert boolean integers to strings for better readability
                row_dict['is_decision_maker'] = 'Yes' if row_dict['is_decision_maker'] == 1 else 'No'
                row_dict['owner_details_loaded'] = 'Yes' if row_dict['owner_details_loaded'] == 1 else 'No'
                
                data.append(row_dict)
                
            # Create DataFrame and export to CSV
            if data:
                df = pd.DataFrame(data)
                df.to_csv(output_path, index=False)
                logging.info(f"Exported {len(data)} properties to {output_path}")
                return True
            else:
                logging.warning("No completed properties to export")
                return False
                
        except Exception as e:
            logging.error(f"Error exporting to CSV: {str(e)}")
            return False

class PropertyExtractionWorker:
    """Worker class that handles property extraction in a single browser session"""
    
    def __init__(self, session_id: int, db_manager: DatabaseManager, headless: bool = True):
        """Initialize the worker"""
        # Load environment variables
        load_dotenv()
        self.session_id = session_id
        self.db = db_manager
        self.headless = headless
        self.username = os.getenv("TELEKOM_USERNAME")
        self.password = os.getenv("TELEKOM_PASSWORD")
        self.otp_secret = os.getenv("TELEKOM_OTP_SECRET")
        
        # Check if credentials are available
        if not self.username or not self.password:
            logging.error(f"Worker {self.session_id}: Missing credentials. Please check your .env file.")
            
        self.session = None
        self.playwright = None
        self.running = False
        self.properties_processed = 0
        self.properties_failed = 0
        
    async def init(self) -> bool:
        """Initialize the worker session"""
        try:
            # Check if credentials are available
            if not self.username or not self.password:
                logging.error(f"Worker {self.session_id}: Missing credentials. Cannot initialize.")
                return False
                
            self.playwright = await async_playwright().start()
            self.session = IBTPropertySearchSession(
                username=self.username,
                password=self.password,
                session_id=self.session_id,
                headless=self.headless
            )
            if self.otp_secret:
                self.session.otp_secret = self.otp_secret
                
            logging.info(f"Worker {self.session_id}: Using credentials - Username: {self.username}, Password: {'*' * len(self.password) if self.password else 'None'}, OTP Secret: {'Available' if self.otp_secret else 'None'}")
                
            await self.session.init_browser()
            self.db.register_session(self.session_id)
            logging.info(f"Worker {self.session_id}: Initialized")
            return True
        except Exception as e:
            logging.error(f"Worker {self.session_id}: Error initializing: {str(e)}")
            return False
            
    async def login(self) -> bool:
        """Login to the IBT portal"""
        try:
            if not self.session:
                logging.error(f"Worker {self.session_id}: Session not initialized")
                return False
                
            login_success = await self.session.login()
            if login_success:
                logging.info(f"Worker {self.session_id}: Login successful")
                return True
            else:
                logging.error(f"Worker {self.session_id}: Login failed")
                return False
        except Exception as e:
            logging.error(f"Worker {self.session_id}: Error during login: {str(e)}")
            return False
            
    async def process_property(self, property_id: str) -> bool:
        """Process a single property"""
        try:
            logging.info(f"Worker {self.session_id}: Processing property {property_id}")
            
            # Mark property as in-progress
            self.db.mark_in_progress(property_id, self.session_id)
            
            # Create a basic PropertyData object
            property_data = PropertyData(property_id=property_id)
            
            # Navigate to property details page
            details_opened = await self.session.open_property_details(property_id)
            if not details_opened:
                error_msg = f"Failed to open property details for ID {property_id}"
                logging.error(f"Worker {self.session_id}: {error_msg}")
                self.db.mark_failed(property_id, error_msg)
                self.properties_failed += 1
                self.db.increment_session_counter(self.session_id, "properties_failed")
                return False
            
            # Extract owner information
            property_data = await self.session.get_owner_information(property_data)
            
            # Mark property as completed
            self.db.mark_completed(property_id, property_data)
            self.properties_processed += 1
            self.db.increment_session_counter(self.session_id, "properties_processed")
            
            logging.info(f"Worker {self.session_id}: Successfully processed property {property_id}")
            return True
            
        except Exception as e:
            error_msg = f"Error processing property {property_id}: {str(e)}"
            logging.error(f"Worker {self.session_id}: {error_msg}")
            self.db.mark_failed(property_id, error_msg)
            self.properties_failed += 1
            self.db.increment_session_counter(self.session_id, "properties_failed")
            return False
            
    async def run(self):
        """Run the worker session"""
        try:
            global shutdown_requested
            self.running = True
            
            # Initialize and login
            init_success = await self.init()
            if not init_success:
                logging.error(f"Worker {self.session_id}: Failed to initialize")
                self.running = False
                return
                
            login_success = await self.login()
            if not login_success:
                logging.error(f"Worker {self.session_id}: Failed to login")
                self.running = False
                return
                
            # Navigate to search page
            await self.session.page.goto(self.session.search_url)
            await self.session.page.wait_for_timeout(5000)
            
            # Main processing loop
            batch_size = 5  # Process properties in small batches
            
            while self.running and not shutdown_requested:
                # Get next batch of properties
                batch = self.db.get_next_batch(self.session_id, batch_size)
                
                if not batch:
                    logging.info(f"Worker {self.session_id}: No more properties to process")
                    await asyncio.sleep(5)  # Wait a bit before checking again
                    continue
                    
                logging.info(f"Worker {self.session_id}: Got batch of {len(batch)} properties")
                
                # Process each property in the batch
                for property_id in batch:
                    if not self.running or shutdown_requested:
                        break
                        
                    await self.process_property(property_id)
                    
                    # Add a small delay between properties to avoid overwhelming the server
                    await asyncio.sleep(random.uniform(1, 3))
                    
                # Update session status
                self.db.update_session_status(self.session_id, "active")
                
                # Add a small delay between batches
                await asyncio.sleep(random.uniform(2, 5))
                
            logging.info(f"Worker {self.session_id}: Finished processing. Processed: {self.properties_processed}, Failed: {self.properties_failed}")
            
        except Exception as e:
            logging.error(f"Worker {self.session_id}: Error in run loop: {str(e)}")
            logging.error(traceback.format_exc())
        finally:
            self.running = False
            self.db.update_session_status(self.session_id, "inactive")
            await self.close()
            
    async def close(self):
        """Close the worker session"""
        try:
            if self.session:
                await self.session.close()
                self.session = None
                
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
                
            logging.info(f"Worker {self.session_id}: Closed")
        except Exception as e:
            logging.error(f"Worker {self.session_id}: Error closing: {str(e)}")

class PropertyExtractionCoordinator:
    """Coordinates multiple worker sessions for property extraction"""
    
    def __init__(self, num_sessions: int = 4, headless: bool = True, db_path: str = "property_extraction.db"):
        """Initialize the coordinator"""
        self.num_sessions = num_sessions
        self.headless = headless
        self.db_path = db_path
        self.db = DatabaseManager(db_path)
        self.workers = []
        self.worker_tasks = []
        self.monitor_task = None
        self.excel_file = None
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)
        
    async def download_excel(self, area: str) -> Optional[str]:
        """Download the Excel file with property data"""
        try:
            logging.info(f"Coordinator: Downloading Excel file for area: {area}")
            
            # Create a single session to download the Excel file
            playwright = await async_playwright().start()
            try:
                # Load environment variables
                load_dotenv()
                username = os.getenv("TELEKOM_USERNAME")
                password = os.getenv("TELEKOM_PASSWORD")
                otp_secret = os.getenv("TELEKOM_OTP_SECRET")
                
                # Check if credentials are available
                if not username or not password:
                    logging.error("Coordinator: Missing credentials. Please check your .env file.")
                    return None
                    
                # Use the original class to handle login and download
                session = IBTPropertySearchSession(
                    username=username,
                    password=password,
                    session_id=0,
                    headless=self.headless
                )
                
                # Set OTP secret if available
                if otp_secret:
                    session.otp_secret = otp_secret
                    
                logging.info(f"Coordinator: Using credentials - Username: {username}, Password: {'*' * len(password) if password else 'None'}, OTP Secret: {'Available' if otp_secret else 'None'}")
                
                # Initialize browser  
                await session.init_browser()
                
                # Login
                login_success = await session.login()
                if not login_success:
                    logging.error("Coordinator: Failed to login for Excel download")
                    await session.close()
                    return None
                    
                # Search by area
                search_success = await session.search_by_area(area)
                if not search_success:
                    logging.error(f"Coordinator: Failed to search for area: {area}")
                    await session.close()
                    return None
                    
                # Download Excel file
                excel_file = await session.download_search_results_excel()
                if not excel_file:
                    logging.error("Coordinator: Failed to download Excel file")
                    await session.close()
                    return None
                    
                logging.info(f"Coordinator: Successfully downloaded Excel file: {excel_file}")
                
                # Close the session
                await session.close()
                
                return excel_file
            except Exception as e:
                logging.error(f"Coordinator: Error in download session: {str(e)}")
                return None
            finally:
                await playwright.stop()
                
        except Exception as e:
            logging.error(f"Coordinator: Error downloading Excel file: {str(e)}")
            return None
            
    def extract_ids_from_excel(self, excel_file: str) -> List[str]:
        """Extract property IDs from the Excel file"""
        try:
            logging.info(f"Coordinator: Extracting property IDs from Excel file: {excel_file}")
            
            # Read the Excel file
            df = pd.read_excel(excel_file)
            
            # Look for property ID column
            # The column name might vary, so we'll try a few common possibilities
            id_column = None
            possible_columns = ['property_id', 'Property ID', 'PropertyID', 'ID', 'FOL ID', 'FOL-ID']
            
            for col in possible_columns:
                if col in df.columns:
                    id_column = col
                    break
                    
            if not id_column:
                # If we couldn't find a column with an exact match, look for columns containing 'id'
                for col in df.columns:
                    if 'id' in col.lower():
                        id_column = col
                        break
                        
            if not id_column:
                # If we still couldn't find a column, use the first column
                id_column = df.columns[0]
                
            logging.info(f"Coordinator: Using column '{id_column}' for property IDs")
            
            # Extract property IDs
            property_ids = df[id_column].astype(str).tolist()
            
            # Remove any empty or NaN values
            property_ids = [id for id in property_ids if id and id.lower() != 'nan']
            
            logging.info(f"Coordinator: Extracted {len(property_ids)} property IDs")
            
            return property_ids
            
        except Exception as e:
            logging.error(f"Coordinator: Error extracting property IDs from Excel: {str(e)}")
            return []
            
    async def start_workers(self):
        """Start the worker sessions"""
        try:
            logging.info(f"Coordinator: Starting {self.num_sessions} worker sessions")
            
            for i in range(self.num_sessions):
                worker = PropertyExtractionWorker(
                    session_id=i,
                    db_manager=self.db,
                    headless=self.headless
                )
                self.workers.append(worker)
                
                # Create task for worker
                task = asyncio.create_task(worker.run())
                self.worker_tasks.append(task)
                
                # Add a small delay between starting workers to avoid overwhelming the system
                await asyncio.sleep(5)
                
            logging.info("Coordinator: All worker sessions started")
            
        except Exception as e:
            logging.error(f"Coordinator: Error starting workers: {str(e)}")
            
    async def monitor_workers(self):
        """Monitor worker sessions and handle failures"""
        global shutdown_requested
        try:
            logging.info("Coordinator: Starting worker monitor")
            
            while not shutdown_requested:
                # Get progress stats
                stats = self.db.get_progress_stats()
                
                # Display progress
                console.print("\n[bold cyan]Extraction Progress:[/bold cyan]")
                console.print(f"Total: {stats['total']} | Pending: {stats['pending']} | In Progress: {stats['in_progress']} | Completed: {stats['completed']} | Failed: {stats['failed']}")
                
                # Display session stats
                session_table = Table(title="Worker Sessions")
                session_table.add_column("Session ID")
                session_table.add_column("Status")
                session_table.add_column("Processed")
                session_table.add_column("Failed")
                session_table.add_column("Last Active")
                
                for session in stats['sessions']:
                    session_table.add_row(
                        str(session['session_id']),
                        session['status'],
                        str(session['properties_processed']),
                        str(session['properties_failed']),
                        session['last_active']
                    )
                    
                console.print(session_table)
                
                # Reset stalled properties
                self.db.reset_stalled_properties(max_age_minutes=30)
                
                # Check if all properties are completed or failed
                if stats
