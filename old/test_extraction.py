#!/usr/bin/env python3
"""
Test IBT Property Extraction System
This script runs a small test extraction to verify that the system is working correctly.
"""

import asyncio
import argparse
import logging
import traceback
import signal
from pathlib import Path

from rich.console import Console

# Import from our multi-session extractor
from multi_session_extractor import (
    PropertyExtractionCoordinator,
    setup_logging,
    signal_handler
)

# Global variables
shutdown_requested = False  # Used for graceful shutdown
console = Console()

class ExtractionTester:
    """Handles testing the extraction system with a small sample"""
    
    def __init__(self, excel_file: str, num_properties: int = 5, num_sessions: int = 2, headless: bool = False, db_path: str = "test_extraction.db"):
        """Initialize the tester"""
        self.excel_file = excel_file
        self.num_properties = num_properties
        self.num_sessions = num_sessions
        self.headless = headless
        self.db_path = db_path
        self.coordinator = PropertyExtractionCoordinator(
            num_sessions=num_sessions,
            headless=headless,
            db_path=db_path
        )
        
    def get_sample_property_ids(self) -> list:
        """Get a sample of property IDs from the Excel file"""
        try:
            logging.info(f"Tester: Getting sample of {self.num_properties} property IDs from {self.excel_file}")
            
            # Check if Excel file exists
            if not Path(self.excel_file).exists():
                logging.error(f"Tester: Excel file not found: {self.excel_file}")
                return []
                
            # Extract all property IDs from Excel
            all_property_ids = self.coordinator.extract_ids_from_excel(self.excel_file)
            if not all_property_ids:
                logging.error("Tester: Failed to extract property IDs from Excel")
                return []
                
            # Take a sample of property IDs
            sample_size = min(self.num_properties, len(all_property_ids))
            sample_property_ids = all_property_ids[:sample_size]
            
            logging.info(f"Tester: Got sample of {len(sample_property_ids)} property IDs")
            return sample_property_ids
            
        except Exception as e:
            logging.error(f"Tester: Error getting sample property IDs: {str(e)}")
            return []
        
    async def run(self):
        """Run the test extraction"""
        try:
            logging.info("Tester: Starting test extraction")
            
            # Get sample property IDs
            property_ids = self.get_sample_property_ids()
            if not property_ids:
                logging.error("Tester: Failed to get sample property IDs. Exiting.")
                return False
                
            # Initialize database with properties
            self.coordinator.db.initialize_properties(property_ids)
            
            # Start monitor task
            self.coordinator.monitor_task = asyncio.create_task(self.coordinator.monitor_workers())
            
            # Start worker sessions
            await self.coordinator.start_workers()
            
            # Wait for all workers to complete
            await asyncio.gather(*self.coordinator.worker_tasks, return_exceptions=True)
            
            # Wait for monitor to complete
            if self.coordinator.monitor_task:
                await self.coordinator.monitor_task
                
            logging.info("Tester: Test extraction completed")
            return True
            
        except Exception as e:
            logging.error(f"Tester: Error in run: {str(e)}")
            logging.error(traceback.format_exc())
            return False
        finally:
            await self.coordinator.close()

async def main():
    """Main entry point for the script"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test IBT Property Extraction System")
    parser.add_argument("--excel-file", required=True, help="Path to the Excel file with property IDs")
    parser.add_argument("--num-properties", type=int, default=5, help="Number of properties to test with (default: 5)")
    parser.add_argument("--sessions", type=int, default=2, help="Number of parallel browser sessions (default: 2)")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument("--db-path", default="test_extraction.db", help="Path to the SQLite database file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--quiet", action="store_true", help="Suppress non-essential output")
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(debug=args.debug, quiet=args.quiet)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Create tester
        tester = ExtractionTester(
            excel_file=args.excel_file,
            num_properties=args.num_properties,
            num_sessions=args.sessions,
            headless=args.headless,
            db_path=args.db_path
        )
        
        # Run test extraction
        console.print("[bold cyan]Starting test extraction...[/bold cyan]")
        console.print(f"Excel file: {args.excel_file}")
        console.print(f"Number of properties: {args.num_properties}")
        console.print(f"Number of sessions: {args.sessions}")
        console.print(f"Headless mode: {'Yes' if args.headless else 'No'}")
        console.print(f"Database path: {args.db_path}")
        
        success = await tester.run()
        
        if success:
            console.print("[bold green]Test extraction completed successfully![/bold green]")
            console.print("You can now run the full extraction with multi_session_extractor.py")
        else:
            console.print("[bold red]Test extraction failed![/bold red]")
            console.print("Please check the logs for details")
            
    except Exception as e:
        console.print(f"[bold red]Error: {str(e)}[/bold red]")
        logging.error(f"Error in main: {str(e)}")
        logging.error(traceback.format_exc())
        return 1
        
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
