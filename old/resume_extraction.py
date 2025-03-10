#!/usr/bin/env python3
"""
Resume IBT Property Extraction from Excel File
This script allows resuming property extraction from an existing Excel file,
without downloading it again from the IBT portal.
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

class ExtractionResumer:
    """Handles resuming extraction from an existing Excel file"""
    
    def __init__(self, excel_file: str, num_sessions: int = 4, headless: bool = True, db_path: str = "property_extraction.db"):
        """Initialize the resumer"""
        self.excel_file = excel_file
        self.num_sessions = num_sessions
        self.headless = headless
        self.db_path = db_path
        self.coordinator = PropertyExtractionCoordinator(
            num_sessions=num_sessions,
            headless=headless,
            db_path=db_path
        )
        
    async def run(self):
        """Run the extraction process using the existing Excel file"""
        try:
            logging.info(f"Resumer: Starting extraction from Excel file: {self.excel_file}")
            
            # Check if Excel file exists
            if not Path(self.excel_file).exists():
                logging.error(f"Resumer: Excel file not found: {self.excel_file}")
                return False
                
            # Extract property IDs from Excel
            property_ids = self.coordinator.extract_ids_from_excel(self.excel_file)
            if not property_ids:
                logging.error("Resumer: Failed to extract property IDs from Excel. Exiting.")
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
                
            logging.info("Resumer: Extraction process completed")
            return True
            
        except Exception as e:
            logging.error(f"Resumer: Error in run: {str(e)}")
            logging.error(traceback.format_exc())
            return False
        finally:
            await self.coordinator.close()

async def main():
    """Main entry point for the script"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Resume IBT Property Extraction from Excel File")
    parser.add_argument("--excel-file", required=True, help="Path to the Excel file with property IDs")
    parser.add_argument("--sessions", type=int, default=4, help="Number of parallel browser sessions (default: 4)")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument("--db-path", default="property_extraction.db", help="Path to the SQLite database file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--quiet", action="store_true", help="Suppress non-essential output")
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(debug=args.debug, quiet=args.quiet)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Create resumer
        resumer = ExtractionResumer(
            excel_file=args.excel_file,
            num_sessions=args.sessions,
            headless=args.headless,
            db_path=args.db_path
        )
        
        # Run extraction process
        success = await resumer.run()
        
        if success:
            console.print("[bold green]Extraction process completed successfully![/bold green]")
        else:
            console.print("[bold red]Extraction process failed![/bold red]")
            
    except Exception as e:
        console.print(f"[bold red]Error: {str(e)}[/bold red]")
        logging.error(f"Error in main: {str(e)}")
        logging.error(traceback.format_exc())
        return 1
        
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
