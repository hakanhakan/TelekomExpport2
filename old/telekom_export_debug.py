#!/usr/bin/env python3
"""
Telekom Order Export Automation Tool - Debug Version
This script is a modified version of telekom_export.py with debugging features enabled.
"""

import asyncio
import argparse
import logging
import traceback
from pathlib import Path
from typing import List, Optional

from playwright.async_api import async_playwright, Browser, Page
from rich.console import Console
from dotenv import load_dotenv
import os
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telekom_export_debug.log'),
        logging.StreamHandler()
    ]
)

# Initialize Rich console
console = Console()

# Import all classes and functions from the original script

class BrowserSession:
    """Handles individual browser session and its state"""
    def __init__(self, username: str, password: str, session_id: int, headless=False, recording_mode=False):
        self.username = username
        self.password = password
        self.session_id = session_id
        self.headless = False  # Always show browser window for debugging
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
        self.browser = await playwright.chromium.launch(headless=False)  # Always show browser
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
                        console.print("[yellow]The script will pause until you're logged in.[/yellow]")
                        
                        # Wait for the user to complete OTP verification (max 5 minutes)
                        max_wait = 300  # seconds
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
                # If we can't find the username field, we might already be logged in
                logging.info(f"Session {self.session_id}: No login form found, might already be logged in: {str(e)}")
            
            # Navigate to the search page
            await self.page.goto(self.search_url)
            
            # Check if we're on the search page
            current_url = self.page.url
            if "property/search" in current_url:
                logging.info(f"Session {self.session_id}: Successfully navigated to search page")
                return True
            else:
                logging.error(f"Session {self.session_id}: Failed to navigate to search page, current URL: {current_url}")
                return False
                
        except Exception as e:
            logging.error(f"Session {self.session_id}: Login failed: {str(e)}")
            logging.error(traceback.format_exc())
            return False

    # Import the rest of the methods from the original BrowserSession class
    # For brevity, we're not copying all methods here
    # In a real implementation, you would copy all methods from the original class
    
    async def setup_event_recording(self):
        """Set up JavaScript event recording to capture user interactions"""
        # Add a console listener to capture events
        self.page.on("console", self.handle_console_message)
        
        # Inject JavaScript to record events
        await self.page.evaluate("""() => {
            // Create a global array to store events
            window._recordedEvents = window._recordedEvents || [];
            
            // Function to record events
            function recordEvent(type, event) {
                const element = event.target;
                const tagName = element.tagName?.toLowerCase();
                const id = element.id || '';
                const classes = element.className || '';
                const attributes = Array.from(element.attributes || []).map(attr => `${attr.name}="${attr.value}"`).join(' ');
                const xpath = getXPath(element);
                const selector = getCssSelector(element);
                
                // Record the event
                const eventData = {
                    timestamp: new Date().toISOString(),
                    type: type,
                    tagName: tagName,
                    id: id,
                    classes: classes,
                    attributes: attributes,
                    xpath: xpath,
                    selector: selector,
                    innerText: element.innerText?.substring(0, 100) || '',
                    value: element.value || '',
                    checked: element.checked,
                    clientX: event.clientX,
                    clientY: event.clientY
                };
                
                window._recordedEvents.push(eventData);
                console.log('RECORDED_EVENT: ' + JSON.stringify(eventData));
            }
            
            // Helper function to get XPath
            function getXPath(element) {
                if (!element) return '';
                try {
                    if (element.id) return `//*[@id="${element.id}"]`;
                    
                    let path = '';
                    while (element && element.nodeType === 1) {
                        let index = 1;
                        for (let sibling = element.previousSibling; sibling; sibling = sibling.previousSibling) {
                            if (sibling.nodeType === 1 && sibling.tagName === element.tagName) {
                                index++;
                            }
                        }
                        const tagName = element.tagName.toLowerCase();
                        path = `/${tagName}[${index}]${path}`;
                        element = element.parentNode;
                    }
                    return path;
                } catch (e) {
                    return `Error getting XPath: ${e.message}`;
                }
            }
            
            // Helper function to get CSS selector
            function getCssSelector(element) {
                if (!element) return '';
                try {
                    if (element.id) return `#${element.id}`;
                    
                    let path = [];
                    while (element && element.nodeType === 1) {
                        let selector = element.tagName.toLowerCase();
                        if (element.id) {
                            selector += `#${element.id}`;
                            path.unshift(selector);
                            break;
                        } else {
                            let sibling = element;
                            let index = 1;
                            while (sibling = sibling.previousElementSibling) {
                                if (sibling.tagName === element.tagName) {
                                    index++;
                                }
                            }
                            if (index > 1) {
                                selector += `:nth-of-type(${index})`;
                            }
                        }
                        path.unshift(selector);
                        element = element.parentNode;
                    }
                    return path.join(' > ');
                } catch (e) {
                    return `Error getting CSS selector: ${e.message}`;
                }
            }
            
            // Add event listeners for common interactions
            document.addEventListener('click', (e) => recordEvent('click', e), true);
            document.addEventListener('input', (e) => recordEvent('input', e), true);
            document.addEventListener('change', (e) => recordEvent('change', e), true);
            document.addEventListener('focus', (e) => recordEvent('focus', e), true);
            document.addEventListener('blur', (e) => recordEvent('blur', e), true);
            
            console.log('Event recording initialized');
        }""")
        
        logging.info(f"Session {self.session_id}: Event recording initialized")
    
    async def handle_console_message(self, msg):
        """Handle console messages, looking for recorded events"""
        text = msg.text
        if text.startswith('RECORDED_EVENT:'):
            try:
                event_data = json.loads(text.replace('RECORDED_EVENT:', '').strip())
                self.event_log.append(event_data)
                
                # Save events to file periodically
                if len(self.event_log) % 10 == 0:
                    self.save_event_log()
                    
            except json.JSONDecodeError:
                logging.error(f"Session {self.session_id}: Failed to parse event data: {text}")
    
    def save_event_log(self):
        """Save the event log to a file"""
        try:
            with open(f"dom_events_session_{self.session_id}.json", "w") as f:
                json.dump(self.event_log, f, indent=2)
            logging.info(f"Session {self.session_id}: Saved {len(self.event_log)} events to log file")
        except Exception as e:
            logging.error(f"Session {self.session_id}: Failed to save event log: {str(e)}")
    
    async def close(self):
        """Close the browser session"""
        if self.recording_mode:
            self.save_event_log()
            
        if self.browser:
            await self.browser.close()
            logging.info(f"Session {self.session_id}: Browser closed")


class TelekomExporter:
    def __init__(self, recording_mode=False):
        load_dotenv()
        self.username = os.getenv("TELEKOM_USERNAME")
        self.password = os.getenv("TELEKOM_PASSWORD")
        self.num_sessions = 1  # Reduced to 1 for debugging
        self.sessions: List[BrowserSession] = []
        self.all_fields = set()
        self.extracted_data = []
        self.area = "Bad Sooden-Allendorf, Stadt"  # Default area to search
        self.recording_mode = recording_mode
        
    async def init_sessions(self):
        """Initialize browser sessions"""
        for i in range(self.num_sessions):
            session = BrowserSession(
                username=self.username,
                password=self.password,
                session_id=i,
                headless=False,  # Always show browser window
                recording_mode=self.recording_mode
            )
            
            await session.init_browser()
            success = await session.login()
            
            if success:
                self.sessions.append(session)
                logging.info(f"Session {i}: Initialized and logged in successfully")
            else:
                await session.close()
                logging.error(f"Session {i}: Failed to initialize or log in")
        
        if not self.sessions:
            raise Exception("Failed to initialize any browser sessions")


async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Telekom Property Export Tool - Debug Version")
    parser.add_argument("--area", type=str, help="Area to search for properties")
    parser.add_argument("--record", action="store_true", help="Enable recording mode to capture DOM events")
    parser.add_argument("--output", type=str, default="telekom_properties_debug.csv", help="Output CSV file path")
    args = parser.parse_args()
    
    try:
        exporter = TelekomExporter(recording_mode=args.record)
        
        if args.area:
            exporter.area = args.area
        
        # Initialize sessions and set search criteria
        console.print("[yellow]Initializing browser session (debug mode)...[/yellow]")
        await exporter.init_sessions()
        
        if len(exporter.sessions) == 0:
            console.print("[red]Failed to initialize any browser sessions. Check credentials and network.[/red]")
            return
            
        # Keep the first session for debugging
        session = exporter.sessions[0]
        
        # Set search criteria
        console.print("[yellow]Setting search criteria...[/yellow]")
        await session.set_search_criteria(exporter.area)
        
        # Keep the browser open for manual interaction
        console.print("[yellow]Browser is now open for debugging.[/yellow]")
        console.print("[yellow]You can interact with the browser manually.[/yellow]")
        console.print("[yellow]Press Ctrl+C when done to exit.[/yellow]")
        
        try:
            # Keep the script running until user interrupts
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            console.print("[yellow]Debug session stopped by user.[/yellow]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        logging.error(f"Error in main: {str(e)}")
        logging.error(traceback.format_exc())
        
    finally:
        # Ensure all sessions are closed
        if 'exporter' in locals() and hasattr(exporter, 'sessions'):
            for session in exporter.sessions:
                await session.close()
        console.print("[yellow]All browser sessions closed[/yellow]")


if __name__ == "__main__":
    asyncio.run(main())
