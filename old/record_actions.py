#!/usr/bin/env python3
"""
Record user actions on the Telekom portal to capture search criteria setting steps.
"""

import asyncio
import logging
import json
from datetime import datetime
from typing import Optional

from playwright.async_api import async_playwright, Browser, Page
from rich.console import Console
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('recorded_actions.log'),
        logging.StreamHandler()
    ]
)

console = Console()

class ActionRecorder:
    def __init__(self):
        load_dotenv()
        self.base_url = "https://glasfaser.telekom.de/auftragnehmerportal-ui"
        self.search_url = f"{self.base_url}/order/ibtorder/search?a-cid=58222"
        self.username = os.getenv("TELEKOM_USERNAME")
        self.password = os.getenv("TELEKOM_PASSWORD")
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.recording = True
        self.actions = []
        self.enable_tracing = False
        self.network_requests = []

    async def init_browser(self):
        """Initialize browser instance with enhanced monitoring"""
        try:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(headless=False)
            self.context = await self.browser.new_context()
            self.page = await self.context.new_page()
            
            # Monitor network requests
            self.page.on("request", lambda request: asyncio.create_task(self.handle_request(request)))
            self.page.on("response", lambda response: asyncio.create_task(self.handle_response(response)))
            
            # Add enhanced event listeners for dynamic content
            await self.page.evaluate('''() => {
                window._recordedActions = [];
                window._lastMutation = null;
                window._mutationTimeout = null;
                
                function recordAction(type, details) {
                    const timestamp = new Date().toISOString();
                    const action = { timestamp, type, ...details };
                    window._recordedActions.push(action);
                    console.log(JSON.stringify(action));
                }
                
                // Monitor DOM mutations
                const observer = new MutationObserver((mutations) => {
                    clearTimeout(window._mutationTimeout);
                    window._lastMutation = mutations;
                    
                    // Debounce to capture the final state after a batch of changes
                    window._mutationTimeout = setTimeout(() => {
                        const relevantChanges = window._lastMutation.filter(m => {
                            // Filter out style/hidden changes
                            if (m.type === 'attributes' && m.attributeName === 'style') return false;
                            if (m.type === 'attributes' && m.attributeName === 'hidden') return false;
                            return true;
                        });
                        
                        if (relevantChanges.length > 0) {
                            recordAction('dom_change', {
                                changes: relevantChanges.map(m => ({
                                    type: m.type,
                                    target: {
                                        tag: m.target.tagName?.toLowerCase(),
                                        id: m.target.id,
                                        class: m.target.className,
                                        text: m.target.textContent?.trim()
                                    },
                                    addedNodes: Array.from(m.addedNodes).map(n => ({
                                        tag: n.tagName?.toLowerCase(),
                                        id: n.id,
                                        class: n.className,
                                        text: n.textContent?.trim()
                                    }))
                                }))
                            });
                        }
                    }, 500);
                });
                
                observer.observe(document.body, {
                    childList: true,
                    subtree: true,
                    attributes: true,
                    characterData: true
                });
                
                // Monitor XHR/Fetch requests
                const originalFetch = window.fetch;
                window.fetch = async function(...args) {
                    const url = args[0];
                    const options = args[1] || {};
                    
                    recordAction('fetch_request', {
                        url: typeof url === 'string' ? url : url.url,
                        method: options.method || 'GET',
                        headers: options.headers,
                        body: options.body
                    });
                    
                    try {
                        const response = await originalFetch.apply(this, args);
                        const clone = response.clone();
                        const body = await clone.text();
                        
                        recordAction('fetch_response', {
                            url: typeof url === 'string' ? url : url.url,
                            status: response.status,
                            headers: Object.fromEntries(response.headers),
                            body: body.length < 1000 ? body : `${body.substring(0, 1000)}...`
                        });
                        
                        return response;
                    } catch (error) {
                        recordAction('fetch_error', {
                            url: typeof url === 'string' ? url : url.url,
                            error: error.message
                        });
                        throw error;
                    }
                };
                
                // Monitor XHR requests
                const originalXHROpen = XMLHttpRequest.prototype.open;
                const originalXHRSend = XMLHttpRequest.prototype.send;
                
                XMLHttpRequest.prototype.open = function(method, url) {
                    this._requestData = { method, url };
                    return originalXHROpen.apply(this, arguments);
                };
                
                XMLHttpRequest.prototype.send = function(data) {
                    const xhr = this;
                    
                    recordAction('xhr_request', {
                        ...xhr._requestData,
                        body: data
                    });
                    
                    xhr.addEventListener('load', function() {
                        recordAction('xhr_response', {
                            ...xhr._requestData,
                            status: xhr.status,
                            response: xhr.responseText.length < 1000 ? 
                                xhr.responseText : 
                                `${xhr.responseText.substring(0, 1000)}...`
                        });
                    });
                    
                    xhr.addEventListener('error', function() {
                        recordAction('xhr_error', {
                            ...xhr._requestData,
                            status: xhr.status
                        });
                    });
                    
                    return originalXHRSend.apply(this, arguments);
                };
                
                // Monitor clicks with enhanced context
                document.addEventListener('click', event => {
                    const target = event.target;
                    const rect = target.getBoundingClientRect();
                    
                    // Get parent context
                    const parents = [];
                    let parent = target.parentElement;
                    while (parent && parents.length < 3) {
                        parents.push({
                            tag: parent.tagName?.toLowerCase(),
                            id: parent.id,
                            class: parent.className,
                            text: parent.textContent?.trim()
                        });
                        parent = parent.parentElement;
                    }
                    
                    recordAction('click', {
                        element: {
                            tag: target.tagName?.toLowerCase(),
                            id: target.id,
                            name: target.name,
                            class: target.className,
                            type: target.type,
                            value: target.value,
                            text: target.textContent?.trim(),
                            href: target.href,
                            placeholder: target.placeholder,
                            title: target.title,
                            rect: {
                                x: rect.x,
                                y: rect.y,
                                width: rect.width,
                                height: rect.height
                            }
                        },
                        parents: parents,
                        x: event.clientX,
                        y: event.clientY
                    });
                }, true);
            }''')
            
            # Listen for console messages
            self.page.on("console", lambda msg: asyncio.create_task(self.handle_console(msg)))
            
            # Record page loads and URL changes
            self.page.on("load", lambda: asyncio.create_task(self.record_page_load()))
            self.page.on("framenavigated", lambda frame: asyncio.create_task(self.record_navigation(frame)))
            
        except Exception as e:
            logging.error(f"Error initializing browser: {str(e)}")
            raise

    async def handle_request(self, request):
        """Handle network requests"""
        try:
            if request.resource_type in ['xhr', 'fetch']:
                self.network_requests.append({
                    'timestamp': datetime.now().isoformat(),
                    'type': 'request',
                    'method': request.method,
                    'url': request.url,
                    'headers': request.headers,
                    'post_data': request.post_data
                })
        except Exception as e:
            logging.error(f"Error handling request: {str(e)}")

    async def handle_response(self, response):
        """Handle network responses"""
        try:
            if response.request.resource_type in ['xhr', 'fetch']:
                try:
                    body = await response.text()
                except:
                    body = '<binary or failed to read>'
                
                self.network_requests.append({
                    'timestamp': datetime.now().isoformat(),
                    'type': 'response',
                    'url': response.url,
                    'status': response.status,
                    'body': body[:1000] + '...' if len(body) > 1000 else body
                })
        except Exception as e:
            logging.error(f"Error handling response: {str(e)}")

    async def handle_console(self, msg):
        """Handle console messages from the page"""
        if msg.type == "log":
            try:
                data = json.loads(msg.text)
                if isinstance(data, dict) and 'type' in data:
                    formatted_action = f"{data['timestamp']} - {data['type'].upper()}"
                    
                    # Format based on action type
                    if data['type'] == 'dom_change':
                        formatted_action += "\n  Changes:"
                        for change in data.get('changes', []):
                            formatted_action += f"\n    - {change['type']} on {change['target']['tag']}"
                            if change['target'].get('id'): 
                                formatted_action += f" #{change['target']['id']}"
                            if change['target'].get('text'): 
                                formatted_action += f" text='{change['target']['text']}'"
                    
                    elif data['type'] in ['fetch_request', 'xhr_request']:
                        formatted_action += f"\n  URL: {data['url']}"
                        formatted_action += f"\n  Method: {data['method']}"
                        if data.get('body'):
                            formatted_action += f"\n  Body: {data['body']}"
                    
                    elif data['type'] in ['fetch_response', 'xhr_response']:
                        formatted_action += f"\n  URL: {data['url']}"
                        formatted_action += f"\n  Status: {data['status']}"
                        if data.get('response'):
                            formatted_action += f"\n  Response: {data['response']}"
                    
                    elif 'element' in data:
                        elem = data['element']
                        formatted_action += f"\n  Element: {elem.get('tag', '')}"
                        if elem.get('id'): formatted_action += f" #{elem['id']}"
                        if elem.get('class'): formatted_action += f" .{elem['class']}"
                        if elem.get('name'): formatted_action += f" name='{elem['name']}'"
                        if elem.get('type'): formatted_action += f" type='{elem['type']}"
                        if elem.get('value'): formatted_action += f" value='{elem['value']}'"
                        if elem.get('text'): formatted_action += f" text='{elem['text']}'"
                        
                        if 'rect' in elem:
                            formatted_action += f"\n  Position: x={elem['rect']['x']}, y={elem['rect']['y']}"
                        
                        if 'parents' in data:
                            formatted_action += "\n  Parents:"
                            for parent in data['parents']:
                                formatted_action += f"\n    - {parent['tag']}"
                                if parent.get('id'): formatted_action += f" #{parent['id']}"
                                if parent.get('text'): formatted_action += f" text='{parent['text']}'"
                    
                    self.actions.append(formatted_action)
            except json.JSONDecodeError:
                pass
            except Exception as e:
                logging.error(f"Error handling console message: {str(e)}")

    async def record_page_load(self):
        """Record page load events"""
        try:
            url = await self.page.evaluate("window.location.href")
            self.actions.append(f"{datetime.now().isoformat()} - PAGE_LOAD\n  URL: {url}")
        except Exception as e:
            logging.error(f"Error recording page load: {str(e)}")

    async def record_navigation(self, frame):
        """Record navigation events"""
        if frame == self.page.main_frame:
            try:
                url = frame.url
                self.actions.append(f"{datetime.now().isoformat()} - NAVIGATION\n  URL: {url}")
            except Exception as e:
                logging.error(f"Error recording navigation: {str(e)}")

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
                    console.print("[yellow]OTP required. Please enter the code:[/yellow]")
                    otp = input("Enter OTP: ")
                    await self.page.fill('input[name="otp"]', otp)
                    await self.page.click('button[type="submit"]')
            except:
                pass

            await self.page.wait_for_selector('#searchResultForm\\:orderSRT', timeout=30000)
            console.print("[green]Successfully logged in![/green]")
            console.print("\n[yellow]Now recording your actions. Set your search criteria as you normally would.[/yellow]")
            console.print("[yellow]Press Ctrl+C in the terminal when you're done to save the recording.[/yellow]")
            return True

        except Exception as e:
            logging.error(f"Login failed: {str(e)}")
            return False

    async def stop_recording(self):
        """Stop recording and save all captured actions"""
        if self.browser:
            try:
                # Save actions to a detailed log file
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                actions_file = f"detailed_actions_{timestamp}.log"
                
                with open(actions_file, 'w') as f:
                    f.write("Recorded Actions:\n\n")
                    # Write actions
                    for action in self.actions:
                        f.write(f"{action}\n\n")
                    
                    # Write network requests
                    f.write("\nNetwork Requests:\n\n")
                    for req in self.network_requests:
                        f.write(f"{json.dumps(req, indent=2)}\n\n")
                
                console.print(f"[green]Actions saved to {actions_file}[/green]")
                
            except Exception as e:
                logging.error(f"Error saving recording: {str(e)}")
            finally:
                try:
                    await self.context.close()
                except Exception as e:
                    logging.error(f"Error closing context: {str(e)}")
                try:
                    await self.browser.close()
                except Exception as e:
                    logging.error(f"Error closing browser: {str(e)}")

async def main():
    recorder = None
    try:
        recorder = ActionRecorder()
        await recorder.init_browser()
        
        if not await recorder.login():
            console.print("[red]Login failed. Please check your credentials.[/red]")
            return

        console.print("\n[yellow]Now recording your actions. Set your search criteria as you normally would.[/yellow]")
        console.print("[yellow]Press Ctrl+C in the terminal when you're done to save the recording.[/yellow]")
        
        # Wait for Ctrl+C
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping recording...[/yellow]")
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        logging.error(f"Error in main: {str(e)}")
    finally:
        if recorder:
            await recorder.stop_recording()
            console.print("[green]Recording completed![/green]")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass  # Ignore the KeyboardInterrupt at the top level
