from bs4 import BeautifulSoup
import os
import requests
from urllib.parse import urljoin

class ExplorationProtocolDownloader:
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()

    def is_exploration_button_active(self, button_html):
        """Check if the exploration protocol button is active."""
        soup = BeautifulSoup(button_html, 'html.parser')
        button = soup.find('button')
        
        # Button is inactive if it has disabled attribute or ui-state-disabled class
        is_disabled = (
            button.get('disabled') == 'disabled' or
            'ui-state-disabled' in button.get('class', [])
        )
        return not is_disabled

    def extract_order_id(self, html):
        """Extract the order ID from the detail page HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        order_id_span = soup.find('span', id='processPageForm:ibtOrderId')
        if order_id_span:
            return order_id_span.text.strip()
        return None

    def download_protocol(self, order_id, save_dir='.'):
        """
        Download the exploration protocol PDF for a given order ID.
        Returns: (success: bool, filepath: str or None, error: str or None)
        """
        if not order_id:
            return False, None, "No order ID provided"

        try:
            # Construct the save path
            filename = f"{order_id}.pdf"
            save_path = os.path.join(save_dir, filename)

            # Create directory if it doesn't exist
            os.makedirs(save_dir, exist_ok=True)

            # Construct the download URL (you'll need to replace this with the actual URL pattern)
            download_url = urljoin(self.base_url, f'protocol/download/{order_id}')

            # Download the PDF
            response = self.session.get(download_url, stream=True)
            response.raise_for_status()

            # Check if the response is actually a PDF
            if 'application/pdf' not in response.headers.get('content-type', '').lower():
                return False, None, "Response is not a PDF"

            # Save the PDF
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            return True, save_path, None

        except requests.exceptions.RequestException as e:
            return False, None, f"Download failed: {str(e)}"
        except IOError as e:
            return False, None, f"Failed to save PDF: {str(e)}"

# Test cases
def test_button_states():
    downloader = ExplorationProtocolDownloader('https://example.com')
    
    # Test inactive button
    inactive_button = '''
    <button id="processPageForm:explorationProtocol" name="processPageForm:explorationProtocol" 
    class="ui-button ui-widget ui-state-default ui-corner-all ui-button-text-only ui-state-disabled" 
    onclick="" style="float: right!important;" title="Download the exploration protocol (PDF)." 
    type="submit" disabled="disabled" role="button" aria-disabled="true">
    <span class="ui-button-text ui-c">Exploration Protocol</span></button>
    '''
    
    # Test active button
    active_button = '''
    <button id="processPageForm:explorationProtocol" name="processPageForm:explorationProtocol" 
    class="ui-button ui-widget ui-state-default ui-corner-all ui-button-text-only" 
    onclick="" style="float: right!important;" title="Download the exploration protocol (PDF)." 
    type="submit" role="button" aria-disabled="false">
    <span class="ui-button-text ui-c">Exploration Protocol</span></button>
    '''

    print("Testing inactive button:", not downloader.is_exploration_button_active(inactive_button))
    print("Testing active button:", downloader.is_exploration_button_active(active_button))

def test_order_id_extraction():
    downloader = ExplorationProtocolDownloader('https://example.com')
    
    # Test order ID extraction
    order_html = '''
    <tr class="ui-widget-content" role="row">
        <td role="gridcell" class="ui-panelgrid-cell">
            <label id="processPageForm:j_idt125" class="ui-outputlabel ui-widget ui-outputlabel ui-widget" 
            title="Order Id" for="processPageForm:ibtOrderId">Order Id</label>
        </td>
        <td role="gridcell" class="ui-panelgrid-cell">
            <span id="processPageForm:ibtOrderId" style="margin-left: 2em;" title="Order Id">2458962</span>
        </td>
    </tr>
    '''
    
    order_id = downloader.extract_order_id(order_html)
    print("Testing order ID extraction:", order_id == "2458962")

if __name__ == "__main__":
    print("Running tests...")
    test_button_states()
    test_order_id_extraction()
