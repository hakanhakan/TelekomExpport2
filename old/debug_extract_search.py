#!/usr/bin/env python3
import asyncio
import logging
import os
from tabulate import tabulate
from ibt_property_search import IBTPropertySearchSession, setup_logging

async def extract_and_log_table(page):
    # Wait for the search results table to be available
    await page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    
    # Select all rows from the search results table body.
    rows = await page.query_selector_all("#searchResultForm\\:propertySearchSRT_data tr")
    extracted_data = []
    
    for row in rows:
        # Get the span elements with the corresponding title attributes.
        fol_elem = await row.query_selector("span[title='FoL-Id']")
        street_elem = await row.query_selector("span[title='Street']")
        house_elem = await row.query_selector("span[title='House number']")
        appendix_elem = await row.query_selector("span[title='House number Appndix']")
        
        # Extract text content (or default to empty string if not found)
        fol_id = (await fol_elem.inner_text()).strip() if fol_elem else ""
        street = (await street_elem.inner_text()).strip() if street_elem else ""
        house_number = (await house_elem.inner_text()).strip() if house_elem else ""
        house_appendix = (await appendix_elem.inner_text()).strip() if appendix_elem else ""
        
        extracted_data.append([fol_id, street, house_number, house_appendix])
    
    # Print the extracted data as a formatted table.
    print(tabulate(extracted_data,
                   headers=["FoL-ID", "Street", "House Number", "House Number Appendix"],
                   tablefmt="pretty"))

async def main():
    # Setup detailed logging.
    setup_logging(debug=True)
    
    # Create a session using environment variables for credentials and OTP secret.
    session = IBTPropertySearchSession(
        username=os.getenv("TELEKOM_USERNAME"),
        password=os.getenv("TELEKOM_PASSWORD"),
        session_id=0,
        headless=False  # Set to True if you want headless mode.
    )
    
    # Set the OTP secret for the session.
    session.otp_secret = os.getenv("TELEKOM_OTP_SECRET")
    
    logging.info("Initializing session")
    await session.init_browser()
    
    if not await session.login():
        logging.error("Login failed!")
        return
    logging.info(f"Logged in successfully. Current URL: {session.page.url}")
    
    # Set search criteria (the area to search)
    area = "Bad Sooden-Allendorf, Stadt"
    logging.info(f"Setting search criteria for area: {area}")
    
    try:
        # Wait for the area input field to be visible.
        # Using a CSS attribute selector works here.
        area_input = await session.page.wait_for_selector("[id='searchCriteriaForm:vvmArea_input']", timeout=10000)
        await area_input.click()
        await area_input.fill("")  # Clear any pre-existing text
        
        # Type the area slowly to simulate human input.
        await area_input.type(area, delay=100)
        # Dispatch an "input" event to trigger any JS listeners.
        await area_input.dispatch_event("input")
        # Wait for the suggestion panel to appear.
        # Note: The panel's id must be escaped (colon replaced with \\:)
        suggestion_panel_selector = "#searchCriteriaForm\\:vvmArea_panel"
        await session.page.wait_for_selector(suggestion_panel_selector, timeout=5000)
        # Click the first suggestion in the panel.
        suggestion = await session.page.wait_for_selector(f"{suggestion_panel_selector} li", timeout=5000)
        await suggestion.click()
        logging.info("Area input set successfully via autocomplete selection.")
        
        # Verify the field's value by reading it back
        area_value = await session.page.evaluate(
            "document.getElementById('searchCriteriaForm:vvmArea_input').value"
        )
        logging.info(f"Area input field value: {area_value}")
    except Exception as e:
        logging.error(f"Failed to set area input: {str(e)}")
    
    try:
        # Set the number of results.
        # Click the dropdown trigger to open the options.
        dropdown = await session.page.wait_for_selector("xpath=//*[@id='searchCriteriaForm:nrOfResults']/div[3]/span", timeout=5000)
        await dropdown.click()

        # Wait for the option with id corresponding to 2500 to appear and then click it.
        option = await session.page.wait_for_selector("#searchCriteriaForm\\:nrOfResults_6", timeout=5000)
        await option.click()
    except Exception as e:
        logging.error(f"Failed to set number of results: {str(e)}")
    
    # Click the search button
    try:
        search_button = await session.page.wait_for_selector("#searchCriteriaForm\\:searchButton", timeout=10000)
        await search_button.click()
        logging.info("Clicked search button.")
    except Exception as e:
        logging.error(f"Error clicking search button: {str(e)}")
        await session.close()
        return
    
    # Wait a few seconds to ensure that the search results table is fully loaded.
    await asyncio.sleep(5)
    
    # Extract and log the table data.
    await extract_and_log_table(session.page)
    
    # Close the browser session.
    await session.close()
    logging.info("Session closed.")

if __name__ == "__main__":
    asyncio.run(main())