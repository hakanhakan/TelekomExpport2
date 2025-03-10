#!/usr/bin/env python3
import asyncio
import logging
import os
from ibt_property_search import IBTPropertySearchSession, setup_logging

async def main():
    # Setup detailed logging
    setup_logging(debug=True)
    
    # Create a session using environment variables for credentials and OTP secret
    session = IBTPropertySearchSession(
        username=os.getenv("TELEKOM_USERNAME"),
        password=os.getenv("TELEKOM_PASSWORD"),
        session_id=0,
        headless=False  # Change to True for headless mode if desired
    )
    
    # Set the OTP secret for the session
    session.otp_secret = os.getenv("TELEKOM_OTP_SECRET")
    
    # Initialize browser and log in
    logging.info("Initializing Session")
    await session.init_browser()
    if await session.login():
        logging.info(f"Logged in successfully. Current URL: {session.page.url}")
    else:
        logging.error("Login failed")
        return
    
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
    
    # Wait for search results to load
    await asyncio.sleep(5)
    logging.info(f"Search results page loaded. Current URL: {session.page.url}")
    
    # Log the count of search result rows
    try:
        rows = await session.page.query_selector_all("#searchResultForm\\:propertySearchSRT_data tr")
        logging.info(f"Found {len(rows)} rows in search results.")
    except Exception as e:
        logging.error(f"Error retrieving search result rows: {str(e)}")
    
    # Close the session
    await session.close()
    logging.info("Session closed.")

if __name__ == "__main__":
    asyncio.run(main())