#!/usr/bin/env python3
import asyncio
import logging
import os
from tabulate import tabulate
from ibt_property_search import IBTPropertySearchSession, setup_logging

async def extract_ownership(page):
    """
    In the property detail page on the Owner tab, extract the owner data from the row
    that is marked as Decision Maker.
    """
    # Wait for the owner table to be available.
    await page.wait_for_selector("#processPageForm\\:propertyTabView\\:propertyOwnerTable_data", timeout=10000)
    owner_rows = await page.query_selector_all("#processPageForm\\:propertyTabView\\:propertyOwnerTable_data tr")
    decision_owner = None

    for row in owner_rows:
        # Check if the last cell (Decision Maker column) contains a check mark with title "Decision Maker"
        decision_elem = await row.query_selector("td:last-child span.fa-check[title='Decision Maker']")
        if decision_elem:
            # Assume the first four cells are: Name, Email, Mobile, Landline.
            tds = await row.query_selector_all("td")
            if len(tds) >= 4:
                name_span = await tds[0].query_selector("span")
                email_span = await tds[1].query_selector("span")
                mobile_span = await tds[2].query_selector("span")
                landline_span = await tds[3].query_selector("span")

                name = (await name_span.inner_text()).strip() if name_span else ""
                email = (await email_span.inner_text()).strip() if email_span else ""
                mobile = (await mobile_span.inner_text()).strip() if mobile_span else ""
                landline = (await landline_span.inner_text()).strip() if landline_span else ""
                decision_owner = [name, email, mobile, landline]
            break
    return decision_owner

async def process_property(session, row_index):
    """
    For the property row at the given index:
      - Click its "eye" icon to open the details page.
      - Click on the Owner tab.
      - Extract the owner data (only from the row marked as Decision Maker).
      - Close the details page.
    Returns the extracted owner info as a list of four elements.
    """
    # Construct selector for the eye icon for this row.
    eye_selector = f"#searchResultForm\\:propertySearchSRT\\:{row_index}\\:viewSelectedRowItem"
    eye_icon = await session.page.wait_for_selector(eye_selector, timeout=5000)
    await eye_icon.click()
    logging.info(f"Opened property details for row {row_index}")

    # Wait for the property detail page to load.
    try:
        await session.page.wait_for_selector("#processPageForm\\:propertyTabView", timeout=15000)
    except Exception as e:
        logging.warning(f"Property tab view did not appear for row {row_index} within timeout: {e}")
        # Wait extra time and try again
        await asyncio.sleep(3)
        await session.page.wait_for_selector("#processPageForm\\:propertyTabView", timeout=15000)

    # Click the Owner tab.
    owner_tab_selector = "xpath=//*[@id='processPageForm:propertyTabView']/ul/li[4]/a"
    owner_tab = await session.page.wait_for_selector(owner_tab_selector, timeout=5000)
    await owner_tab.click()
    logging.info("Clicked on Owner tab")

    # Attempt to wait for the owner table. If it doesn't appear, log a warning and use empty values.
    try:
        await session.page.wait_for_selector("#processPageForm\\:propertyTabView\\:propertyOwnerTable_data", timeout=10000)
        owner_data = await extract_ownership(session.page)
    except Exception as e:
        logging.warning(f"Owner table not found for property at row {row_index}. Skipping owner extraction. Error: {e}")
        owner_data = None

    # Close the property details page.
    close_selector = "#processPageForm\\:j_idt340 span"
    try:
        close_button = await session.page.wait_for_selector(close_selector, timeout=5000)
    except Exception:
        # Fall back to an alternative selector if the first fails.
        close_selector = "xpath=//*[@id='processPageForm:j_idt341']/span"
        close_button = await session.page.wait_for_selector(close_selector, timeout=5000)
    await close_button.click()
    logging.info("Closed property details page")

    # Wait until we are back on the search results page.
    await session.page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    return owner_data

async def extract_search_results(session):
    """
    Extracts the search result data (FoL‑ID, Street, House Number, and House Number Appendix)
    from the search results table. First, the static data for each row is extracted and stored;
    then for each row the property details page is opened to extract owner information.
    """
    await session.page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    rows = await session.page.query_selector_all("#searchResultForm\\:propertySearchSRT_data tr")
    extracted_data = []
    rows_info = []

    # First extract the text data from each row.
    for idx, row in enumerate(rows):
        fol_elem = await row.query_selector("span[title='FoL-Id']")
        street_elem = await row.query_selector("span[title='Street']")
        house_elem = await row.query_selector("span[title='House number']")
        appendix_elem = await row.query_selector("span[title='House number Appndix']")

        fol_id = (await fol_elem.inner_text()).strip() if fol_elem else ""
        street = (await street_elem.inner_text()).strip() if street_elem else ""
        house_number = (await house_elem.inner_text()).strip() if house_elem else ""
        house_appendix = (await appendix_elem.inner_text()).strip() if appendix_elem else ""
        rows_info.append([fol_id, street, house_number, house_appendix])

    # Now process each property to extract owner info.
    for idx, row_info in enumerate(rows_info):
        owner_info = await process_property(session, idx)
        if owner_info:
            combined = row_info + owner_info
        else:
            combined = row_info + ["", "", "", ""]
        extracted_data.append(combined)
    return extracted_data

async def main():
    # Setup detailed logging.
    setup_logging(debug=True)

    session = IBTPropertySearchSession(
        username=os.getenv("TELEKOM_USERNAME"),
        password=os.getenv("TELEKOM_PASSWORD"),
        session_id=0,
        headless=False  # Change to True for headless mode.
    )
    session.otp_secret = os.getenv("TELEKOM_OTP_SECRET")
    
    logging.info("Initializing session")
    await session.init_browser()
    
    if not await session.login():
        logging.error("Login failed!")
        return
    logging.info(f"Logged in successfully. Current URL: {session.page.url}")
    
    # Set search criteria.
    area = "Bad Sooden-Allendorf, Stadt"
    logging.info(f"Setting search criteria for area: {area}")
    try:
        area_input = await session.page.wait_for_selector("[id='searchCriteriaForm:vvmArea_input']", timeout=10000)
        await area_input.click()
        await area_input.fill("")
        await area_input.type(area, delay=100)
        await area_input.dispatch_event("input")
        suggestion_panel_selector = "#searchCriteriaForm\\:vvmArea_panel"
        await session.page.wait_for_selector(suggestion_panel_selector, timeout=5000)
        suggestion = await session.page.wait_for_selector(f"{suggestion_panel_selector} li", timeout=5000)
        await suggestion.click()
        logging.info("Area input set successfully via autocomplete selection.")
        area_value = await session.page.evaluate("document.getElementById('searchCriteriaForm:vvmArea_input').value")
        logging.info(f"Area input field value: {area_value}")
    except Exception as e:
        logging.error(f"Failed to set area input: {str(e)}")
    
    try:
        # Set the number of results to 2500.
        dropdown = await session.page.wait_for_selector("xpath=//*[@id='searchCriteriaForm:nrOfResults']/div[3]/span", timeout=5000)
        await dropdown.click()
        option = await session.page.wait_for_selector("#searchCriteriaForm\\:nrOfResults_6", timeout=5000)
        await option.click()
    except Exception as e:
        logging.error(f"Failed to set number of results: {str(e)}")
    
    try:
        search_button = await session.page.wait_for_selector("#searchCriteriaForm\\:searchButton", timeout=10000)
        await search_button.click()
        logging.info("Clicked search button.")
    except Exception as e:
        logging.error(f"Error clicking search button: {str(e)}")
        await session.close()
        return

    # Allow time for the search results to load.
    await asyncio.sleep(5)
    
    # Extract search results along with owner information.
    extracted_data = await extract_search_results(session)
    
    # Print the final combined table.
    headers = ["FoL-ID", "Street", "House Number", "House Number Appendix",
               "Owner Name", "Owner Email", "Owner Mobile", "Owner Landline"]
    print(tabulate(extracted_data, headers=headers, tablefmt="pretty"))
    
    await session.close()
    logging.info("Session closed.")

if __name__ == "__main__":
    asyncio.run(main())