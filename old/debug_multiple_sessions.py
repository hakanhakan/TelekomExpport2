#!/usr/bin/env python3
import asyncio
import logging
import os
from tabulate import tabulate
import aiosqlite

from ibt_property_search import IBTPropertySearchSession, setup_logging

# ================================
# Ownership Extraction
# ================================

async def extract_ownership(page):
    """
    In the property detail page on the Owner tab, extract the owner data from the row
    that is marked as Decision Maker.
    """
    try:
        await page.wait_for_selector("#processPageForm\\:propertyTabView\\:propertyOwnerTable_data", timeout=10000)
    except Exception as e:
        logging.warning(f"Owner table did not appear: {e}")
        return None

    owner_rows = await page.query_selector_all("#processPageForm\\:propertyTabView\\:propertyOwnerTable_data tr")
    decision_owner = None

    for row in owner_rows:
        decision_elem = await row.query_selector("td:last-child span.fa-check[title='Decision Maker']")
        if decision_elem:
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

# ================================
# Property-Level Extraction
# ================================

async def process_property(session, ri):
    """
    For a given row (using its data-ri attribute value):
      - Click the "eye" icon,
      - Wait for the detail page,
      - Click the Owner tab,
      - Extract the Decision Maker row,
      - Close the detail page,
      - Return [owner_name, owner_email, owner_mobile, owner_landline].
    """
    # Build selector using the actual data-ri value (e.g., "40", "41", etc.)
    eye_selector = f"#searchResultForm\\:propertySearchSRT\\:{ri}\\:viewSelectedRowItem"
    try:
        eye_icon = await session.page.wait_for_selector(eye_selector, timeout=5000)
    except Exception as e:
        logging.error(f"[Session {session.session_id}] Eye icon not found for data-ri {ri}: {e}")
        return None

    await eye_icon.click()
    logging.info(f"[Session {session.session_id}] Opened property details for data-ri {ri}")

    # Wait for the detail page to load
    try:
        await session.page.wait_for_selector("#processPageForm\\:propertyTabView", timeout=15000)
    except Exception as e:
        logging.warning(f"[Session {session.session_id}] Property tab view did not appear for data-ri {ri}: {e}")
        await asyncio.sleep(3)
        await session.page.wait_for_selector("#processPageForm\\:propertyTabView", timeout=15000)

    # Click the Owner tab.
    owner_tab_selector = "xpath=//*[@id='processPageForm:propertyTabView']/ul/li[4]/a"
    try:
        owner_tab = await session.page.wait_for_selector(owner_tab_selector, timeout=5000)
        await owner_tab.click()
        logging.info(f"[Session {session.session_id}] Clicked on Owner tab (data-ri {ri})")
    except Exception as e:
        logging.error(f"[Session {session.session_id}] Owner tab not found for data-ri {ri}: {e}")
        return None

    # Extract owner information.
    try:
        await session.page.wait_for_selector("#processPageForm\\:propertyTabView\\:propertyOwnerTable_data", timeout=10000)
        owner_data = await extract_ownership(session.page)
    except Exception as e:
        logging.warning(f"[Session {session.session_id}] Owner table not found for data-ri {ri}: {e}")
        owner_data = None

    # Close the detail page.
    close_selector = "#processPageForm\\:j_idt340 span"
    try:
        close_button = await session.page.wait_for_selector(close_selector, timeout=5000)
    except Exception:
        close_selector = "xpath=//*[@id='processPageForm:j_idt341']/span"
        close_button = await session.page.wait_for_selector(close_selector, timeout=5000)
    await close_button.click()
    logging.info(f"[Session {session.session_id}] Closed detail page (data-ri {ri})")

    # Wait until the search results table is visible again.
    await session.page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    return owner_data

# ================================
# Page Extraction
# ================================

async def extract_search_results(session):
    """
    Extract static data from the current page's table rows.
    For each row, also retrieve its "data-ri" attribute.
    Then, call process_property for that specific data-ri.
    Returns a list of rows:
    [FoL-ID, Street, House#, House#Appendix, OwnerName, OwnerEmail, OwnerMobile, OwnerLandline].
    """
    try:
        await session.page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    except Exception as e:
        logging.error(f"[Session {session.session_id}] Search results table not found: {e}")
        return []

    rows = await session.page.query_selector_all("#searchResultForm\\:propertySearchSRT_data tr")
    extracted_data = []
    row_data_cache = []

    for row in rows:
        # Get the data-ri attribute from the row.
        ri = await row.get_attribute("data-ri")
        fol_elem = await row.query_selector("span[title='FoL-Id']")
        street_elem = await row.query_selector("span[title='Street']")
        house_elem = await row.query_selector("span[title='House number']")
        appendix_elem = await row.query_selector("span[title='House number Appndix']")

        fol_id = (await fol_elem.inner_text()).strip() if fol_elem else ""
        street = (await street_elem.inner_text()).strip() if street_elem else ""
        house_number = (await house_elem.inner_text()).strip() if house_elem else ""
        house_appendix = (await appendix_elem.inner_text()).strip() if appendix_elem else ""
        row_data_cache.append((ri, fol_id, street, house_number, house_appendix))

    for ri, fol_id, street, house_number, house_appendix in row_data_cache:
        owner_info = await process_property(session, ri)
        if owner_info:
            combined = [fol_id, street, house_number, house_appendix] + owner_info
        else:
            combined = [fol_id, street, house_number, house_appendix, "", "", "", ""]
        extracted_data.append(combined)

    return extracted_data

# ================================
# Database Functions
# ================================

async def save_page_data_to_db(session_id, page_number, data):
    """
    Saves each row into a SQLite table 'property_data' in extraction.db.
    """
    async with aiosqlite.connect("extraction.db") as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS property_data (
                session_id INTEGER,
                page INTEGER,
                fol_id TEXT,
                street TEXT,
                house_number TEXT,
                house_appendix TEXT,
                owner_name TEXT,
                owner_email TEXT,
                owner_mobile TEXT,
                owner_landline TEXT
            )
        """)
        for row in data:
            await db.execute("""
                INSERT INTO property_data 
                (session_id, page, fol_id, street, house_number, house_appendix, owner_name, owner_email, owner_mobile, owner_landline)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (session_id, page_number, *row))
        await db.commit()

# ================================
# Page Navigation Helpers
# ================================

async def click_next_page(session):
    """
    Clicks the "Next" button in the paginator and waits for the new page to load.
    """
    next_selector = "#searchResultForm\\:propertySearchSRT_paginator_top > a.ui-paginator-next > span"
    try:
        next_button = await session.page.wait_for_selector(next_selector, timeout=5000)
        await next_button.click()
        logging.info(f"[Session {session.session_id}] Clicked next button.")
        await asyncio.sleep(3)
        await session.page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    except Exception as e:
        logging.error(f"[Session {session.session_id}] Failed to click next page: {e}")
        raise

async def go_to_page_by_clicking_number(session, page_number):
    """
    Clicks the page label for a specific page number.
    """
    try:
        link_selector = f"xpath=//*[@id='searchResultForm:propertySearchSRT_paginator_top']/span[1]/a[text()='{page_number}']"
        page_link = await session.page.wait_for_selector(link_selector, timeout=5000)
        await page_link.click()
        logging.info(f"[Session {session.session_id}] Clicked page label {page_number}.")
        await asyncio.sleep(3)
        await session.page.wait_for_selector("#searchResultForm\\:propertySearchSRT_data", timeout=10000)
    except Exception as e:
        logging.warning(f"[Session {session.session_id}] Could not directly click page {page_number}: {e}")
        current_page = 1
        while current_page < page_number:
            await click_next_page(session)
            current_page += 1

# ================================
# Range Processing
# ================================

async def process_page_range(session, start_page, end_page):
    """
    Jumps to 'start_page' (if needed) then extracts and saves data from pages start_page to end_page.
    """
    if start_page > 1:
        await go_to_page_by_clicking_number(session, start_page)

    for page_number in range(start_page, end_page + 1):
        logging.info(f"[Session {session.session_id}] Extracting data from page {page_number}")
        page_data = await extract_search_results(session)
        await save_page_data_to_db(session.session_id, page_number, page_data)
        logging.info(f"[Session {session.session_id}] Saved data for page {page_number}")

        if page_number < end_page:
            await click_next_page(session)

# ================================
# Main & Multi-Session
# ================================

async def main():
    setup_logging(debug=True)

    total_pages = 5     # Adjust total pages for your test; change this as needed.
    num_sessions = 5    # Number of concurrent sessions.
    pages_per_session = total_pages // num_sessions

    sessions = []
    # Create and log in each session.
    for i in range(num_sessions):
        s = IBTPropertySearchSession(
            username=os.getenv("TELEKOM_USERNAME"),
            password=os.getenv("TELEKOM_PASSWORD"),
            session_id=i,
            headless=False  # Change to True for headless operation.
        )
        s.otp_secret = os.getenv("TELEKOM_OTP_SECRET")
        await s.init_browser()
        if not await s.login():
            logging.error(f"Session {i} login failed.")
            continue
        logging.info(f"Successfully logged in session {i}")
        sessions.append(s)

    if not sessions:
        logging.error("No sessions available. Exiting.")
        return

    # Set the same search criteria for all sessions.
    for s in sessions:
        area = "Bad Sooden-Allendorf, Stadt"
        logging.info(f"[Session {s.session_id}] Setting search criteria for area: {area}")
        try:
            area_input = await s.page.wait_for_selector("[id='searchCriteriaForm:vvmArea_input']", timeout=10000)
            await area_input.click()
            await area_input.fill("")
            await area_input.type(area, delay=50)
            await area_input.dispatch_event("input")
            suggestion_panel_selector = "#searchCriteriaForm\\:vvmArea_panel"
            await s.page.wait_for_selector(suggestion_panel_selector, timeout=5000)
            suggestion = await s.page.wait_for_selector(f"{suggestion_panel_selector} li", timeout=5000)
            await suggestion.click()
        except Exception as e:
            logging.error(f"[Session {s.session_id}] Failed area input: {e}")

        # Set number of results (optional).
        try:
            dropdown = await s.page.wait_for_selector("xpath=//*[@id='searchCriteriaForm:nrOfResults']/div[3]/span", timeout=5000)
            await dropdown.click()
            option = await s.page.wait_for_selector("#searchCriteriaForm\\:nrOfResults_6", timeout=5000)
            await option.click()
            logging.info(f"[Session {s.session_id}] Set number of results to 2500 (option index 6).")
        except Exception as e:
            logging.error(f"[Session {s.session_id}] Failed to set number of results: {e}")

        # Click the search button.
        try:
            search_btn = await s.page.wait_for_selector("#searchCriteriaForm\\:searchButton", timeout=10000)
            await search_btn.click()
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"[Session {s.session_id}] Could not click search: {e}")

    # Distribute page ranges among sessions.
    tasks = []
    for i, s in enumerate(sessions):
        start_page = i * pages_per_session + 1
        if i == num_sessions - 1:
            end_page = total_pages
        else:
            end_page = (i + 1) * pages_per_session

        logging.info(f"[Session {s.session_id}] Assigned pages {start_page} to {end_page}")
        tasks.append(process_page_range(s, start_page, end_page))

    await asyncio.gather(*tasks)

    # (Optional) Print combined results from the SQLite database.
    async with aiosqlite.connect("extraction.db") as db:
        async with db.execute("SELECT * FROM property_data") as cursor:
            all_rows = await cursor.fetchall()
            headers = [
                "session_id","page","fol_id","street","house_number",
                "house_appendix","owner_name","owner_email","owner_mobile","owner_landline"
            ]
            print(tabulate(all_rows, headers=headers, tablefmt="pretty"))

    # Close all sessions.
    for s in sessions:
        await s.close()
    logging.info("All sessions closed.")

if __name__ == "__main__":
    asyncio.run(main())