#!/usr/bin/env python3
import asyncio
import logging
import os
from ibt_property_search import IBTPropertySearchSession, setup_logging

async def main():
    # Setup logging (adjust debug/quiet as needed)
    setup_logging(debug=True)
    
    # Create two sessions using environment variables for credentials and OTP secret
    session0 = IBTPropertySearchSession(
        username=os.getenv("TELEKOM_USERNAME"),
        password=os.getenv("TELEKOM_PASSWORD"),
        session_id=0,
        headless=False  # Set to True if you prefer headless mode
    )
    session1 = IBTPropertySearchSession(
        username=os.getenv("TELEKOM_USERNAME"),
        password=os.getenv("TELEKOM_PASSWORD"),
        session_id=1,
        headless=False
    )
    
    # Set the OTP secret for both sessions
    otp_secret = os.getenv("TELEKOM_OTP_SECRET")
    session0.otp_secret = otp_secret
    session1.otp_secret = otp_secret

    # Initialize and log in Session 0
    logging.info("Initializing Session 0")
    await session0.init_browser()
    if await session0.login():
        logging.info(f"Session 0 logged in successfully. Current URL: {session0.page.url}")
    else:
        logging.error("Session 0 login failed.")
    
    # Initialize and log in Session 1
    logging.info("Initializing Session 1")
    await session1.init_browser()
    if await session1.login():
        logging.info(f"Session 1 logged in successfully. Current URL: {session1.page.url}")
    else:
        logging.error("Session 1 login failed.")
    
    # Wait for a while to allow manual verification if needed
    await asyncio.sleep(5000)
    
    # Close both sessions
    await session0.close()
    await session1.close()
    logging.info("All sessions closed.")

if __name__ == "__main__":
    asyncio.run(main())