#!/usr/bin/env python3
"""
Test script for owner information extraction
"""

import asyncio
from dotenv import load_dotenv
import os
from playwright.async_api import async_playwright
from ibt_property_search import IBTPropertySearchSession, PropertyData, setup_logging

async def test_owner_extraction():
    """Test the owner information extraction functionality"""
    # Setup logging
    setup_logging(debug=True)
    
    # Load environment variables
    load_dotenv()
    
    # Get credentials from environment
    username = os.getenv("TELEKOM_USERNAME")
    password = os.getenv("TELEKOM_PASSWORD")
    
    if not username or not password:
        print("Error: TELEKOM_USERNAME and TELEKOM_PASSWORD environment variables must be set")
        return
    
    # Create a test property data object
    test_property = PropertyData(property_id="test_property")
    
    async with async_playwright():
        # Initialize session
        session = IBTPropertySearchSession(username, password, session_id=1, headless=False)
        
        try:
            # Initialize browser
            await session.init_browser()
            
            # Login
            login_success = await session.login()
            if not login_success:
                print("Login failed")
                return
            
            print("Login successful")
            
            # Navigate to property search
            await session.page.goto(session.search_url)
            await session.page.wait_for_timeout(3000)
            
            # Search for a specific area
            area = "Bad Sooden-Allendorf, Stadt"
            search_success = await session.search_by_area(area)
            if not search_success:
                print(f"Search for area '{area}' failed")
                return
            
            print(f"Search for area '{area}' successful")
            
            # Extract properties from search results
            properties = await session.extract_properties_from_results()
            if not properties:
                print("No properties found")
                return
            
            print(f"Found {len(properties)} properties")
            
            # Test owner information extraction on the first property
            if properties:
                test_property = properties[0]
                print(f"Testing owner information extraction for property ID: {test_property.property_id}")
                
                # Navigate to property details
                await session.open_property_details(0)  # Open the first property
                await session.page.wait_for_timeout(3000)
                
                # Extract owner information
                property_with_owner = await session.get_owner_information(test_property)
                
                # Print the extracted information
                print("\nExtracted Owner Information:")
                print(f"Owner Name: {property_with_owner.owner_name}")
                print(f"Owner Email: {property_with_owner.owner_email}")
                print(f"Owner Mobile: {property_with_owner.owner_mobile}")
                print(f"Owner Phone: {property_with_owner.owner_phone}")
                print(f"Is Decision Maker: {property_with_owner.is_decision_maker}")
                
                # Take a screenshot for verification
                await session.page.screenshot(path="owner_info_test.png")
                print("Screenshot saved to owner_info_test.png")
            
        except Exception as e:
            print(f"Error during test: {str(e)}")
        finally:
            # Close the browser
            await session.close()

if __name__ == "__main__":
    asyncio.run(test_owner_extraction())
