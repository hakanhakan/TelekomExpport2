Here's a comprehensive PRD for the Telekom Order Export Python application:

```
markdown
```

CopyInsert

```` # Product Requirements Document: Telekom Order Export Automation ## Overview A Python-based automation tool to extract order details from the Telekom supplier portal (glasfaser.telekom.de/auftragnehmerportal-ui/). The application will handle authentication, navigate the portal, extract order information, and export to CSV format. ## Implementation Status ### Completed Features 1. **Authentication Management**    - Username/password login implemented    - OTP (One-Time Password) handling    - Secure credential storage using .env    - Login validation and error handling 2. **Excel Export Processing**    - Automated Excel file download capture    - User-controlled search criteria    - Excel parsing for Order IDs    - Unique Order ID extraction 3. **Order Detail Extraction**    - Detail page navigation with robust pagination    - Data extraction from detail pages    - Batch processing implementation    - Dynamic row indexing using data-ri attributes    - Comprehensive error handling    - Robust detail page closing mechanism    - Automatic cleanup after extraction 4. **Exploration Protocol Download**    - Automatic PDF download for available protocols    - Smart button state detection    - Robust error handling    - Efficient page management    - Automatic cleanup after download    - Organized file storage in downloads directory 5. **Data Export**    - CSV export formatting    - Data validation    - Export error handling    - Timestamp-based file naming 6. **Project Structure**    - Basic project setup    - Core dependencies management    - Error logging implementation    - Console output using Rich 7. **Performance Optimization**    - Rate limiting implemented    - Session management    - Retry mechanisms    - Error recovery logic ### In Progress 1. **Testing**    - Unit tests    - Integration tests    - Error scenario testing 2. **Performance Optimization**    - Concurrent processing ### Pending Features 1. **Advanced Features**    - GUI interface    - Real-time monitoring    - API endpoint    - Automated scheduling ## Technical Implementation ### Current Architecture 1. **TelekomExporter Class**    - Browser initialization    - Login management    - Excel download handling    - Order data extraction with pagination    - Exploration protocol download    - Robust error handling and recovery    - Dynamic element selection    - Efficient page state management 2. **OrderData Model**    - Comprehensive data structure    - Field validation    - Export formatting ### Workflow 1. User starts the script 2. Script handles login (including OTP) 3. User sets search criteria in portal 4. User triggers Excel export 5. Script captures and processes Excel 6. Script visits each order's detail page (with pagination) 7. For each order:    - Extracts order details    - Downloads exploration protocol if available    - Automatically closes detail page 8. Data is extracted and exported to CSV ### Dependencies - Playwright: Browser automation - Pandas: Excel/CSV processing - Rich: Console interface - Pydantic: Data validation - python-dotenv: Environment management - beautifulsoup4: HTML parsing - requests: HTTP requests handling ## Next Steps 1. Implement performance optimization 2. Add comprehensive testing 3. Implement CSV export functionality 4. Add unit tests and documentation ## Known Limitations - Manual OTP input required - Single-threaded processing - Manual search criteria setup - Sensitivity to website structure changes ## Future Enhancements - Automated search criteria setting - Concurrent processing - Advanced error recovery - Progress saving and resumption - Detailed logging and reporting ## Core Features ### 1. Authentication Management - Support for username/password login - Handle OTP (One-Time Password) input - Session persistence to minimize login frequency - Secure credential storage - Session recovery mechanisms ### 2. Order Processing - Batch processing of order IDs - Configurable processing delays - Automatic retry on failures - Progress tracking and reporting - Concurrent processing capabilities ### 3. Data Extraction Fields to extract: - externalOrderId - orderId - orderStatus - orderType - customerName - customerType - customerOrderReference - street - houseNumber - postalCode - city - appointmentStatus - installationDueDate - klsId - folId - buildingType - accommodationUnits - buildUpAgreement - constructionType - projectId - customerEmail - customerPhone - customerMobile - carrierName (default: "Deutsche Telekom AG") Additional artifacts: - Exploration Protocol PDF (when available) ### 4. Export Capabilities - CSV export with configurable delimiter - Excel export option - Automatic file naming with timestamps - Support for custom export paths - Append/overwrite options ### 5. Error Handling - Comprehensive error logging - Automatic retry mechanisms - Session expiration handling - Network error recovery - Invalid data detection ## Technical Requirements ### Technology Stack - Python 3.9+ - Playwright for browser automation - Pandas for data handling - Rich for CLI interface - Pydantic for data validation - Python-dotenv for configuration ### Architecture Components 1. Browser Automation Module - Playwright-based navigation - Element interaction handling - Wait and retry logic 2. Data Processing Module - HTML parsing - Data validation - Field extraction - Data transformation 3. Authentication Module - Login flow management - Session handling - Cookie management - Credential security 4. Export Module - CSV generation - Data formatting - File management 5. CLI Interface - Progress display - Error reporting - User input handling ### Configuration Options - Login credentials - Export path - Retry attempts - Delay between requests - Logging level - Output format - Concurrent processing settings ## User Interface ### Command Line Interface ```bash # Basic usage python telekom_export.py --orders order1,order2,order3 # Batch file input python telekom_export.py --input orders.txt # Export options python telekom_export.py --output custom_path.csv --format excel ````

### **Interactive Features**

- Login credential prompt
- OTP input handling
- Progress bars
- Error notifications
- Status updates

## **Error Handling**

### **Scenarios**

1. Authentication Failures
   - Invalid credentials
   - OTP timeout
   - Session expiration
2. Network Issues
   - Connection timeout
   - Server errors
   - Rate limiting
3. Data Extraction
   - Missing fields
   - Invalid data formats
   - Page structure changes
4. Export Errors
   - File permission issues
   - Disk space limitations
   - Invalid file paths

## **Performance Requirements**

- Process minimum 100 orders per hour
- Maximum 2 second delay between requests
- 95% successful extraction rate
- Automatic retry on temporary failures
- Memory efficient for large datasets

## **Security Requirements**

- Secure credential storage
- No sensitive data logging
- Session cookie protection
- Secure error reporting

## **Testing Requirements**

- Unit tests for each module
- Integration tests for full workflow
- Error scenario testing
- Performance testing
- Security testing

## **Deployment**

- Python package distribution
- Dependencies management
- Environment setup script
- Configuration template
- Documentation

## **Documentation**

- Installation guide
- Usage instructions
- Configuration reference
- Troubleshooting guide
- API documentation
- Error code reference

## **Future Enhancements**

- GUI interface
- Real-time monitoring
- API endpoint
- Multiple carrier support
- Advanced reporting
- Automated scheduling

```
Code
```

CopyInsert

` This PRD provides a comprehensive blueprint for the Telekom Order Export automation tool. It covers all essential aspects while maintaining flexibility for future enhancements. The modular architecture ensures maintainability and extensibility. Would you like me to explain any specific part in more detail or proceed with implementing a particular component?`
```