#!/usr/bin/env python3
"""
check_missing_records.py

This script checks for records in the buildings table that have data in the first_name column
but don't have a corresponding entry in the property_data table.

Relationship:
- buildings.extra_field_1 → property_data.fol_id
"""
import os
import sqlite3
import logging
import csv
from datetime import datetime
from tabulate import tabulate
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
log_filename = f'missing_records_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,
    filemode='w'
)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter('%(message)s'))
logging.getLogger().addHandler(console)
logger = logging.getLogger("missing_records_check")

def check_missing_records(db_path="extraction.db", report_path=None):
    """
    Check for records in buildings table that have first_name data but
    don't have a corresponding entry in property_data table.
    
    Args:
        db_path: Path to the SQLite database
        report_path: Optional path to save the CSV report, defaults to 'missing_records_report_YYYYMMDD_HHMMSS.csv'
    
    Returns:
        List of dictionaries containing the missing records
    """
    if not report_path:
        report_path = f'missing_records_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # This allows accessing columns by name
    
    logger.info("Checking for missing records...")
    
    try:
        # Query to find buildings records with first_name that don't have a match in property_data
        cursor = conn.execute("""
            SELECT b.*
            FROM buildings b
            LEFT JOIN property_data p ON b.extra_field_1 = p.fol_id
            WHERE b.first_name IS NOT NULL 
              AND b.first_name != ''
              AND p.fol_id IS NULL
        """)
        
        missing_records = [dict(row) for row in cursor.fetchall()]
        
        logger.info(f"Found {len(missing_records)} buildings with first_name data but no matching property_data record")
        
        # Display a summary table of the missing records
        if missing_records:
            # Define which columns to display in the summary table
            summary_columns = ['record_id', 'building_name', 'extra_field_1', 'first_name', 'last_name', 'email', 'phone_1']
            summary_data = []
            
            for record in missing_records:
                summary_row = [record.get(col, '') for col in summary_columns]
                summary_data.append(summary_row)
            
            print("\nMissing Records Summary:")
            print(tabulate(summary_data, headers=summary_columns, tablefmt="grid"))
            
            # Save detailed report to CSV
            with open(report_path, 'w', newline='', encoding='utf-8') as csv_file:
                # Use all columns for the CSV export
                fieldnames = list(missing_records[0].keys())
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(missing_records)
            
            logger.info(f"Detailed report saved to {report_path}")
        
        return missing_records
    
    except Exception as e:
        logger.error(f"Error checking for missing records: {str(e)}")
        raise
    finally:
        conn.close()

def generate_detailed_report(missing_records, csv_path=None, html_path=None):
    """
    Generate a more detailed report from the missing records data.
    
    Args:
        missing_records: List of dictionaries containing the missing records
        csv_path: Path to save the CSV report, defaults to 'missing_records_detailed_YYYYMMDD_HHMMSS.csv'
        html_path: Path to save the HTML report, defaults to 'missing_records_report_YYYYMMDD_HHMMSS.html'
    """
    if not missing_records:
        logger.info("No missing records to report.")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if not csv_path:
        csv_path = f'missing_records_detailed_{timestamp}.csv'
    
    if not html_path:
        html_path = f'missing_records_report_{timestamp}.html'
    
    # Prepare data for reports
    report_data = []
    
    # Define the columns we want to include in the detailed report
    detail_columns = [
        'record_id', 'area_record_id', 'building_name', 'extra_field_1',
        'first_name', 'last_name', 'email', 'phone_1', 'phone_2',
        'homes', 'offices', 'nvt', 'last_updated'
    ]
    
    for record in missing_records:
        row_data = {col: record.get(col, '') for col in detail_columns}
        report_data.append(row_data)
    
    # Generate CSV report
    with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=detail_columns)
        writer.writeheader()
        writer.writerows(report_data)
    
    logger.info(f"Detailed CSV report saved to {csv_path}")
    
    # Generate HTML report
    html_content = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        "    <title>Missing Records Report</title>",
        "    <style>",
        "        body { font-family: Arial, sans-serif; margin: 20px; }",
        "        h1 { color: #333366; }",
        "        .summary { margin-bottom: 20px; }",
        "        table { border-collapse: collapse; width: 100%; }",
        "        th, td { padding: 8px; text-align: left; border: 1px solid #ddd; }",
        "        th { background-color: #f2f2f2; }",
        "        tr:nth-child(even) { background-color: #f9f9f9; }",
        "        .timestamp { color: #666; font-style: italic; }",
        "    </style>",
        "</head>",
        "<body>",
        f"    <h1>Missing Records Report</h1>",
        f"    <div class='summary'>",
        f"        <p>This report shows {len(missing_records)} buildings with contact data but no corresponding property record.</p>",
        f"        <p class='timestamp'>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>",
        f"    </div>",
        "    <table>",
        "        <tr>"
    ]
    
    # Add table headers
    for col in detail_columns:
        html_content.append(f"            <th>{col}</th>")
    html_content.append("        </tr>")
    
    # Add table rows
    for record in report_data:
        html_content.append("        <tr>")
        for col in detail_columns:
            html_content.append(f"            <td>{record.get(col, '')}</td>")
        html_content.append("        </tr>")
    
    # Close HTML tags
    html_content.extend([
        "    </table>",
        "</body>",
        "</html>"
    ])
    
    # Write HTML file
    with open(html_path, 'w', encoding='utf-8') as html_file:
        html_file.write('\n'.join(html_content))
    
    logger.info(f"HTML report saved to {html_path}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Check for missing records in property_data table')
    parser.add_argument('--db-path', default='extraction.db', help='Path to the SQLite database')
    parser.add_argument('--report-path', help='Path to save the CSV report')
    parser.add_argument('--html-report', help='Path to save the HTML report')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Run the main check
        missing_records = check_missing_records(
            db_path=args.db_path,
            report_path=args.report_path
        )
        
        # Generate detailed report
        generate_detailed_report(
            missing_records,
            html_path=args.html_report
        )
        
        if missing_records:
            logger.info(f"Check completed. Found {len(missing_records)} records that need attention.")
            logger.info(f"Log file saved to {log_filename}")
        else:
            logger.info("Check completed. No issues found.")
            
    except Exception as e:
        logger.error(f"Check process failed with error: {str(e)}")
        raise
