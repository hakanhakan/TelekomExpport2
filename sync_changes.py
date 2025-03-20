#!/usr/bin/env python3
"""
sync_changes.py

Synchronizes changes between the property_data table (Telekom data)
and the buildings table (Airtable snapshot), pushing only the differences
to Airtable.
"""
import os
import sqlite3
import logging
from datetime import datetime
from pyairtable import Api
from tabulate import tabulate
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'airtable_sync_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    filemode='w'
)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter('%(message)s'))
logging.getLogger().addHandler(console)
logger = logging.getLogger("airtable_sync")

# Airtable configuration
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

import re

# Field mapping between property_data and buildings tables
FIELD_MAPPING = {
    'fol_id': 'extra_field_1',
    'exploration': 'extra_field_3',  # Corrected from extra_field_4 to extra_field_3
    'owner_name': 'first_name',
    'owner_email': 'email',
    'owner_mobile': 'phone_1',
    'owner_landline': 'phone_2',
    'au': 'homes', 
    'bu': 'offices',
    'nvt_area': 'nvt',
    'calculated_box': 'extra_field_2'  # Special calculated field for box type
}

# Reverse mapping (Airtable field names to their API field names)
AIRTABLE_FIELD_NAMES = {
    'extra_field_1': 'Extra field 1',
    'extra_field_2': 'Extra field 2',
    'extra_field_3': 'Extra field 3',
    'first_name': 'First name',
    'email': 'Email',
    'phone_1': 'Phone 1',
    'phone_2': 'Phone 2',
    'homes': 'HOMES',
    'offices': 'OFFICES',
    'nvt': 'NVT'
}

# Box type mapping based on the number of units (WE)
BOX_TYPE_MAPPING = [
    (1, "Box: G-AP OneBox XS (1WE), 10er Pack | Material Nr.:47122083"),
    (3, "Box: GI-AP OneBox  1 - 3 WE | Material Nr.:47100635"),
    (8, "Box: GI-AP OneBox  4 - 8 WE | Material Nr.:47100636"),
    (12, "Box: GI-AP OneBox  9 -12 WE | Material Nr.:47100637"),
    (20, "Box: GI-AP OneBox 13 - 20 WE | Material Nr.:47100638"),
    (32, "Box: GI-AP OneBox 21 - 32 WE | Material Nr.:47100639"),
    (float('inf'), None)  # For any value above 32
]

def extract_exploration_date(value):
    """
    Extract just the date portion from a string like "Exploration done: 6/1/2024 01:01AM"
    Returns the original value if no pattern match is found.
    """
    if not value:
        return value
        
    match = re.search(r"Exploration done:\s*(.*)", str(value))
    if match:
        return match.group(1).strip()
    return value

def get_box_type_for_units(total_units):
    """
    Determine the appropriate box type based on the total number of units (au + bu).
    Returns the full box description string for the given number of units.
    """
    if total_units is None or total_units == 0:
        return None
        
    # Convert to int to ensure proper comparison
    try:
        total_units = int(total_units)
    except (ValueError, TypeError):
        logger.warning(f"Invalid unit count for box type calculation: {total_units}")
        return None
    
    # Find the appropriate box type based on the total units
    for max_units, box_description in BOX_TYPE_MAPPING:
        if total_units <= max_units:
            return box_description
    
    # Should never reach here due to the float('inf') entry, but just in case
    return None

def load_data_from_sqlite(db_path="extraction.db"):
    """
    Load data from both tables and return as dictionaries.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # This allows accessing columns by name
    
    # Load property_data (Telekom data)
    property_data = {}
    cursor = conn.execute("SELECT * FROM property_data")
    for row in cursor:
        row_dict = dict(row)
        fol_id = row_dict.get('fol_id')
        if fol_id:
            property_data[fol_id] = row_dict
    
    # Load buildings (Airtable snapshot)
    buildings = {}
    cursor = conn.execute("SELECT * FROM buildings")
    for row in cursor:
        row_dict = dict(row)
        fol_id = row_dict.get('extra_field_1')
        if fol_id:
            buildings[fol_id] = row_dict
    
    conn.close()
    
    logger.info(f"Loaded {len(property_data)} records from property_data")
    logger.info(f"Loaded {len(buildings)} records from buildings")
    
    return property_data, buildings

def compare_records(telekom_record, airtable_record):
    """
    Compare fields between Telekom and Airtable records, return differences.
    """
    differences = {}
    
    # Process normal fields first to collect numeric values for calculated fields
    au_value = 0
    bu_value = 0
    
    for telekom_field, airtable_field in FIELD_MAPPING.items():
        # Skip the special calculated field for now
        if telekom_field == 'calculated_box':
            continue
            
        # Skip if the field doesn't exist in either record
        if telekom_field not in telekom_record or airtable_field not in airtable_record:
            continue
        
        telekom_value = telekom_record.get(telekom_field)
        airtable_value = airtable_record.get(airtable_field)
        
        # Normalize None/NULL values for all fields
        if telekom_value is None:
            telekom_value = ""
        if airtable_value is None:
            airtable_value = ""
            
        # Skip empty → empty updates for all fields
        if (not telekom_value and not airtable_value) and telekom_field != 'au' and telekom_field != 'bu':
            continue
        
        # Store values for calculated fields (keep numeric handling)
        if telekom_field == 'au':
            try:
                au_value = int(telekom_value) if telekom_value else 0
            except (ValueError, TypeError):
                au_value = 0
                
        if telekom_field == 'bu':
            try:
                bu_value = int(telekom_value) if telekom_value else 0
            except (ValueError, TypeError):
                bu_value = 0
        
        # Special case for numeric fields
        if airtable_field in ['homes', 'offices']:
            try:
                if telekom_value:
                    telekom_value = int(telekom_value)
                else:
                    telekom_value = 0
                    
                if airtable_value:
                    airtable_value = int(airtable_value)
                else:
                    airtable_value = 0
            except (ValueError, TypeError):
                # If conversion fails, we'll just compare as strings
                pass
        
        # Special handling for exploration date in extra_field_3
        if airtable_field == 'extra_field_3':
            airtable_value = extract_exploration_date(airtable_value)
            # Prepare the formatted value for Airtable if we have a telekom value
            if telekom_value:
                telekom_value_for_airtable = f"Exploration done: {telekom_value}"
            else:
                telekom_value_for_airtable = None
            
        # For string type values, ensure comparison is done as strings 
        # and ignore whitespace-only differences
        if (isinstance(telekom_value, str) and isinstance(airtable_value, str) and 
            telekom_value.strip() == airtable_value.strip()):
            continue
            
        # Compare values - account for type differences (e.g., int vs str)
        if str(telekom_value) != str(airtable_value):
            # Map to Airtable API field names
            airtable_api_field = AIRTABLE_FIELD_NAMES.get(airtable_field, airtable_field)
            
            # Handle special case for exploration date formatting
            if airtable_field == 'extra_field_3':
                if telekom_value:
                    differences[airtable_api_field] = f"Exploration done: {telekom_value}"
                else:
                    # Only update if we're changing from a value to empty, not None to empty
                    if airtable_value:
                        differences[airtable_api_field] = None
            else:
                # Only update when there's a meaningful change
                # Don't update empty string to None or vice versa
                if not (not telekom_value and not airtable_value):
                    differences[airtable_api_field] = telekom_value
                
            logger.debug(f"Field {airtable_field} differs: {airtable_value} -> {telekom_value}")
    
    # Now handle the calculated box type based on au + bu
    total_units = au_value + bu_value
    calculated_box = get_box_type_for_units(total_units)
    
    if calculated_box:
        airtable_field = 'extra_field_2'
        airtable_value = airtable_record.get(airtable_field)
        
        # Only update if different
        if calculated_box != airtable_value:
            airtable_api_field = AIRTABLE_FIELD_NAMES.get(airtable_field, airtable_field)
            differences[airtable_api_field] = calculated_box
            logger.debug(f"Calculated box type differs for {total_units} units: {airtable_value} -> {calculated_box}")
    
    return differences

def sync_changes_to_airtable(batch_size=10, max_records=None):
    """
    Main function to sync changes from Telekom to Airtable.
    
    Args:
        batch_size: Number of records to update in a single API call
        max_records: Maximum number of records to update (for testing/debugging)
    """
    # 1. Load data from both tables
    telekom_data, airtable_data = load_data_from_sqlite()
    
    # 2. Initialize Airtable connection
    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, TABLE_NAME)
    
    # 3. Prepare for batching and statistics
    updates_batch = []
    stats = {'matched': 0, 'updated': 0, 'unchanged': 0, 'errors': 0, 'batches': 0}
    
    # 4. Create a lookup from FOL-ID to Airtable record ID
    fol_to_record_id = {
        fol_id: record.get('record_id') 
        for fol_id, record in airtable_data.items() 
        if record.get('record_id')
    }
    
    # 5. Process each Telekom record
    logger.info("Comparing records and preparing updates...")
    for fol_id, telekom_record in telekom_data.items():
        # Find matching Airtable record
        if fol_id not in airtable_data:
            logger.warning(f"No matching Airtable record for FOL-ID: {fol_id}")
            continue
        
        stats['matched'] += 1
        airtable_record = airtable_data[fol_id]
        record_id = fol_to_record_id.get(fol_id)
        
        if not record_id:
            logger.warning(f"No Airtable record ID for FOL-ID: {fol_id}")
            continue
        
        # Compare and find differences
        differences = compare_records(telekom_record, airtable_record)
        
        # If differences exist, queue update
        if differences:
            updates_batch.append({
                'id': record_id,
                'fields': differences
            })
            stats['updated'] += 1
            logger.info(f"Found changes for FOL-ID {fol_id}: {', '.join(differences.keys())}")
            
            # Check if we've reached the maximum number of records to update
            if max_records is not None and stats['updated'] >= max_records:
                logger.info(f"Reached maximum number of records to update ({max_records})")
                break
        else:
            stats['unchanged'] += 1
        
        # Process updates in batches
        if len(updates_batch) >= batch_size:
            try:
                if updates_batch:
                    logger.info(f"Pushing batch of {len(updates_batch)} updates to Airtable...")
                    table.batch_update(updates_batch)
                    stats['batches'] += 1
                    updates_batch = []
            except Exception as e:
                logger.error(f"Error updating batch: {str(e)}")
                stats['errors'] += 1
    
    # 6. Process any remaining updates
    if updates_batch:
        try:
            logger.info(f"Pushing final batch of {len(updates_batch)} updates to Airtable...")
            table.batch_update(updates_batch)
            stats['batches'] += 1
        except Exception as e:
            logger.error(f"Error updating final batch: {str(e)}")
            stats['errors'] += 1
    
    # 7. Log summary
    logger.info("\n" + "="*50)
    logger.info("SYNC COMPLETED")
    logger.info(f"Matched records: {stats['matched']}")
    logger.info(f"Updated records: {stats['updated']}")
    logger.info(f"Unchanged records: {stats['unchanged']}")
    logger.info(f"Error count: {stats['errors']}")
    logger.info(f"Total batches: {stats['batches']}")
    logger.info("="*50)
    
    return stats

def generate_diff_report(output_path="sync_diff_report.txt"):
    """
    Generate a detailed report of all differences found between the two tables.
    """
    telekom_data, airtable_data = load_data_from_sqlite()
    
    report_lines = [
        "="*80,
        f"SYNC DIFF REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "="*80,
        ""
    ]
    
    total_diffs = 0
    field_diff_counts = {field: 0 for field in FIELD_MAPPING.values()}
    
    for fol_id, telekom_record in telekom_data.items():
        if fol_id in airtable_data:
            airtable_record = airtable_data[fol_id]
            differences = compare_records(telekom_record, airtable_record)
            
            if differences:
                total_diffs += 1
                report_lines.append(f"FOL-ID: {fol_id}")
                report_lines.append("-"*40)
                
                for api_field, new_value in differences.items():
                    # Map back to local field name
                    for local_field, api_name in AIRTABLE_FIELD_NAMES.items():
                        if api_name == api_field:
                            airtable_field = local_field
                            telekom_field = next((t for t, a in FIELD_MAPPING.items() if a == airtable_field), None)
                            
                            old_value = airtable_record.get(airtable_field)
                            report_lines.append(f"  {airtable_field}: {old_value} -> {new_value}")
                            field_diff_counts[airtable_field] += 1
                            break
                            
                report_lines.append("")
    
    # Add summary statistics
    report_lines.append("="*80)
    report_lines.append(f"SUMMARY: {total_diffs} records with differences")
    report_lines.append("-"*40)
    report_lines.append("Field-level differences:")
    for field, count in sorted(field_diff_counts.items(), key=lambda x: x[1], reverse=True):
        if count > 0:
            report_lines.append(f"  {field}: {count}")
    report_lines.append("="*80)
    
    # Write to file
    with open(output_path, 'w') as f:
        f.write('\n'.join(report_lines))
    
    logger.info(f"Diff report written to {output_path}")
    return total_diffs

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync changes from property_data to Airtable')
    parser.add_argument('--report-only', action='store_true', help='Generate diff report without syncing')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for Airtable updates')
    parser.add_argument('--max-records', type=int, help='Maximum number of records to update (for testing/debugging)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        if args.report_only:
            logger.info("Generating diff report only (no syncing)...")
            total_diffs = generate_diff_report()
            logger.info(f"Found {total_diffs} records with differences. See report for details.")
        else:
            logger.info("Beginning sync process...")
            generate_diff_report()  # Always generate a report for reference
            
            if args.max_records:
                logger.info(f"Limited to maximum {args.max_records} records for testing/debugging")
                
            stats = sync_changes_to_airtable(
                batch_size=args.batch_size,
                max_records=args.max_records
            )
            
            logger.info(f"Sync completed. Updated {stats['updated']} records in {stats['batches']} batches.")
    except Exception as e:
        logger.error(f"Sync process failed with error: {str(e)}")
        raise
