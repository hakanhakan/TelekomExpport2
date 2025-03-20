#!/usr/bin/env python3
import os
import sqlite3
from pyairtable import Table, Api
from tabulate import tabulate
from dotenv import load_dotenv
import re
import requests

load_dotenv()
# Configuration: Replace these values with your actual Airtable credentials and table details.
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")  # or set your key directly
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

# Connect to Airtable using the new PyAirtable API.
def fetch_airtable_records():
    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, TABLE_NAME)
    records = table.all()
    # Each record is a dict with keys 'id' and 'fields'
    return records

# Create a local SQLite table to sync the mapping.
def create_airtable_sync_table(db_path="extraction.db"):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS airtable_sync (
            fol_id TEXT PRIMARY KEY,
            airtable_record_id TEXT,
            area TEXT,
            building TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

# Sync Airtable records with the local SQLite table.
def sync_airtable_records(db_path="extraction.db"):
    records = fetch_airtable_records()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Ensure the sync table exists.
    create_airtable_sync_table(db_path)
    
    for record in records:
        record_id = record.get("id")
        fields = record.get("fields", {})
        # Assume 'fol_id', 'area', and 'building' are fields in Airtable.
        fol_id = fields.get("fol_id")
        area = fields.get("area")
        building = fields.get("building")
        
        if fol_id:
            # Use an UPSERT to store or update the mapping.
            cursor.execute("""
                INSERT INTO airtable_sync (fol_id, airtable_record_id, area, building)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(fol_id) DO UPDATE SET
                    airtable_record_id=excluded.airtable_record_id,
                    area=excluded.area,
                    building=excluded.building,
                    last_updated=CURRENT_TIMESTAMP
            """, (fol_id, record_id, area, building))
            print(f"Synced record: fol_id={fol_id}, Airtable ID={record_id}")
        else:
            print(f"Warning: Record {record_id} missing fol_id. Skipping.")
    
    conn.commit()
    # Optionally, display the sync table contents.
    cursor.execute("SELECT * FROM airtable_sync")
    rows = cursor.fetchall()
    print("Current Airtable Sync Table:")
    print(tabulate(rows, headers=["fol_id", "airtable_record_id", "area", "building", "last_updated"], tablefmt="pretty"))
    conn.close()

def print_airtable_schema():
    """Fetches Airtable records and prints the union of all field keys as the schema."""
    try:
        records = fetch_airtable_records()
        schema = set()
        for record in records:
            fields = record.get('fields', {})
            schema.update(fields.keys())
        if schema:
            print("Airtable Schema (fields):")
            for field in sorted(schema):
                print(f" - {field}")
        else:
            print("No fields found in Airtable records.")
    except Exception as e:
        print(f"Error fetching schema: {e}")

def fetch_airtable_metadata():
    """Fetch metadata (including field types) for all tables in a given Airtable base using the Metadata API."""
    url = f"https://api.airtable.com/v0/meta/bases/{BASE_ID}/tables"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def print_airtable_schema_with_types():
    """Fetches Airtable metadata and prints each table's fields with their types, including related table information if available."""
    try:
        metadata = fetch_airtable_metadata()
        tables = metadata.get("tables", [])
        if not tables:
            print("No tables found in metadata.")
            return
        for table in tables:
            table_name = table.get("name")
            print(f"Table: {table_name}")
            fields = table.get("fields", [])
            if fields:
                for field in fields:
                    field_name = field.get("name")
                    field_type = field.get("type")
                    output = f"  Field: {field_name} (Type: {field_type})"
                    # Check for related table info in field options
                    options = field.get("options", {})
                    if options:
                        linked_table_id = options.get("linkedTableId")
                        if linked_table_id:
                            output += f" - Linked Table ID: {linked_table_id}"
                    print(output)
            else:
                print("  No fields found for this table.")
    except Exception as e:
        print(f"Error fetching metadata: {e}")

def get_area_record(area_name):
    """
    Fetch the record from the 'Areas' table where the 'Name' field matches area_name.
    Returns the record (dict) if found, or None.
    """
    try:
        api = Api(AIRTABLE_API_KEY)
        areas_table = api.table(BASE_ID, "Areas")
        # Debug: Print total records in Areas table.
        all_records = areas_table.all()
        print(f"Total records in Areas table: {len(all_records)}")
        
        # Try using filterByFormula
        formula = f"{{Name}}='{area_name}'"
        print(f"Using filter formula: {formula}")  # Debug print
        try:
            records = areas_table.all(filterByFormula=formula)
        except Exception as e:
            print(f"Filter by formula failed with exception: {e}")
            # Fallback: manually filter the records
            records = [r for r in all_records if r.get('fields', {}).get('Name') == area_name]
            print(f"Manually filtered records count: {len(records)}")
        
        print(f"Filtered records count: {len(records)}")
        if records:
            return records[0]
        else:
            print(f"No area record found for area name: {area_name}")
            return None
    except Exception as e:
        print(f"Error fetching area record: {e}")
        return None

def create_buildings_table(db_path="extraction.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # First check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='buildings'")
    table_exists = cursor.fetchone() is not None
    
    if not table_exists:
        # If table doesn't exist, create it with all columns
        conn.execute("""
            CREATE TABLE buildings (
                record_id TEXT PRIMARY KEY,
                area_record_id TEXT,
                building_name TEXT,
                extra_field_1 TEXT,
                extra_field_2 TEXT,
                extra_field_3 TEXT,
                first_name TEXT,
                last_name TEXT,
                phone_1 TEXT,
                phone_2 TEXT,
                email TEXT,
                homes INTEGER,
                offices INTEGER,
                nvt TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("Created new buildings table with all columns")
    else:
        # Table exists, check if it has all required columns
        required_columns = [
            "extra_field_2", "extra_field_3", "first_name", "last_name",
            "phone_1", "phone_2", "email", "homes", "offices", "nvt"
        ]
        
        # Get existing columns
        cursor.execute("PRAGMA table_info(buildings)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        
        # Add missing columns
        for column in required_columns:
            if column not in existing_columns:
                column_type = "INTEGER" if column in ["homes", "offices"] else "TEXT"
                try:
                    cursor.execute(f"ALTER TABLE buildings ADD COLUMN {column} {column_type}")
                    print(f"Added missing column {column} to buildings table")
                except sqlite3.OperationalError as e:
                    print(f"Error adding column {column}: {e}")
    
    conn.commit()
    conn.close()

def sync_buildings_for_area(area_name, db_path="extraction.db"):
    """
    For a given area name, fetch the area's record from the 'Areas' table.
    Then, query the 'Objects' table for records linked to that area (via the 'Area' field)
    and with {Type} equal to 'building'. UPSERT these records into a local SQLite 'buildings' table.
    """
    area_record = get_area_record(area_name)
    if not area_record:
        return

    area_record_id = area_record.get("id")
    if not area_record_id:
        print("Area record does not have an ID.")
        return

    try:
        api = Api(AIRTABLE_API_KEY)
        objects_table = api.table(BASE_ID, "Objects")
        # Try fetching building records using filterByFormula. Adjust the type value as needed; here we assume 'Building' is correct.
        formula = '{Type}="Building"'
        try:
            building_records = objects_table.all(filterByFormula=formula)
            print(f"Total building records fetched using filter: {len(building_records)}")
        except Exception as filter_exception:
            print(f"Filter by formula for building records failed with exception: {filter_exception}")
            print("Falling back to manual filtering from all records...")
            all_records = objects_table.all()
            building_records = [r for r in all_records if r.get('fields', {}).get('Type') == "Building"]
            print(f"Total building records fetched after manual filtering: {len(building_records)}")

        # Manually filter the records that have the area_record_id in their 'Area' field (which should be a list).
        building_records = [r for r in building_records if area_record_id in r.get('fields', {}).get('Area', [])]
        print(f"Building records after manual filtering by area: {len(building_records)}")

        if not building_records:
            print(f"No building records found for area: {area_name}")
            return
    except Exception as e:
        print(f"Error fetching building records: {e}")
        return

    # Ensure the local 'buildings' table exists.
    create_buildings_table(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for record in building_records:
        record_id = record.get("id")
        fields = record.get("fields", {})
        
        # Retrieve all needed fields from Airtable
        building_name = fields.get("Name")
        extra_field_1 = fields.get("Extra field 1")
        extra_field_1 = extract_fol_id(extra_field_1)
        
        # Get the additional fields
        extra_field_2 = fields.get("Extra field 2")
        extra_field_3 = fields.get("Extra field 3")
        first_name = fields.get("First name")
        last_name = fields.get("Last name")
        phone_1 = fields.get("Phone 1")
        phone_2 = fields.get("Phone 2")
        email = fields.get("Email")
        
        # Convert numeric fields if available, otherwise default to 0
        try:
            homes = int(fields.get("HOMES", 0))
        except (ValueError, TypeError):
            homes = 0
            
        try:
            offices = int(fields.get("OFFICES", 0))
        except (ValueError, TypeError):
            offices = 0
            
        nvt = fields.get("NVT")
        
        if record_id and building_name:
            try:
                cursor.execute("""
                    INSERT INTO buildings (
                        record_id, area_record_id, building_name, extra_field_1,
                        extra_field_2, extra_field_3, first_name, last_name, 
                        phone_1, phone_2, email, homes, offices, nvt
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(record_id) DO UPDATE SET
                        area_record_id=excluded.area_record_id,
                        building_name=excluded.building_name,
                        extra_field_1=excluded.extra_field_1,
                        extra_field_2=excluded.extra_field_2,
                        extra_field_3=excluded.extra_field_3,
                        first_name=excluded.first_name,
                        last_name=excluded.last_name,
                        phone_1=excluded.phone_1,
                        phone_2=excluded.phone_2,
                        email=excluded.email,
                        homes=excluded.homes,
                        offices=excluded.offices,
                        nvt=excluded.nvt,
                        last_updated=CURRENT_TIMESTAMP
                """, (
                    record_id, area_record_id, building_name, extra_field_1,
                    extra_field_2, extra_field_3, first_name, last_name,
                    phone_1, phone_2, email, homes, offices, nvt
                ))
                print(f"Synced building: {building_name} (Record ID: {record_id})")
            except sqlite3.Error as e:
                print(f"Error syncing building {building_name}: {e}")
                # If there was an error, we'll try a simpler insert with only the original columns
                try:
                    print("Attempting fallback insert with original columns only...")
                    cursor.execute("""
                        INSERT INTO buildings (record_id, area_record_id, building_name, extra_field_1)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(record_id) DO UPDATE SET
                            area_record_id=excluded.area_record_id,
                            building_name=excluded.building_name,
                            extra_field_1=excluded.extra_field_1,
                            last_updated=CURRENT_TIMESTAMP
                    """, (record_id, area_record_id, building_name, extra_field_1))
                    print(f"Fallback sync successful for building: {building_name}")
                except sqlite3.Error as e2:
                    print(f"Fallback sync also failed: {e2}")
        else:
            print(f"Skipping record with missing ID or Name: {record}")
    conn.commit()
    # Optionally, display the synced buildings
    cursor.execute("SELECT * FROM buildings")
    rows = cursor.fetchall()
    print("Current Buildings Table:")
    print(tabulate(rows, headers=[
        "record_id", "area_record_id", "building_name", "extra_field_1", 
        "extra_field_2", "extra_field_3", "first_name", "last_name", 
        "phone_1", "phone_2", "email", "homes", "offices", "nvt", "last_updated"
    ], tablefmt="pretty"))
    conn.close()

def extract_fol_id(text):
    """
    Extracts and returns the numeric part from a string like 'FoL-ID: 1000004314821'.
    If no match is found, returns the original text.
    """
    if text:
        match = re.search(r"FoL-ID:\s*(\d+)", text)
        if match:
            return match.group(1)
    return text

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == 'schema':
            print("Fetching and printing Airtable schema...")
            print_airtable_schema()
        elif command == 'schema-types':
            print("Fetching and printing Airtable schema with field types...")
            print_airtable_schema_with_types()
        elif command == 'sync-buildings':
            if len(sys.argv) < 3:
                print("Usage: uv run airtable_connector.py sync-buildings \"Area Name\"")
            else:
                area_name = sys.argv[2]
                print(f"Syncing building records for area: {area_name} ...")
                sync_buildings_for_area(area_name)
        else:
            print(f"Unknown argument: {sys.argv[1]}")
    else:
        print("Fetching and syncing Airtable records...")
        sync_airtable_records()
