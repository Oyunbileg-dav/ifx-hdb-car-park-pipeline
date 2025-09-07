from etl.db import get_conn
import pandas as pd
from datetime import datetime, date
import json

def load_carpark_current_availability(records):
    """Load carpark availability data to database"""
    if not records:
        print("No availability records to load")
        return
    
    with get_conn() as conn:
        cur = conn.cursor()
        
        loaded_count = 0
        for record in records:
            try:
                cur.execute("""
                    INSERT INTO raw_carpark_current_availability 
                    (ingest_ts_sgt, carpark_number, total_lots, lot_type, 
                     available_lots, update_datetime_sg, payload_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    record['ingest_ts_utc'],
                    record['carpark_number'],
                    record['total_lots'],
                    record['lot_type'],
                    record['available_lots'],
                    record['update_datetime_sg'],
                    json.dumps(record['payload_json'])
                ))
                loaded_count += 1
            except Exception as e:
                print(f"Error loading record {record.get('carpark_number')}: {e}")
                continue
        
        print(f"Loaded {loaded_count} availability records")

def load_carpark_info(records):
    """Load carpark reference data"""
    if not records:
        print("No carpark info records to load")
        return
    
    with get_conn() as conn:
        cur = conn.cursor()
        
        # Clear existing data first
        cur.execute("TRUNCATE TABLE ref_carpark_info")
        
        loaded_count = 0
        for record in records:
            try:
                cur.execute("""
                    INSERT INTO ref_carpark_info 
                    (car_park_no, address, x_coord, y_coord, car_park_type,
                     type_of_parking_system, short_term_parking, free_parking,
                     night_parking, car_park_decks, gantry_height, car_park_basement)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (car_park_no) DO UPDATE SET
                    address = EXCLUDED.address,
                    car_park_type = EXCLUDED.car_park_type,
                    type_of_parking_system = EXCLUDED.type_of_parking_system
                """, (
                    record.get('car_park_no'),
                    record.get('address'),
                    record.get('x_coord'),
                    record.get('y_coord'),
                    record.get('car_park_type'),
                    record.get('type_of_parking_system'),
                    record.get('short_term_parking'),
                    record.get('free_parking'),
                    record.get('night_parking'),
                    record.get('car_park_decks'),
                    record.get('gantry_height'),
                    record.get('car_park_basement')
                ))
                loaded_count += 1
            except Exception as e:
                print(f"Error loading carpark info {record.get('car_park_no')}: {e}")
                continue
        
        print(f"Loaded {loaded_count} carpark info records")

def load_carpark_availability_6pm_last_30days(records):
    """Load historical 6pm carpark availability data (30 days) to database"""
    if not records:
        print("No 6pm historical records to load")
        return
    
    with get_conn() as conn:
        cur = conn.cursor()
        
        # Clear existing historical data first to avoid duplicates
        cur.execute("TRUNCATE TABLE raw_carpark_availability_6pm_last_30days")
        print("Cleared existing 6pm historical data")
        
        loaded_count = 0
        error_count = 0
        
        for record in records:
            try:
                cur.execute("""
                    INSERT INTO raw_carpark_availability_6pm_last_30days 
                    (ingest_ts_sgt, carpark_number, total_lots, lot_type, 
                     available_lots, update_datetime_sg, payload_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (update_datetime_sg, carpark_number) DO UPDATE SET
                    total_lots = EXCLUDED.total_lots,
                    available_lots = EXCLUDED.available_lots,
                    lot_type = EXCLUDED.lot_type,
                    payload_json = EXCLUDED.payload_json
                """, (
                    record['ingest_ts_utc'],
                    record['carpark_number'],
                    record['total_lots'],
                    record['lot_type'],
                    record['available_lots'],
                    record['update_datetime_sg'],
                    json.dumps(record['payload_json'])
                ))
                loaded_count += 1
            except Exception as e:
                print(f"Error loading 6pm record {record.get('carpark_number')} at {record.get('update_datetime_sg')}: {e}")
                error_count += 1
                continue
        
        print(f"Loaded {loaded_count} historical 6pm records")
        if error_count > 0:
            print(f"Failed to load {error_count} records due to errors")