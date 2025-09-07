from etl.db import get_conn
import pandas as pd
from datetime import datetime, date
import json
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

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

def load_carpark_availability_6pm_last_30days(records, clear_existing=False):
    """
    Load historical 6pm carpark availability data to database
    
    Args:
        records: List of records to load
        clear_existing: If True, truncate table before loading (for full refresh)
                       If False, use delta loading with ON CONFLICT handling
    """
    if not records:
        print("No 6pm historical records to load")
        return
    
    with get_conn() as conn:
        cur = conn.cursor()
        
        if clear_existing:
            # Full refresh mode - clear existing data
            print("🔍 DEBUG: About to execute TRUNCATE...")
            cur.execute("TRUNCATE TABLE raw_carpark_availability_6pm_last_30days")
            print("🔍 DEBUG: TRUNCATE executed successfully")
            print("🗑️  Cleared existing 6pm historical data (full refresh mode)")
            
            # Check if table is actually empty
            cur.execute("SELECT COUNT(*) FROM raw_carpark_availability_6pm_last_30days")
            count = cur.fetchone()[0]
            print(f"🔍 DEBUG: Table now has {count} records")
        else:
            # Delta loading mode - use upsert
            print("⚡ Using delta loading mode (incremental updates)")
        
        loaded_count = 0
        error_count = 0
        
        for record in records:
            try:
                if clear_existing:
                    # Use upsert even for full refresh to handle duplicates
                    cur.execute("""
                        INSERT INTO raw_carpark_availability_6pm_last_30days 
                        (ingest_ts_sgt, carpark_number, total_lots, lot_type, 
                         available_lots, update_datetime_sg, payload_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (update_datetime_sg, carpark_number) DO UPDATE SET
                        ingest_ts_sgt = EXCLUDED.ingest_ts_sgt,
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
                else:
                    # Upsert for delta loading
                    cur.execute("""
                        INSERT INTO raw_carpark_availability_6pm_last_30days 
                        (ingest_ts_sgt, carpark_number, total_lots, lot_type, 
                         available_lots, update_datetime_sg, payload_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (update_datetime_sg, carpark_number) DO UPDATE SET
                        ingest_ts_sgt = EXCLUDED.ingest_ts_sgt,
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
                    
                    # Check if it was an update or insert
                    if cur.rowcount > 0:
                        # Check if it was an actual update by looking at the query result
                        loaded_count += 1
                
            except Exception as e:
                print(f"Error loading 6pm record {record.get('carpark_number')} at {record.get('update_datetime_sg')}: {e}")
                error_count += 1
                continue
        
        print(f"📊 Load summary:")
        print(f"  - Records processed: {loaded_count}")
        if error_count > 0:
            print(f"  - Errors encountered: {error_count}")
        
        # Final check - count records in table
        cur.execute("SELECT COUNT(*) FROM raw_carpark_availability_6pm_last_30days")
        final_count = cur.fetchone()[0]
        print(f"🔍 DEBUG: Final table count: {final_count} records")

def load_carpark_availability_6pm_full_refresh(records):
    """Load historical 6pm data with full table refresh (legacy compatibility)"""
    return load_carpark_availability_6pm_last_30days(records, clear_existing=True)

def cleanup_old_historical_data(days_to_keep=30):
    """Remove historical data older than specified days"""
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            
            cur.execute("""
                DELETE FROM raw_carpark_availability_6pm_last_30days 
                WHERE update_datetime_sg < CURRENT_DATE - INTERVAL '%s days'
            """, (days_to_keep,))
            
            deleted_count = cur.rowcount
            if deleted_count > 0:
                print(f"🧹 Cleaned up {deleted_count} old historical records (>{days_to_keep} days)")
            else:
                print(f"✅ No old historical records to clean up")
                
            return deleted_count
            
    except Exception as e:
        print(f"Error cleaning up old historical data: {e}")
        return 0