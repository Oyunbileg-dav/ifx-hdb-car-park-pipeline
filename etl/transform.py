from datetime import datetime, timezone, timedelta
import pandas as pd
import json

def transform_carpark_current_availability(raw_data, max_age_hours=10):
    """Transform raw carpark availability data, filtering out stale records"""
    items = raw_data.get('items', [])
    if not items:
        print("No availability data to transform")
        return []
    
    records = []
    # Use Singapore timezone for all datetime operations
    singapore_tz = timezone(timedelta(hours=8))
    ingest_ts = datetime.now(singapore_tz)
    cutoff_time = ingest_ts - timedelta(hours=max_age_hours)
    
    stale_count = 0
    
    for item in items:
        carpark_data = item.get('carpark_data', [])
        update_datetime_str = item.get('timestamp')
        
        # Parse and validate timestamp
        try:
            if update_datetime_str:
                # Parse timestamp and convert to Singapore time for comparison
                update_datetime = datetime.fromisoformat(update_datetime_str.replace('Z', '+00:00'))
                if update_datetime.tzinfo is None:
                    # Assume Singapore time if no timezone info
                    update_datetime = update_datetime.replace(tzinfo=singapore_tz)
                else:
                    # Convert to Singapore time for consistent comparison
                    update_datetime = update_datetime.astimezone(singapore_tz)
                
                # Skip if data is too old
                if update_datetime < cutoff_time:
                    stale_count += len(carpark_data)
                    continue
            else:
                # Skip records without timestamp
                stale_count += len(carpark_data)
                continue
                
        except (ValueError, TypeError) as e:
            print(f"Invalid timestamp format: {update_datetime_str}, skipping")
            stale_count += len(carpark_data)
            continue
        
        for carpark in carpark_data:
            carpark_number = carpark.get('carpark_number')
            carpark_info = carpark.get('carpark_info', [])
            
            for info in carpark_info:
                record = {
                    'ingest_ts_utc': ingest_ts.replace(tzinfo=None),
                    'carpark_number': carpark_number,
                    'total_lots': int(info.get('total_lots', 0)) if info.get('total_lots') else None,
                    'lot_type': info.get('lot_type'),
                    'available_lots': int(info.get('lots_available', 0)) if info.get('lots_available') else None,
                    'update_datetime_sg': update_datetime_str,
                    'payload_json': carpark
                }
                records.append(record)
    
    print(f"Transformed {len(records)} current availability records")
    if stale_count > 0:
        print(f"Filtered out {stale_count} stale records (older than {max_age_hours} hours)")
    
    return records

def transform_carpark_info(raw_data):
    """Transform HDB carpark reference data"""
    records = raw_data.get('result', {}).get('records', [])
    print(f"Transformed {len(records)} carpark info records")
    return records

def transform_carpark_availability_6pm_historical(raw_data):
    """Transform historical carpark availability data for 6pm analysis, filtering out records older than 30 days"""
    items = raw_data.get('items', [])
    if not items:
        print("No historical 6pm availability data to transform")
        return []
    
    records = []
    # Use Singapore timezone for all datetime operations
    singapore_tz = timezone(timedelta(hours=8))
    ingest_ts = datetime.now(singapore_tz)
    cutoff_time = ingest_ts - timedelta(days=30)
    
    old_count = 0
    
    for item in items:
        carpark_data = item.get('carpark_data', [])
        update_datetime_str = item.get('timestamp')
        
        # Parse and validate timestamp
        try:
            if update_datetime_str:
                # Parse timestamp and convert to Singapore time for comparison
                update_datetime = datetime.fromisoformat(update_datetime_str.replace('Z', '+00:00'))
                if update_datetime.tzinfo is None:
                    # Assume Singapore time if no timezone info
                    update_datetime = update_datetime.replace(tzinfo=singapore_tz)
                else:
                    # Convert to Singapore time for consistent comparison
                    update_datetime = update_datetime.astimezone(singapore_tz)
                
                # Skip if data is older than 30 days
                if update_datetime < cutoff_time:
                    old_count += len(carpark_data)
                    continue
            else:
                # Skip records without timestamp
                print(f"Skipping record without timestamp")
                continue
                
        except (ValueError, TypeError) as e:
            print(f"Invalid timestamp format: {update_datetime_str}, skipping")
            continue
        
        for carpark in carpark_data:
            carpark_number = carpark.get('carpark_number')
            carpark_info = carpark.get('carpark_info', [])
            
            for info in carpark_info:
                record = {
                    'ingest_ts_utc': ingest_ts.replace(tzinfo=None),
                    'carpark_number': carpark_number,
                    'total_lots': int(info.get('total_lots', 0)) if info.get('total_lots') else None,
                    'lot_type': info.get('lot_type'),
                    'available_lots': int(info.get('lots_available', 0)) if info.get('lots_available') else None,
                    'update_datetime_sg': update_datetime_str,
                    'payload_json': carpark
                }
                records.append(record)
    
    print(f"Transformed {len(records)} historical 6pm availability records")
    if old_count > 0:
        print(f"Filtered out {old_count} records older than 30 days")
    
    return records