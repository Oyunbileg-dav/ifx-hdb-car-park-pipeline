import httpx
import json
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
from etl.db import get_conn

load_dotenv()

def fetch_carpark_availability():
    """Fetch real-time carpark availability from HDB API"""
    url = os.getenv("HDB_CARPARK_API_URL")
    print(f"Fetching carpark availability from: {url}")
    
    with httpx.Client() as client:
        response = client.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"Fetched {len(data.get('items', []))} availability records")
        return data

def fetch_carpark_info():
    """Fetch HDB carpark information"""
    url = os.getenv("HDB_CARPARK_INFO_URL")
    print(f"Fetching carpark info from: {url}")
    
    with httpx.Client() as client:
        response = client.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data.get('result', {}).get('records', [])
        print(f"Fetched {len(records)} carpark info records")
        return data

def get_existing_historical_dates():
    """Get dates that already exist in the historical database table"""
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            # Get distinct dates from existing historical data (extract date from timestamp)
            cur.execute("""
                SELECT DISTINCT DATE(update_datetime_sg) as existing_date
                FROM raw_carpark_availability_6pm_last_30days
                WHERE update_datetime_sg IS NOT NULL
                ORDER BY existing_date DESC
            """)
            
            existing_dates = {row[0] for row in cur.fetchall()}
            print(f"Found {len(existing_dates)} existing historical dates in database")
            
            if existing_dates:
                latest_date = max(existing_dates)
                earliest_date = min(existing_dates)
                print(f"  Date range: {earliest_date} to {latest_date}")
            
            return existing_dates
            
    except Exception as e:
        print(f"Error checking existing historical dates: {e}")
        return set()

def fetch_carpark_availability_6pm_historical():
    """Fetch historical carpark availability for 6pm analysis (delta loading)"""
    url = os.getenv("HDB_CARPARK_API_URL")
    print(f"Fetching historical 6pm carpark availability data with delta loading...")
    
    # Use Singapore timezone for date calculations
    singapore_tz = timezone(timedelta(hours=8))
    today = datetime.now(singapore_tz)
    
    # Get existing dates from database
    existing_dates = get_existing_historical_dates()
    
    # Determine which dates we need to fetch
    dates_to_fetch = []
    for days_back in range(30):
        target_date = today - timedelta(days=days_back)
        target_date_only = target_date.date()
        
        if target_date_only not in existing_dates:
            dates_to_fetch.append(target_date)
    
    if not dates_to_fetch:
        print("✅ All historical data up to date - no new dates to fetch")
        return {"items": []}
    
    print(f"📥 Need to fetch {len(dates_to_fetch)} missing dates out of 30 days")
    print(f"📅 Date range to fetch: {min(d.date() for d in dates_to_fetch)} to {max(d.date() for d in dates_to_fetch)}")
    
    all_records = []
    successful_fetches = 0
    
    for target_date in dates_to_fetch:
        # Set to 6pm Singapore time
        target_datetime = target_date.replace(hour=18, minute=0, second=0, microsecond=0)
        
        # Format datetime for API (YYYY-MM-DDTHH:MM:SS format) - API expects SGT directly
        datetime_param = target_datetime.strftime('%Y-%m-%dT%H:%M:%S')
        
        try:
            with httpx.Client() as client:
                params = {"date_time": datetime_param}
                response = client.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                items = data.get('items', [])
                if items:
                    print(f"  ✅ Fetched {len(items)} records for {target_date.strftime('%Y-%m-%d')} 6pm SGT")
                    all_records.extend(items)
                    successful_fetches += 1
                else:
                    print(f"  ⚠️  No data available for {target_date.strftime('%Y-%m-%d')} 6pm SGT")
                    
        except Exception as e:
            print(f"  ❌ Error fetching data for {target_date.strftime('%Y-%m-%d')}: {e}")
            continue
    
    print(f"📊 Delta fetch summary:")
    print(f"  - Total dates needed: {len(dates_to_fetch)}")
    print(f"  - Successfully fetched: {successful_fetches}")
    print(f"  - Total new records: {len(all_records)}")
    
    # Return in same format as regular fetch function
    return {"items": all_records}

def fetch_carpark_availability_6pm_historical_full():
    """Fetch complete historical data (force full refresh) - for manual use"""
    url = os.getenv("HDB_CARPARK_API_URL")
    print(f"Fetching FULL historical 6pm carpark availability data (30 days)...")
    
    all_records = []
    singapore_tz = timezone(timedelta(hours=8))
    today = datetime.now(singapore_tz)
    
    print(f"📅 Today's date in Singapore: {today.strftime('%Y-%m-%d')}")
    
    for days_back in range(30):
        target_date = today - timedelta(days=days_back)
        target_datetime = target_date.replace(hour=18, minute=0, second=0, microsecond=0)
        datetime_param = target_datetime.strftime('%Y-%m-%dT%H:%M:%S')
        
        try:
            with httpx.Client() as client:
                params = {"date_time": datetime_param}
                response = client.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                items = data.get('items', [])
                if items:
                    print(f"  ✅ Fetched {len(items)} records for {target_date.strftime('%Y-%m-%d')} 6pm SGT")
                    all_records.extend(items)
                else:
                    print(f"  ⚠️  No data available for {target_date.strftime('%Y-%m-%d')} 6pm SGT")
                    
        except Exception as e:
            print(f"  ❌ Error fetching data for {target_date.strftime('%Y-%m-%d')}: {e}")
            continue
    
    print(f"📊 Full fetch completed: {len(all_records)} total records")
    return {"items": all_records}