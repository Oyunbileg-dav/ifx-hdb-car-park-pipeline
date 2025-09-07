import httpx
import json
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv

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

def fetch_carpark_availability_6pm_historical():
    """Fetch historical carpark availability for 6pm analysis (last 30 days)"""
    url = os.getenv("HDB_CARPARK_API_URL")
    print(f"Fetching historical 6pm carpark availability data...")
    
    all_records = []
    # Use Singapore timezone for date calculations
    singapore_tz = timezone(timedelta(hours=8))
    today = datetime.now(singapore_tz)
    
    # Fetch data for last 30 days at 6pm SGT
    print(f"Today's date in Singapore: {today}")
    
    for days_back in range(30):
        target_date = today - timedelta(days=days_back)
        # Set to 6pm Singapore time
        target_datetime = target_date.replace(hour=18, minute=0, second=0, microsecond=0)
        
        # Format datetime for API (YYYY-MM-DDTHH:MM:SS format) - API expects SGT directly
        datetime_param = target_datetime.strftime('%Y-%m-%dT%H:%M:%S')
        print(f"Requesting data for: {datetime_param} (days_back: {days_back})")
        
        try:
            with httpx.Client() as client:
                params = {"date_time": datetime_param}
                response = client.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                items = data.get('items', [])
                if items:
                    print(f"  Fetched {len(items)} records for {target_date.strftime('%Y-%m-%d')} 6pm SGT")
                    all_records.extend(items)
                else:
                    print(f"  No data available for {target_date.strftime('%Y-%m-%d')} 6pm SGT")
                    
        except Exception as e:
            print(f"  Error fetching data for {target_date.strftime('%Y-%m-%d')}: {e}")
            continue
    
    print(f"Total historical 6pm records fetched: {len(all_records)}")
    
    # Return in same format as regular fetch function
    return {"items": all_records}
