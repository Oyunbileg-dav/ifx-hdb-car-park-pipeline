"""
Historical 6PM Utilization Pipeline

This pipeline answers the question:
"How many HDB carparks with electronic parking are utilised at more than 
or equal to 80% capacity on average at approximately 6pm this month?"

It focuses on historical trend analysis for peak hour utilization with
optimized delta loading to only fetch missing data.
"""

from prefect import flow, task
from prefect.utilities.annotations import quote
from etl.extract import fetch_carpark_availability_6pm_historical, fetch_carpark_availability_6pm_historical_full, fetch_carpark_info
from etl.transform import transform_carpark_availability_6pm_historical, transform_carpark_info
from etl.load import load_carpark_availability_6pm_last_30days, load_carpark_info, cleanup_old_historical_data
from etl.reports import get_6pm_high_utilization_carparks
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

@task
def extract_6pm_historical_data(force_full_refresh=False):
    """Extract historical 6pm carpark availability data (delta loading by default)"""
    if force_full_refresh:
        return fetch_carpark_availability_6pm_historical_full()
    else:
        return fetch_carpark_availability_6pm_historical()

@task
def extract_carpark_reference_info():
    """Extract carpark reference information"""
    return fetch_carpark_info()

@task
def transform_6pm_historical_data(raw_data):
    """Transform historical 6pm availability data"""
    return transform_carpark_availability_6pm_historical(raw_data)

@task
def transform_reference_info(raw_data):
    """Transform carpark reference data"""
    return transform_carpark_info(raw_data)

@task
def load_6pm_historical_data(records, use_delta_loading=True):
    """Load historical 6pm data to database with delta loading support"""
    return load_carpark_availability_6pm_last_30days(records, clear_existing=not use_delta_loading)

@task
def load_reference_info(records):
    """Load carpark reference data to database"""
    return load_carpark_info(records)

@task
def cleanup_old_data():
    """Clean up historical data older than 30 days"""
    return cleanup_old_historical_data(days_to_keep=30)

@task
def analyze_6pm_high_utilization():
    """Analyze 6pm high utilization carparks and return results"""
    summary_result, detail_result = get_6pm_high_utilization_carparks()
    
    if not summary_result.empty:
        high_util_count = summary_result.iloc[0]['high_utilization_carparks']
        very_high_util_count = summary_result.iloc[0]['very_high_utilization_carparks']
        avg_utilization = summary_result.iloc[0]['overall_avg_utilization']
        max_utilization = summary_result.iloc[0]['max_utilization']
        
        print(f"\n🌆 6PM HIGH UTILIZATION ANALYSIS")
        print(f"=" * 60)
        print(f"📊 Carparks with ≥80% utilization: {high_util_count}")
        print(f"🔥 Carparks with ≥90% utilization: {very_high_util_count}")
        print(f"📈 Average utilization (high-util): {avg_utilization:.1f}%")
        print(f"🏆 Maximum utilization: {max_utilization:.1f}%")
        print(f"=" * 60)
        
        if not detail_result.empty:
            print(f"\n🏅 TOP HIGH-UTILIZATION CARPARKS:")
            for idx, row in detail_result.head(5).iterrows():
                print(f"  {row['carpark_number']} - {row['avg_utilization_percent']}% "
                      f"({row['data_points']} data points)")
    
    return summary_result, detail_result

@flow(name="Historical 6PM Utilization Analysis")
def historical_6pm_pipeline(force_full_refresh=False):
    """
    Pipeline to answer: How many HDB carparks with electronic parking are 
    utilised at ≥80% capacity on average at approximately 6pm this month?
    
    This pipeline:
    1. Checks existing historical data and only fetches missing dates (delta loading)
    2. Extracts carpark reference information
    3. Transforms and loads the data efficiently
    4. Cleans up old data (>30 days)
    5. Analyzes monthly 6pm utilization patterns for electronic parking systems
    6. Identifies carparks with high utilization (≥80% capacity)
    
    Args:
        force_full_refresh: If True, ignores existing data and fetches full 30 days
    """
    print("🌆 Starting Historical 6PM Utilization Pipeline with Delta Loading...")
    
    if force_full_refresh:
        print("⚠️  Force full refresh mode enabled - will fetch all 30 days")
    
    # Extract data (delta loading by default)
    historical_data = extract_6pm_historical_data(force_full_refresh)
    reference_data = extract_carpark_reference_info()
    
    # Transform data
    historical_records = transform_6pm_historical_data(quote(historical_data))
    reference_records = transform_reference_info(quote(reference_data))
    
    # Load data with delta loading (unless force_full_refresh is True)
    print(f"Loading {len(historical_records)} historical records with delta loading: {not force_full_refresh}")
    load_6pm_historical_data(quote(historical_records), use_delta_loading=not force_full_refresh)
    load_reference_info(quote(reference_records))
    
    # Clean up old data
    cleanup_old_data()
    
    # Analyze and report
    summary_results, detail_results = analyze_6pm_high_utilization()
    
    print("✅ Historical 6PM Utilization Pipeline completed!")
    return summary_results, detail_results

@flow(name="Historical 6PM Analysis - Full Refresh")
def historical_6pm_pipeline_full_refresh():
    """Force a complete refresh of all historical data (30 days)"""
    print("🔄 Running Historical 6PM Pipeline with FULL REFRESH...")
    return historical_6pm_pipeline(force_full_refresh=True)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["full", "refresh", "force"]:
        print("🔄 Running with full refresh...")
        historical_6pm_pipeline_full_refresh()
    else:
        print("⚡ Running with delta loading...")
        historical_6pm_pipeline()