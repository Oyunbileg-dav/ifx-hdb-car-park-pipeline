from etl.db import get_conn
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

def generate_analysis_report():
    """Generate analysis report answering the business questions"""
    print("Generating analysis report...")
    
    # Question 1: Current occupancy
    current_occupancy = get_current_occupancy()
    
    print(current_occupancy)
    
    print("Analysis report generated successfully!")

def get_current_occupancy():
    """Answer: How many HDB carpark lots are currently occupied?"""
    print("Analyzing current occupancy...")
    
    with get_conn() as conn:
        query = """
        SELECT 
            SUM(total_lots - available_lots) as occupied_lots,
            SUM(total_lots) as total_lots,
            SUM(available_lots) as available_lots,
            ROUND(SUM(total_lots - available_lots)::numeric / NULLIF(SUM(total_lots), 0) * 100, 2) as occupancy_rate
        FROM (
            SELECT DISTINCT ON (latest.carpark_number, latest.lot_type)
                latest.carpark_number, latest.lot_type, latest.total_lots, latest.available_lots
            FROM raw_carpark_current_availability latest
            INNER JOIN ref_carpark_info info ON latest.carpark_number = info.car_park_no
            WHERE latest.total_lots IS NOT NULL 
              AND latest.available_lots IS NOT NULL
              AND latest.total_lots > 0
            ORDER BY latest.carpark_number, latest.lot_type, latest.ingest_ts_sgt DESC
        ) filtered_latest
        """
        df = pd.read_sql(query, conn)
        print(f"Current occupancy analysis completed")
        return df

def get_6pm_high_utilization_carparks():
    """Answer: How many HDB carparks with electronic parking are utilised at >=80% capacity on average at approximately 6pm this month?"""
    print("Analyzing 6pm high utilization carparks...")
    
    with get_conn() as conn:
        query = """
        WITH carpark_6pm_utilization AS (
            SELECT 
                hist.carpark_number,
                info.address,
                AVG(
                    CASE 
                        WHEN hist.total_lots > 0 
                        THEN ((hist.total_lots - hist.available_lots)::numeric / hist.total_lots * 100)
                        ELSE 0 
                    END
                ) as avg_utilization_percent,
                COUNT(*) as data_points,
                AVG(hist.total_lots) as avg_total_lots,
                AVG(hist.available_lots) as avg_available_lots
            FROM raw_carpark_availability_6pm_last_30days hist
            INNER JOIN ref_carpark_info info ON hist.carpark_number = info.car_park_no
            WHERE hist.total_lots IS NOT NULL 
              AND hist.available_lots IS NOT NULL
              AND hist.total_lots > 0
              AND info.type_of_parking_system = 'ELECTRONIC PARKING'
              AND hist.update_datetime_sg >= DATE_TRUNC('month', CURRENT_DATE)  -- This month only
            GROUP BY hist.carpark_number, info.address
        )
        SELECT 
            COUNT(*) as high_utilization_carparks,
            COUNT(*) FILTER (WHERE avg_utilization_percent >= 90) as very_high_utilization_carparks,
            AVG(avg_utilization_percent) as overall_avg_utilization,
            MAX(avg_utilization_percent) as max_utilization,
            MIN(avg_utilization_percent) as min_utilization
        FROM carpark_6pm_utilization
        WHERE avg_utilization_percent >= 80.0
        """
        
        df = pd.read_sql(query, conn)
        
        # Also get detailed breakdown
        detail_query = """
        WITH carpark_6pm_utilization AS (
            SELECT 
                hist.carpark_number,
                info.address,
                AVG(
                    CASE 
                        WHEN hist.total_lots > 0 
                        THEN ((hist.total_lots - hist.available_lots)::numeric / hist.total_lots * 100)
                        ELSE 0 
                    END
                ) as avg_utilization_percent,
                COUNT(*) as data_points,
                AVG(hist.total_lots) as avg_total_lots
            FROM raw_carpark_availability_6pm_last_30days hist
            INNER JOIN ref_carpark_info info ON hist.carpark_number = info.car_park_no
            WHERE hist.total_lots IS NOT NULL 
              AND hist.available_lots IS NOT NULL
              AND hist.total_lots > 0
              AND info.type_of_parking_system = 'ELECTRONIC PARKING'
              AND hist.update_datetime_sg >= DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY hist.carpark_number, info.address
        )
        SELECT 
            carpark_number,
            address,
            ROUND(avg_utilization_percent, 2) as avg_utilization_percent,
            data_points,
            ROUND(avg_total_lots, 0) as avg_total_lots
        FROM carpark_6pm_utilization
        WHERE avg_utilization_percent >= 80.0
        ORDER BY avg_utilization_percent DESC
        LIMIT 10
        """
        
        detail_df = pd.read_sql(detail_query, conn)
        
        print(f"6pm high utilization analysis completed")
        print(f"Summary results:")
        print(df)
        print(f"\nTop 10 highest utilization carparks (>=80%):")
        print(detail_df)
        
        return df, detail_df