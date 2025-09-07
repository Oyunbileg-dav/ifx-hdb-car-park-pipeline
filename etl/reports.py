from etl.db import get_conn
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

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
            SELECT DISTINCT ON (carpark_number, lot_type)
                carpark_number, lot_type, total_lots, available_lots
            FROM raw_carpark_current_availability
            WHERE total_lots IS NOT NULL AND available_lots IS NOT NULL
            ORDER BY carpark_number, lot_type, ingest_ts_sgt DESC
        ) latest
        """
        df = pd.read_sql(query, conn)
        print(f"Current occupancy analysis completed")
        return df

