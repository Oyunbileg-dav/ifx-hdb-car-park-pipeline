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
from etl.reports import get_6pm_high_utilization_carparks, get_6pm_scatterplot_data, get_6pm_capacity_buckets
import pandas as pd
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

@task
def generate_6pm_html_report(summary_result, detail_result):
    """Generate HTML report for 6pm historical analysis"""
    print("Generating 6pm historical analysis HTML report...")
    
    # Get additional data for visualizations
    scatterplot_data = get_6pm_scatterplot_data()
    capacity_buckets_data = get_6pm_capacity_buckets()
    
    # Create HTML report
    html_content = create_6pm_html_report(summary_result, detail_result, scatterplot_data, capacity_buckets_data)
    
    # Save to file
    report_path = "reports/historical_6pm_report.html"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"📊 6PM Historical Analysis Report saved to: {report_path}")
    return report_path

def create_6pm_html_report(summary_df, detail_df, scatterplot_df, capacity_buckets_df):
    """Create HTML report for 6pm historical analysis"""
    
    # Extract summary data
    if not summary_df.empty:
        high_util_count = summary_df.iloc[0]['high_utilization_carparks']
        very_high_util_count = summary_df.iloc[0]['very_high_utilization_carparks']
        avg_utilization = summary_df.iloc[0]['overall_avg_utilization']
        max_utilization = summary_df.iloc[0]['max_utilization']
        min_utilization = summary_df.iloc[0]['min_utilization']
    else:
        high_util_count = very_high_util_count = avg_utilization = max_utilization = min_utilization = 0
    
    # Convert DataFrames to HTML tables
    summary_table = summary_df.to_html(index=False, classes='data-table') if not summary_df.empty else "<p>No data available</p>"
    detail_table = detail_df.to_html(index=False, classes='data-table') if not detail_df.empty else "<p>No data available</p>"
    
    # Create scatterplot data for JavaScript
    scatterplot_json = scatterplot_df.to_json(orient='records') if not scatterplot_df.empty else "[]"
    
    # Group capacity buckets data by bucket
    capacity_buckets_html = ""
    if not capacity_buckets_df.empty:
        for bucket in ['Small (1-10 lots)', 'Medium (11-50 lots)', 'Large (51-100 lots)', 'Very Large (100+ lots)']:
            bucket_data = capacity_buckets_df[capacity_buckets_df['capacity_bucket'] == bucket]
            if not bucket_data.empty:
                bucket_table = bucket_data[['carpark_number', 'address', 'avg_utilization_percent', 'avg_total_lots', 'data_points']].to_html(index=False, classes='data-table')
                capacity_buckets_html += f"""
                <div class="capacity-bucket">
                    <h3>{bucket}</h3>
                    {bucket_table}
                </div>
                """
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>HDB 6PM Historical Analysis Report</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                overflow: hidden;
            }}
            .header {{
                background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }}
            .header h1 {{
                margin: 0;
                font-size: 2.5em;
                font-weight: 300;
            }}
            .header p {{
                margin: 10px 0 0 0;
                opacity: 0.9;
                font-size: 1.1em;
            }}
            .content {{
                padding: 40px;
            }}
            .summary-cards {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }}
            .card {{
                background: #f8f9fa;
                border: 1px solid #e9ecef;
                border-radius: 10px;
                padding: 25px;
                text-align: center;
                transition: transform 0.2s ease;
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }}
            .card:hover {{
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }}
            .card h3 {{
                margin: 0 0 10px 0;
                font-size: 1.1em;
                color: #6c757d;
                text-transform: uppercase;
                letter-spacing: 1px;
            }}
            .card .value {{
                font-size: 2.5em;
                font-weight: bold;
                margin: 10px 0;
                color: #2c3e50;
            }}
            .section {{
                margin-bottom: 40px;
            }}
            .section h2 {{
                color: #2c3e50;
                border-bottom: 3px solid #007bff;
                padding-bottom: 10px;
                margin-bottom: 20px;
            }}
            .chart-container {{
                background: #f8f9fa;
                border: 1px solid #e9ecef;
                border-radius: 10px;
                padding: 25px;
                margin-bottom: 30px;
            }}
            .data-table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 15px;
                background: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }}
            .data-table th {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 15px;
                text-align: left;
                font-weight: 600;
            }}
            .data-table td {{
                padding: 12px 15px;
                border-bottom: 1px solid #dee2e6;
            }}
            .data-table tr:hover {{
                background-color: #f8f9fa;
            }}
            .capacity-bucket {{
                margin-bottom: 30px;
            }}
            .capacity-bucket h3 {{
                color: #2c3e50;
                background: linear-gradient(135deg, #007bff 0%, #0056b3 100%);
                color: white;
                padding: 15px;
                margin: 0 0 15px 0;
                border-radius: 8px;
                text-align: center;
            }}
            .timestamp {{
                text-align: center;
                color: #6c757d;
                font-size: 0.9em;
                margin-top: 30px;
                padding-top: 20px;
                border-top: 1px solid #dee2e6;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🌆 HDB 6PM Historical Analysis</h1>
                <p>Electronic Parking Utilization Analysis for Peak Hours</p>
                <p>This report shows the 30-day historical utilization of HDB carparks with electronic parking at 6pm.</p>
            </div>
            
            <div class="content">
                <!-- Summary Cards -->
                <div class="summary-cards">
                    <div class="card">
                        <h3>High Utilization (≥80%)</h3>
                        <div class="value">{high_util_count}</div>
                    </div>
                    <div class="card">
                        <h3>Very High Utilization (≥90%)</h3>
                        <div class="value">{very_high_util_count}</div>
                    </div>
                    <div class="card">
                        <h3>Average Utilization</h3>
                        <div class="value">{avg_utilization:.1f}%</div>
                    </div>
                    <div class="card">
                        <h3>Max Utilization</h3>
                        <div class="value">{max_utilization:.1f}%</div>
                    </div>
                </div>
                
                <!-- Scatterplot -->
                <div class="section">
                    <h2>📊 Capacity vs Utilization Scatterplot</h2>
                    <div class="chart-container">
                        <canvas id="scatterplotChart" width="800" height="400"></canvas>
                    </div>
                </div>
                
                <!-- Summary Table -->
                <div class="section">
                    <h2>📋 Summary Statistics</h2>
                    {summary_table}
                </div>
                
                <!-- Top 10 High Utilization -->
                <div class="section">
                    <h2>🏆 Top 10 Highest Utilization Carparks (≥80%)</h2>
                    {detail_table}
                </div>
                
                <!-- Capacity Buckets -->
                <div class="section">
                    <h2>📈 Top Carparks by Capacity Size</h2>
                    {capacity_buckets_html}
                </div>

                <div style="margin-top: 25px; padding: 20px; background: white; border-radius: 8px; border-left: 4px solid #007bff;">
                    <h4 style="margin: 0 0 10px 0; color: #007bff;">📋 Business Question Answered:</h4>
                    <p style="margin: 0; color: #2c3e50; font-style: italic;">
                        "How many HDB carparks with electronic parking are utilised at more than or equal to 80%capacity on average at approximately 6pm this month?"
                    </p>
                    <p style="margin: 10px 0 0 0; color: #6c757d; font-size: 0.95em;">
                        <strong>Answer:</strong> Based on the analysis of electronic parking systems over the past 30 days at 6pm, 
                        {high_util_count} carparks consistently achieve ≥80% utilization, with {very_high_util_count} reaching ≥90% capacity. 
                        The scatterplot reveals utilization patterns across different carpark sizes, while the capacity buckets 
                        identify top performers in each size category. This data helps identify high-demand locations and 
                        optimize parking resource allocation during peak evening hours.
                    </p>
                </div>
                
                <div class="timestamp">
                    Report generated on {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')} SGT
                </div>
            </div>
        </div>
        
        <script>
            // Scatterplot Chart
            const scatterplotData = {scatterplot_json};
            
            const ctx = document.getElementById('scatterplotChart').getContext('2d');
            new Chart(ctx, {{
                type: 'scatter',
                data: {{
                    datasets: [{{
                        label: 'Carpark Utilization',
                        data: scatterplotData.map(item => ({{
                            x: item.avg_total_lots,
                            y: item.avg_utilization_percent,
                            carpark_number: item.carpark_number,
                            address: item.address,
                            data_points: item.data_points
                        }})),
                        backgroundColor: 'rgba(52, 152, 219, 0.6)',
                        borderColor: 'rgba(52, 152, 219, 1)',
                        borderWidth: 2,
                        pointRadius: 6,
                        pointHoverRadius: 8
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        title: {{
                            display: true,
                            text: 'Carpark Capacity vs Average Utilization at 6PM',
                            font: {{
                                size: 16,
                                weight: 'bold'
                            }}
                        }},
                        legend: {{
                            display: false
                        }},
                        tooltip: {{
                            callbacks: {{
                                title: function(context) {{
                                    const dataPoint = context[0].raw;
                                    return 'Carpark ' + dataPoint.carpark_number;
                                }},
                                label: function(context) {{
                                    const dataPoint = context.raw;
                                    return [
                                        'Address: ' + dataPoint.address,
                                        'Total Lots: ' + dataPoint.x,
                                        'Utilization: ' + dataPoint.y + '%',
                                        'Data Points: ' + dataPoint.data_points
                                    ];
                                }},
                                afterLabel: function(context) {{
                                    const dataPoint = context.raw;
                                    const utilization = dataPoint.y;
                                    if (utilization >= 90) {{
                                        return '🔥 Very High Utilization';
                                    }} else if (utilization >= 80) {{
                                        return '⚠️ High Utilization';
                                    }} else if (utilization >= 60) {{
                                        return '📊 Moderate Utilization';
                                    }} else {{
                                        return '✅ Low Utilization';
                                    }}
                                }}
                            }},
                            backgroundColor: 'rgba(0, 0, 0, 0.8)',
                            titleColor: 'white',
                            bodyColor: 'white',
                            borderColor: 'rgba(52, 152, 219, 1)',
                            borderWidth: 2,
                            cornerRadius: 8,
                            displayColors: false,
                            padding: 12
                        }}
                    }},
                    scales: {{
                        x: {{
                            title: {{
                                display: true,
                                text: 'Total Lots'
                            }},
                            grid: {{
                                color: 'rgba(0,0,0,0.1)'
                            }}
                        }},
                        y: {{
                            title: {{
                                display: true,
                                text: 'Average Utilization (%)'
                            }},
                            grid: {{
                                color: 'rgba(0,0,0,0.1)'
                            }},
                            min: 0,
                            max: 100
                        }}
                    }},
                    interaction: {{
                        intersect: false
                    }}
                }}
            }});
        </script>
    </body>
    </html>
    """
    
    return html_content

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
    
    # Generate HTML report
    report_path = generate_6pm_html_report(summary_results, detail_results)
    
    print("✅ Historical 6PM Utilization Pipeline completed!")
    print(f"📊 HTML Report available at: {report_path}")
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