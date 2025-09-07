"""
Current Occupancy Pipeline

This pipeline answers the question:
"How many HDB carpark lots are currently occupied?"

It focuses on real-time data collection and analysis with interactive map visualization.
"""

from prefect import flow, task
from prefect.utilities.annotations import quote
from etl.extract import fetch_carpark_availability, fetch_carpark_info
from etl.transform import transform_carpark_current_availability, transform_carpark_info
from etl.load import load_carpark_current_availability, load_carpark_info
from etl.reports import get_current_occupancy
from etl.db import get_conn
from datetime import datetime
from pathlib import Path
import pandas as pd
import json
import os
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

@task
def extract_current_availability():
    """Extract real-time carpark availability data"""
    return fetch_carpark_availability()

@task
def extract_carpark_reference_info():
    """Extract carpark reference information"""
    return fetch_carpark_info()

@task
def transform_current_availability(raw_data):
    """Transform current availability data"""
    return transform_carpark_current_availability(raw_data)

@task
def transform_reference_info(raw_data):
    """Transform carpark reference data"""
    return transform_carpark_info(raw_data)

@task
def load_current_availability(records):
    """Load current availability data to database"""
    return load_carpark_current_availability(records)

@task
def load_reference_info(records):
    """Load carpark reference data to database"""
    return load_carpark_info(records)

@task
def analyze_current_occupancy():
    """Analyze current occupancy and return results"""
    result = get_current_occupancy()
    
    if not result.empty:
        occupied_lots = result.iloc[0]['occupied_lots']
        total_lots = result.iloc[0]['total_lots']
        available_lots = result.iloc[0]['available_lots']
        occupancy_rate = result.iloc[0]['occupancy_rate']
        
        print(f"\n🏗️  CURRENT OCCUPANCY ANALYSIS")
        print(f"=" * 50)
        print(f"📊 Occupied Lots: {occupied_lots:,}")
        print(f"🅿️  Total Lots: {total_lots:,}")
        print(f"🟢 Available Lots: {available_lots:,}")
        print(f"📈 Occupancy Rate: {occupancy_rate}%")
        print(f"=" * 50)
    
    return result

@task
def get_map_data():
    """Get carpark location and availability data for map visualization"""
    with get_conn() as conn:
        query = """
        SELECT 
            latest.carpark_number,
            info.address,
            info.x_coord,
            info.y_coord,
            SUM(latest.total_lots) as total_lots,
            SUM(latest.available_lots) as available_lots,
            SUM(latest.total_lots - latest.available_lots) as occupied_lots,
            ROUND(SUM(latest.total_lots - latest.available_lots)::numeric / NULLIF(SUM(latest.total_lots), 0) * 100, 1) as occupancy_rate
        FROM (
            SELECT DISTINCT ON (carpark_number, lot_type)
                carpark_number, lot_type, total_lots, available_lots
            FROM raw_carpark_current_availability
            WHERE total_lots IS NOT NULL 
              AND available_lots IS NOT NULL
              AND total_lots > 0
            ORDER BY carpark_number, lot_type, ingest_ts_sgt DESC
        ) latest
        INNER JOIN ref_carpark_info info ON latest.carpark_number = info.car_park_no
        WHERE info.x_coord IS NOT NULL 
          AND info.y_coord IS NOT NULL
          AND info.x_coord != ''
          AND info.y_coord != ''
        GROUP BY latest.carpark_number, info.address, info.x_coord, info.y_coord
        HAVING SUM(latest.total_lots) > 0
        ORDER BY SUM(latest.available_lots) DESC
        """
        
        df = pd.read_sql(query, conn)
        print(f"📍 Retrieved {len(df)} carparks with location data for mapping")
        return df

@task
def generate_html_report(analysis_result, map_data):
    """Generate HTML report for current occupancy analysis with interactive map"""
    
    # Ensure reports directory exists
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    
    if analysis_result.empty:
        html_content = generate_empty_report_html()
    else:
        # Extract data from analysis result
        occupied_lots = int(analysis_result.iloc[0]['occupied_lots'])
        total_lots = int(analysis_result.iloc[0]['total_lots'])
        available_lots = int(analysis_result.iloc[0]['available_lots'])
        occupancy_rate = float(analysis_result.iloc[0]['occupancy_rate'])
        
        html_content = generate_occupancy_report_html(
            occupied_lots, total_lots, available_lots, occupancy_rate, map_data
        )
    
    # Write HTML report
    report_path = reports_dir / "current_occupancy_report.html"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"📄 HTML report with interactive map generated: {report_path}")
    return str(report_path)

def generate_occupancy_report_html(occupied_lots, total_lots, available_lots, occupancy_rate, map_data):
    """Generate HTML content for occupancy report with interactive map"""
    
    # Determine status color based on occupancy rate
    if occupancy_rate >= 90:
        status_color = "#dc3545"  # Red - Very High
        status_text = "Very High"
        status_icon = "🔴"
    elif occupancy_rate >= 80:
        status_color = "#fd7e14"  # Orange - High
        status_text = "High"
        status_icon = "🟠"
    elif occupancy_rate >= 60:
        status_color = "#ffc107"  # Yellow - Moderate
        status_text = "Moderate"
        status_icon = "🟡"
    else:
        status_color = "#198754"  # Green - Low
        status_text = "Low"
        status_icon = "🟢"
    
    # Calculate utilization percentage for visual bar
    utilization_width = min(occupancy_rate, 100)
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S SGT")
    
    # Prepare map data as JSON
    map_markers = []
    if not map_data.empty:
        for _, row in map_data.iterrows():
            try:
                # Convert Singapore SVY21 coordinates to relative positioning
                x_coord = float(row['x_coord'])
                y_coord = float(row['y_coord'])
                
                # Use raw coordinates for relative positioning (no geographic conversion needed)
                # Scale down significantly for better visualization in simple coordinate system
                lat = (y_coord - 30000) / 10000  # Scale Y coordinate to range roughly -2 to +2
                lng = (x_coord - 20000) / 10000  # Scale X coordinate to range roughly -1 to +3
                
                # Calculate marker size based on total lots (min 6, max 40 pixels)
                total = int(row['total_lots'])
                marker_size = max(6, min(40, 6 + total / 100))  # Scale based on total capacity
                
                # Color based on occupancy rate
                occ_rate = float(row['occupancy_rate']) if row['occupancy_rate'] else 0
                if occ_rate >= 90:
                    color = '#dc3545'  # Red
                elif occ_rate >= 80:
                    color = '#fd7e14'  # Orange
                elif occ_rate >= 60:
                    color = '#ffc107'  # Yellow
                else:
                    color = '#28a745'  # Green
                
                map_markers.append({
                    'lat': lat,
                    'lng': lng,
                    'carpark_number': row['carpark_number'],
                    'address': row['address'],
                    'total_lots': total,
                    'available_lots': int(row['available_lots']),
                    'occupied_lots': int(row['occupied_lots']),
                    'occupancy_rate': occ_rate,
                    'marker_size': marker_size,
                    'color': color
                })
            except (ValueError, TypeError):
                continue  # Skip invalid coordinates
    
    map_data_json = json.dumps(map_markers)
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>HDB Carpark Current Occupancy Report</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
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
            .header .subtitle {{
                margin: 10px 0 0 0;
                font-size: 1.1em;
                opacity: 0.9;
            }}
            .main-content {{
                padding: 40px;
            }}
            .status-card {{
                background: {status_color};
                color: white;
                padding: 25px;
                border-radius: 10px;
                text-align: center;
                margin-bottom: 30px;
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }}
            .status-card h2 {{
                margin: 0;
                font-size: 2em;
                font-weight: 400;
            }}
            .metrics-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }}
            .metric-card {{
                background: #f8f9fa;
                border: 1px solid #e9ecef;
                border-radius: 10px;
                padding: 25px;
                text-align: center;
                transition: transform 0.2s ease;
            }}
            .metric-card:hover {{
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }}
            .metric-number {{
                font-size: 2.5em;
                font-weight: bold;
                margin: 10px 0;
                color: #2c3e50;
            }}
            .metric-label {{
                font-size: 1.1em;
                color: #6c757d;
                margin: 0;
            }}
            .utilization-bar {{
                background: #e9ecef;
                border-radius: 50px;
                height: 30px;
                margin: 20px 0;
                overflow: hidden;
            }}
            .utilization-fill {{
                background: linear-gradient(90deg, #28a745 0%, #ffc107 50%, #dc3545 100%);
                height: 100%;
                width: {utilization_width}%;
                border-radius: 50px;
                transition: width 0.5s ease;
                display: flex;
                align-items: center;
                justify-content: flex-end;
                padding-right: 15px;
                color: white;
                font-weight: bold;
            }}
            .map-section {{
                background: #f8f9fa;
                border-radius: 10px;
                padding: 25px;
                margin: 30px 0;
            }}
            .map-section h3 {{
                margin-top: 0;
                color: #2c3e50;
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            #map {{
                height: 500px;
                width: 100%;
                border-radius: 8px;
                border: 2px solid #dee2e6;
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            }}
            .map-legend {{
                background: white;
                padding: 15px;
                border-radius: 8px;
                margin-top: 15px;
                border: 1px solid #dee2e6;
            }}
            .legend-item {{
                display: flex;
                align-items: center;
                margin: 5px 0;
                gap: 10px;
            }}
            .legend-color {{
                width: 20px;
                height: 20px;
                border-radius: 50%;
                border: 2px solid #333;
            }}
            .details-section {{
                background: #f8f9fa;
                border-radius: 10px;
                padding: 25px;
                margin-top: 30px;
            }}
            .details-section h3 {{
                margin-top: 0;
                color: #2c3e50;
            }}
            .info-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-top: 20px;
            }}
            .info-item {{
                display: flex;
                justify-content: space-between;
                padding: 10px 0;
                border-bottom: 1px solid #dee2e6;
            }}
            .info-label {{
                color: #6c757d;
                font-weight: 500;
            }}
            .info-value {{
                color: #2c3e50;
                font-weight: bold;
            }}
            .timestamp {{
                text-align: center;
                color: #6c757d;
                font-size: 0.9em;
                margin-top: 30px;
                padding-top: 20px;
                border-top: 1px solid #dee2e6;
            }}
            .emoji {{
                font-size: 1.5em;
                margin-right: 10px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🏗️ HDB Carpark Occupancy Report</h1>
                <p class="subtitle">Real-time Analysis of Electronic Parking Systems</p>
            </div>
            
            <div class="main-content">
                <div class="status-card">
                    <h2>{status_icon} Occupancy Level: {status_text}</h2>
                    <p style="margin: 10px 0 0 0; font-size: 1.2em;">{occupancy_rate}% of total capacity</p>
                </div>
                
                <div class="metrics-grid">
                    <div class="metric-card">
                        <div class="emoji">🚗</div>
                        <div class="metric-number">{occupied_lots:,}</div>
                        <p class="metric-label">Occupied Lots</p>
                    </div>
                    
                    <div class="metric-card">
                        <div class="emoji">🅿️</div>
                        <div class="metric-number">{total_lots:,}</div>
                        <p class="metric-label">Total Lots</p>
                    </div>
                    
                    <div class="metric-card">
                        <div class="emoji">🟢</div>
                        <div class="metric-number">{available_lots:,}</div>
                        <p class="metric-label">Available Lots</p>
                    </div>
                </div>
                
                <div class="utilization-bar">
                    <div class="utilization-fill">{occupancy_rate}%</div>
                </div>
                
                <div class="map-section">
                    <h3>🗺️ Carpark Distribution Visualization</h3>
                    <p style="margin-bottom: 15px; color: #6c757d;">
                        Circle sizes represent total capacity. Colors indicate occupancy levels. Click markers for details.
                    </p>
                    
                    <!-- Search Bar -->
                    <div style="margin-bottom: 15px;">
                        <div style="display: flex; gap: 10px; align-items: center;">
                            <input type="text" id="searchInput" placeholder="🔍 Search by address or carpark number..." 
                                   style="flex: 1; padding: 10px; border: 2px solid #dee2e6; border-radius: 8px; font-size: 14px;">
                            <button id="searchBtn" onclick="searchCarpark()" 
                                    style="padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 8px; cursor: pointer; font-weight: bold;">
                                Search
                            </button>
                            <button id="clearBtn" onclick="clearSearch()" 
                                    style="padding: 10px 15px; background: #6c757d; color: white; border: none; border-radius: 8px; cursor: pointer;">
                                Clear
                            </button>
                        </div>
                        <div id="searchResults" style="margin-top: 10px; font-size: 14px; color: #6c757d;"></div>
                    </div>
                    
                    <div id="map"></div>
                    
                    <div class="map-legend">
                        <h4 style="margin: 0 0 10px 0;">🎨 Legend</h4>
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px;">
                            <div class="legend-item">
                                <div class="legend-color" style="background-color: #28a745;"></div>
                                <span>Low occupancy (&lt;60%)</span>
                            </div>
                            <div class="legend-item">
                                <div class="legend-color" style="background-color: #ffc107;"></div>
                                <span>Moderate (60-79%)</span>
                            </div>
                            <div class="legend-item">
                                <div class="legend-color" style="background-color: #fd7e14;"></div>
                                <span>High (80-89%)</span>
                            </div>
                            <div class="legend-item">
                                <div class="legend-color" style="background-color: #dc3545;"></div>
                                <span>Very High (≥90%)</span>
                            </div>
                        </div>
                        <p style="margin: 10px 0 0 0; font-size: 0.9em; color: #6c757d;">
                            💡 Larger circles = higher total capacity (more parking lots)
                        </p>
                    </div>
                </div>
                
                <div class="details-section">
                    <h3>📊 Analysis Details</h3>
                    <div class="info-grid">
                        <div class="info-item">
                            <span class="info-label">Data Source:</span>
                            <span class="info-value">Singapore data.gov.sg API</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">Parking Systems:</span>
                            <span class="info-value">All</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">Coverage:</span>
                            <span class="info-value">All carpark sizes</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">Data Freshness:</span>
                            <span class="info-value">Real-time (last 10 hours)</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">Map Points:</span>
                            <span class="info-value">{len(map_markers)} carparks</span>
                        </div>
                    </div>
                    
                    <div style="margin-top: 25px; padding: 20px; background: white; border-radius: 8px; border-left: 4px solid #007bff;">
                        <h4 style="margin: 0 0 10px 0; color: #007bff;">📋 Business Question Answered:</h4>
                        <p style="margin: 0; color: #2c3e50; font-style: italic;">
                            "How many HDB carpark lots are currently occupied?"
                        </p>
                        <p style="margin: 10px 0 0 0; color: #6c757d; font-size: 0.95em;">
                            This analysis includes all electronic parking systems with real-time availability data, 
                            filtering out only stale data (&gt;10 hours old) to provide comprehensive occupancy 
                            insights covering carparks of all sizes for complete capacity planning and utilization analysis.
                            The visualization shows the relative distribution and availability across carparks.
                        </p>
                    </div>
                </div>
                
                <div class="timestamp">
                    📅 Report generated on {current_time}
                </div>
            </div>
        </div>
        
        <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
        <script>
            // Initialize map with plain background (no tiles)
            var map = L.map('map', {{
                crs: L.CRS.Simple,
                minZoom: -5,
                maxZoom: 10
            }});
            
            // Add carpark markers
            var mapData = {map_data_json};
            var markers = [];
            var markerLookup = {{}};  // For search functionality
            
            mapData.forEach(function(carpark) {{
                
                var marker = L.circleMarker([carpark.lat, carpark.lng], {{
                    radius: carpark.marker_size,
                    fillColor: carpark.color,
                    color: '#333',
                    weight: 2,
                    opacity: 1,
                    fillOpacity: 0.8
                }});
                
                var popupContent = `
                    <div style="min-width: 200px;">
                        <h4 style="margin: 0 0 10px 0; color: #2c3e50;">🅿️ ${{carpark.carpark_number}}</h4>
                        <p style="margin: 0 0 8px 0; font-size: 0.9em; color: #6c757d;">📍 ${{carpark.address}}</p>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin: 10px 0;">
                            <div style="text-align: center; padding: 8px; background: #f8f9fa; border-radius: 4px;">
                                <div style="font-size: 1.2em; font-weight: bold; color: #28a745;">${{carpark.available_lots}}</div>
                                <div style="font-size: 0.8em; color: #6c757d;">Available</div>
                            </div>
                            <div style="text-align: center; padding: 8px; background: #f8f9fa; border-radius: 4px;">
                                <div style="font-size: 1.2em; font-weight: bold; color: #dc3545;">${{carpark.occupied_lots}}</div>
                                <div style="font-size: 0.8em; color: #6c757d;">Occupied</div>
                            </div>
                        </div>
                        <div style="text-align: center; padding: 8px; background: ${{carpark.color}}; color: white; border-radius: 4px; margin-top: 8px;">
                            <strong>${{carpark.occupancy_rate}}% Occupied</strong>
                        </div>
                        <div style="text-align: center; margin-top: 8px; font-size: 0.8em; color: #6c757d;">
                            Total: ${{carpark.total_lots}} lots
                        </div>
                    </div>
                `;
                
                marker.bindPopup(popupContent);
                marker.addTo(map);
                markers.push(marker);
                
                // Store marker for search functionality
                markerLookup[carpark.carpark_number.toLowerCase()] = marker;
                markerLookup[carpark.address.toLowerCase()] = marker;
            }});
            
            // Set initial view and fit bounds
            if (markers.length > 0) {{
                var group = new L.featureGroup(markers);
                var bounds = group.getBounds();
                map.fitBounds(bounds, {{padding: [20, 20]}});
            }} else {{
                // Set a default view if no markers
                map.setView([0, 0], 0);
            }}
            
            // Search functionality
            var currentHighlight = null;
            var originalBounds = null;
            
            function searchCarpark() {{
                var searchTerm = document.getElementById('searchInput').value.toLowerCase().trim();
                var resultsDiv = document.getElementById('searchResults');
                
                if (!searchTerm) {{
                    resultsDiv.innerHTML = '<span style="color: #dc3545;">⚠️ Please enter a search term</span>';
                    return;
                }}
                
                // Clear previous highlight
                if (currentHighlight) {{
                    currentHighlight.setStyle({{
                        color: '#333',
                        weight: 2
                    }});
                }}
                
                // Search through carpark data
                var matches = [];
                mapData.forEach(function(carpark) {{
                    if (carpark.address.toLowerCase().includes(searchTerm) || 
                        carpark.carpark_number.toLowerCase().includes(searchTerm)) {{
                        matches.push(carpark);
                    }}
                }});
                
                if (matches.length === 0) {{
                    resultsDiv.innerHTML = '<span style="color: #dc3545;">❌ No carparks found matching "' + searchTerm + '"</span>';
                    return;
                }}
                
                if (matches.length === 1) {{
                    // Single match - zoom to it
                    var carpark = matches[0];
                    var marker = null;
                    
                    // Find the corresponding marker
                    markers.forEach(function(m) {{
                        var popup = m.getPopup();
                        if (popup && popup.getContent().includes(carpark.carpark_number)) {{
                            marker = m;
                        }}
                    }});
                    
                    if (marker) {{
                        // Highlight the marker
                        marker.setStyle({{
                            color: '#ff6b35',
                            weight: 4
                        }});
                        currentHighlight = marker;
                        
                        // Zoom to marker
                        map.setView(marker.getLatLng(), 5);
                        marker.openPopup();
                        
                        resultsDiv.innerHTML = '<span style="color: #28a745;">✅ Found: ' + carpark.address + '</span>';
                    }}
                }} else {{
                    // Multiple matches - show list
                    resultsDiv.innerHTML = '<span style="color: #007bff;">📍 Found ' + matches.length + ' carparks:</span><br>' +
                        matches.slice(0, 5).map(function(cp) {{
                            return '<small style="margin-left: 10px;">• ' + cp.carpark_number + ': ' + cp.address + '</small>';
                        }}).join('<br>') +
                        (matches.length > 5 ? '<br><small style="margin-left: 10px; color: #6c757d;">... and ' + (matches.length - 5) + ' more. Try a more specific search.</small>' : '');
                }}
            }}
            
            function clearSearch() {{
                document.getElementById('searchInput').value = '';
                document.getElementById('searchResults').innerHTML = '';
                
                // Clear highlight
                if (currentHighlight) {{
                    currentHighlight.setStyle({{
                        color: '#333',
                        weight: 2
                    }});
                    currentHighlight = null;
                }}
                
                // Reset map view
                if (markers.length > 0) {{
                    var group = new L.featureGroup(markers);
                    map.fitBounds(group.getBounds(), {{padding: [20, 20]}});
                }}
            }}
            
            // Allow Enter key to search
            document.getElementById('searchInput').addEventListener('keypress', function(e) {{
                if (e.key === 'Enter') {{
                    searchCarpark();
                }}
            }});
        </script>
    </body>
    </html>
    """
    
    return html_content

def generate_empty_report_html():
    """Generate HTML content when no data is available"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S SGT")
    
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>HDB Carpark Current Occupancy Report</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }}
            .container {{
                max-width: 800px;
                margin: 0 auto;
                background: white;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                padding: 40px;
                text-align: center;
            }}
            .error-icon {{
                font-size: 4em;
                margin-bottom: 20px;
            }}
            h1 {{
                color: #dc3545;
                margin-bottom: 20px;
            }}
            .timestamp {{
                color: #6c757d;
                font-size: 0.9em;
                margin-top: 30px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="error-icon">⚠️</div>
            <h1>No Data Available</h1>
            <p>Unable to generate occupancy report. No current availability data found.</p>
            <p>Please check:</p>
            <ul style="text-align: left; display: inline-block;">
                <li>Database connection</li>
                <li>Data availability</li>
                <li>Pipeline execution status</li>
            </ul>
            <div class="timestamp">
                📅 Report generated on {current_time}
            </div>
        </div>
    </body>
    </html>
    """

@flow(name="Current Occupancy Analysis")
def current_occupancy_pipeline():
    """
    Pipeline to answer: How many HDB carpark lots are currently occupied?
    
    This pipeline:
    1. Extracts real-time carpark availability data
    2. Extracts carpark reference information
    3. Transforms and loads the data
    4. Analyzes current occupancy focusing on electronic parking systems
    5. Generates an HTML report with interactive map visualization
    """
    print("🚀 Starting Current Occupancy Pipeline...")
    
    # Extract data
    availability_data = extract_current_availability()
    reference_data = extract_carpark_reference_info()
    
    # Transform data
    availability_records = transform_current_availability(quote(availability_data))
    reference_records = transform_reference_info(quote(reference_data))
    
    # Load data
    load_current_availability(quote(availability_records))
    load_reference_info(quote(reference_records))
    
    # Analyze and report
    results = analyze_current_occupancy()
    
    # Get map data
    map_data = get_map_data()
    
    # Generate HTML report with map
    report_path = generate_html_report(results, map_data)
    
    print("✅ Current Occupancy Pipeline completed!")
    print(f"📄 HTML report with interactive map available at: {report_path}")
    return results

if __name__ == "__main__":
    current_occupancy_pipeline()