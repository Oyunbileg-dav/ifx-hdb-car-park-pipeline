"""
IFX HDB Carpark Analysis Pipeline

This is the main orchestrator pipeline that runs both business question analyses:

1. Current Occupancy Analysis: How many HDB carpark lots are currently occupied?
2. Historical 6PM Utilization Analysis: How many HDB carparks with electronic parking 
   are utilised at ≥80% capacity on average at approximately 6pm this month?

The pipeline can run both analyses together or individually based on user needs.
"""

from prefect import flow
from flows.current_occupancy_pipeline import current_occupancy_pipeline
from flows.historical_6pm_pipeline import historical_6pm_pipeline

@flow(name="Complete HDB Carpark Analysis")
def complete_analysis_pipeline():
    """
    Run both business question analyses:
    1. Current occupancy analysis (real-time data)
    2. Historical 6pm utilization analysis (trend data)
    """
    print("🚀 Starting Complete HDB Carpark Analysis Pipeline...")
    print("=" * 70)
    
    # Run current occupancy analysis
    print("\n📊 Phase 1: Current Occupancy Analysis")
    print("-" * 40)
    current_results = current_occupancy_pipeline()
    
    print("\n" + "=" * 70)
    
    # Run historical 6pm utilization analysis  
    print("\n🌆 Phase 2: Historical 6PM Utilization Analysis")
    print("-" * 50)
    historical_summary, historical_details = historical_6pm_pipeline()
    
    print("\n" + "=" * 70)
    print("✅ Complete HDB Carpark Analysis Pipeline finished!")
    print("📋 Both business questions have been answered.")
    
    return {
        "current_occupancy": current_results,
        "historical_6pm_summary": historical_summary,
        "historical_6pm_details": historical_details
    }

@flow(name="Current Occupancy Only")
def current_occupancy_only():
    """Run only the current occupancy analysis"""
    print("📊 Running Current Occupancy Analysis Only...")
    return current_occupancy_pipeline()

@flow(name="Historical 6PM Analysis Only") 
def historical_6pm_only():
    """Run only the historical 6pm utilization analysis"""
    print("🌆 Running Historical 6PM Utilization Analysis Only...")
    return historical_6pm_pipeline()

# Legacy compatibility - maintains the original daily_pipeline name
@flow(name="Daily Pipeline (Legacy)")
def daily_pipeline():
    """Legacy pipeline - now runs the complete analysis"""
    print("⚠️  Using legacy daily_pipeline - redirecting to complete analysis...")
    return complete_analysis_pipeline()

if __name__ == "__main__":
    import sys
    
    # Allow running specific pipelines via command line arguments
    if len(sys.argv) > 1:
        pipeline_type = sys.argv[1].lower()
        
        if pipeline_type in ["current", "occupancy", "real-time"]:
            print("🎯 Running Current Occupancy Analysis...")
            current_occupancy_only()
        elif pipeline_type in ["historical", "6pm", "utilization"]:
            print("🎯 Running Historical 6PM Analysis...")
            historical_6pm_only()
        elif pipeline_type in ["complete", "both", "all"]:
            print("🎯 Running Complete Analysis...")
            complete_analysis_pipeline()
        else:
            print(f"❌ Unknown pipeline type: {pipeline_type}")
            print("Available options:")
            print("  current/occupancy/real-time - Current occupancy analysis")
            print("  historical/6pm/utilization - Historical 6pm analysis") 
            print("  complete/both/all - Both analyses")
            sys.exit(1)
    else:
        # Default: run complete analysis
        print("🎯 No arguments provided - running complete analysis...")
        complete_analysis_pipeline()