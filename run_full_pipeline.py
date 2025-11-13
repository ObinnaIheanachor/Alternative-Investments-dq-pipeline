"""
Master Pipeline Script
Runs full data quality pipeline from start to finish
"""

import subprocess
import sys
import time
from datetime import datetime

def print_banner(text):
    print("\n" + "="*70)
    print(text.center(70))
    print("="*70 + "\n")

def run_script(script_path, script_name):
    """
    Run a Python script and handle errors
    """
    print(f"▶️  Running: {script_name}")
    print("-"*70)
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=False,
            text=True,
            check=True
        )
        
        duration = time.time() - start_time
        print(f"\n✅ {script_name} completed in {duration:.2f} seconds")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {script_name} failed!")
        print(f"Error: {e}")
        return False

def main():
    """
    Execute full pipeline
    """
    print_banner("BLOOMBERG DQ SYSTEM - FULL PIPELINE EXECUTION")
    
    start_time = time.time()
    
    # Pipeline steps
    steps = [
        ('src/01_ingest_standardize.py', 'Data Ingestion & Standardization'),
        ('src/02_validate_quality.py', 'Data Quality Validation'),
        ('src/03_generate_metrics.py', 'Metrics Generation'),
        ('src/04_export_for_powerbi.py', 'Power BI Export')
    ]
    
    # Execute each step
    for script_path, script_name in steps:
        success = run_script(script_path, script_name)
        
        if not success:
            print("\n" + "="*70)
            print("❌ PIPELINE FAILED".center(70))
            print("="*70)
            sys.exit(1)
        
        print("\n")
    
    # Final summary
    total_duration = time.time() - start_time
    
    print_banner("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    
    print(f"📊 Execution Summary:")
    print(f"   • Total Duration: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
    print(f"   • Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\n📁 Output Files:")
    print(f"   • Database: database/bloomberg_dq.db")
    print(f"   • Power BI CSVs: data/output/*.csv")
    print(f"   • Executive Summary: data/output/executive_summary.csv")
    print(f"   • Critical Alerts: data/output/CRITICAL_ALERTS.csv")
    
    print(f"\n🎯 Next Steps:")
    print(f"   1. Open Power BI Dashboard: dashboard/Bloomberg_DQ_Dashboard.pbix")
    print(f"   2. Click 'Refresh' in Power BI to load latest data")
    print(f"   3. Review Critical Alerts in data/output/CRITICAL_ALERTS.csv")
    print(f"   4. Practice your interview demo!")
    
    print("\n" + "="*70)

if __name__ == '__main__':
    main()