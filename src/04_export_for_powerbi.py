"""
Export data to CSV files for Power BI consumption
"""

import pandas as pd
from config import get_db_connection, print_header

def export_all_tables():
    """
    Export all relevant tables to CSV for Power BI
    """
    print_header("BLOOMBERG DQ SYSTEM - DATA EXPORT")
    
    conn = get_db_connection()
    
    # Tables to export
    tables = {
        'standardized_funds': 'Funds data with USD normalization',
        'standardized_performance': 'Performance metrics',
        'quality_issues': 'All detected quality issues',
        'quality_metrics': 'Quality KPIs and trends',
        'quality_alerts': 'Critical alerts requiring action'
    }
    
    print("\n📤 Exporting data for Power BI...")
    print("="*70)
    
    export_count = 0
    
    for table_name, description in tables.items():
        try:
            # Read from database
            df = pd.read_sql(f'SELECT * FROM {table_name}', conn)
            
            # Export to CSV
            output_path = f'../data/output/{table_name}.csv'
            df.to_csv(output_path, index=False)
            
            print(f"   ✅ {table_name:30s} → {len(df):>6} records")
            print(f"      {description}")
            
            export_count += 1
            
        except Exception as e:
            print(f"   ❌ {table_name}: {str(e)}")
    
    conn.close()
    
    print("\n" + "="*70)
    print(f"✅ Export Complete! {export_count} tables exported to data/output/")
    print("="*70)
    
    print("\n📊 Next Steps:")
    print("   1. Open Power BI Desktop")
    print("   2. Click 'Get Data' → 'Text/CSV'")
    print("   3. Navigate to: data/output/")
    print("   4. Load the exported CSV files")
    print("   5. Create visualizations")
    
    print("\n💡 Recommended Power BI Visuals:")
    print("   • Overall DQ Score: Gauge chart")
    print("   • Issues by Type: Bar chart")
    print("   • Issues by Severity: Donut chart")
    print("   • Manager Quality: Table with conditional formatting")
    print("   • Trends: Line chart (if multiple runs)")

if __name__ == '__main__':
    export_all_tables()
