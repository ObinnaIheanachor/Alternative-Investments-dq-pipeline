"""
Data Ingestion and Standardization Module
Loads data from multiple formats and normalizes to standard schema
"""

import pandas as pd
import json
import time
from datetime import datetime
from config import (get_db_connection, initialize_database, CURRENCY_RATES, 
                   convert_to_usd, log_audit, print_header, get_input_path)

class DataIngestionPipeline:
    
    def __init__(self):
        self.conn = get_db_connection()
        self.ingestion_timestamp = datetime.now().isoformat()
        self.stats = {
            'funds_ingested': 0,
            'performance_ingested': 0,
            'regulatory_ingested': 0,
            'funds_standardized': 0,
            'performance_standardized': 0
        }
    
    def ingest_fund_master_csv(self):
        """
        Ingest fund master data from CSV file
        """
        print("\n📥 Step 1: Ingesting Fund Master Data (CSV)")
        start_time = time.time()
        
        try:
            # Load CSV
            df = pd.read_csv(get_input_path('fund_master.csv'))
            print(f"   ✅ Loaded {len(df)} records from CSV")
            
            # Add metadata
            df['ingestion_timestamp'] = self.ingestion_timestamp
            df['source_file'] = 'fund_master.csv'
            
            # Save to raw table
            df.to_sql('raw_fund_master', self.conn, if_exists='replace', index=False)
            
            self.stats['funds_ingested'] = len(df)
            duration = time.time() - start_time
            
            log_audit('INGEST', 'raw_fund_master', len(df), duration, 'SUCCESS')
            print(f"   ✅ Ingested to raw_fund_master table")
            print(f"   ⏱️  Duration: {duration:.2f} seconds")
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            log_audit('INGEST', 'raw_fund_master', 0, duration, 'FAILED', str(e))
            print(f"   ❌ Error: {str(e)}")
            return False
    
    def ingest_performance_json(self):
        """
        Ingest performance data from JSON file
        """
        print("\n📥 Step 2: Ingesting Performance Data (JSON)")
        start_time = time.time()
        
        try:
            # Load JSON
            with open(get_input_path('fund_performance.json'), 'r') as f:
                data = json.load(f)
            
            print(f"   ✅ Loaded {len(data)} records from JSON")
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add metadata
            df['ingestion_timestamp'] = self.ingestion_timestamp
            df['source_file'] = 'fund_performance.json'
            
            # Save to raw table
            df.to_sql('raw_performance', self.conn, if_exists='replace', index=False)
            
            self.stats['performance_ingested'] = len(df)
            duration = time.time() - start_time
            
            log_audit('INGEST', 'raw_performance', len(df), duration, 'SUCCESS')
            print(f"   ✅ Ingested to raw_performance table")
            print(f"   ⏱️  Duration: {duration:.2f} seconds")
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            log_audit('INGEST', 'raw_performance', 0, duration, 'FAILED', str(e))
            print(f"   ❌ Error: {str(e)}")
            return False
    
    def ingest_regulatory_json(self):
        """
        Ingest regulatory filings from JSON file
        """
        print("\n�� Step 3: Ingesting Regulatory Filings (JSON)")
        start_time = time.time()
        
        try:
            # Load JSON
            with open(get_input_path('regulatory_filings.json'), 'r') as f:
                data = json.load(f)
            
            print(f"   ✅ Loaded {len(data)} records from JSON")
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add metadata
            df['ingestion_timestamp'] = self.ingestion_timestamp
            df['source_file'] = 'regulatory_filings.json'
            
            # Save to raw table
            df.to_sql('raw_regulatory', self.conn, if_exists='replace', index=False)
            
            self.stats['regulatory_ingested'] = len(df)
            duration = time.time() - start_time
            
            log_audit('INGEST', 'raw_regulatory', len(df), duration, 'SUCCESS')
            print(f"   ✅ Ingested to raw_regulatory table")
            print(f"   ⏱️  Duration: {duration:.2f} seconds")
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            log_audit('INGEST', 'raw_regulatory', 0, duration, 'FAILED', str(e))
            print(f"   ❌ Error: {str(e)}")
            return False
    
    def standardize_fund_master(self):
        """
        Standardize fund master data:
        - Convert all currencies to USD
        - Normalize field names
        - Create clean, consistent schema
        """
        print("\n🔧 Step 4: Standardizing Fund Master Data")
        start_time = time.time()
        
        try:
            # Load raw data
            df = pd.read_sql('SELECT * FROM raw_fund_master', self.conn)
            print(f"   📊 Processing {len(df)} fund records...")
            
            # Currency conversion
            print("   💱 Converting currencies to USD...")
            currency_conversions = {}
            
            df['fund_size_usd_millions'] = df.apply(
                lambda row: convert_to_usd(row['fund_size_millions'], row['currency']) 
                if pd.notna(row['fund_size_millions']) else None,
                axis=1
            )
            
            df['target_size_usd_millions'] = df.apply(
                lambda row: convert_to_usd(row['target_size_millions'], row['currency']) 
                if pd.notna(row['target_size_millions']) else None,
                axis=1
            )
            
            # Track currency conversions
            for currency in df['currency'].unique():
                if pd.notna(currency):
                    count = len(df[df['currency'] == currency])
                    currency_conversions[currency] = count
            
            print(f"      Conversions: {currency_conversions}")
            
            # Create standardized DataFrame
            standardized_df = pd.DataFrame({
                'fund_id': df['fund_id'],
                'fund_name': df['fund_name'],
                'manager_name': df['manager_name'],
                'fund_type': df['fund_type'],
                'strategy': df['strategy'],
                'vintage_year': df['vintage_year'],
                'inception_date': df['inception_date'],
                'fund_size_usd_millions': df['fund_size_usd_millions'],
                'original_currency': df['currency'],
                'original_fund_size': df['fund_size_millions'],
                'target_size_usd_millions': df['target_size_usd_millions'],
                'status': df['status'],
                'geography': df['geography'],
                'sector_focus': df['sector_focus'],
                'administrator': df['administrator'],
                'last_updated': df['last_updated'],
                'standardization_timestamp': datetime.now().isoformat(),
                'data_quality_passed': False  # Will be updated after validation
            })
            
            # Save to standardized table
            standardized_df.to_sql('standardized_funds', self.conn, if_exists='replace', index=False)
            
            self.stats['funds_standardized'] = len(standardized_df)
            duration = time.time() - start_time
            
            log_audit('STANDARDIZE', 'standardized_funds', len(standardized_df), duration, 'SUCCESS')
            print(f"   ✅ Standardized {len(standardized_df)} fund records")
            print(f"   ⏱️  Duration: {duration:.2f} seconds")
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            log_audit('STANDARDIZE', 'standardized_funds', 0, duration, 'FAILED', str(e))
            print(f"   ❌ Error: {str(e)}")
            return False
    
    def standardize_performance(self):
        """
        Standardize performance data:
        - Calculate expected values (e.g., TVPI = DPI + RVPI)
        - Normalize date formats
        - Create consistent schema
        """
        print("\n🔧 Step 5: Standardizing Performance Data")
        start_time = time.time()
        
        try:
            # Load raw data
            df = pd.read_sql('SELECT * FROM raw_performance', self.conn)
            print(f"   📊 Processing {len(df)} performance records...")
            
            # Calculate what TVPI should be (for validation)
            df['tvpi_calculated'] = df['dpi'] + df['rvpi']
            
            # Create standardized DataFrame
            standardized_df = pd.DataFrame({
                'fund_id': df['fund_id'],
                'report_date': df['report_date'],
                'report_quarter': df['report_quarter'],
                'irr_net_pct': df['irr_net_pct'],
                'moic': df['moic'],
                'dpi': df['dpi'],
                'rvpi': df['rvpi'],
                'tvpi': df['tvpi'],
                'tvpi_calculated': df['tvpi_calculated'],
                'capital_called_millions': df['capital_called_millions'],
                'distributions_millions': df['distributions_millions'],
                'remaining_value_millions': df['remaining_value_millions'],
                'nav_per_share': df['nav_per_share'],
                'monthly_return_pct': df['monthly_return_pct'],
                'standardization_timestamp': datetime.now().isoformat(),
                'data_quality_passed': False  # Will be updated after validation
            })
            
            # Save to standardized table
            standardized_df.to_sql('standardized_performance', self.conn, if_exists='replace', index=False)
            
            self.stats['performance_standardized'] = len(standardized_df)
            duration = time.time() - start_time
            
            log_audit('STANDARDIZE', 'standardized_performance', len(standardized_df), duration, 'SUCCESS')
            print(f"   ✅ Standardized {len(standardized_df)} performance records")
            print(f"   ⏱️  Duration: {duration:.2f} seconds")
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            log_audit('STANDARDIZE', 'standardized_performance', 0, duration, 'FAILED', str(e))
            print(f"   ❌ Error: {str(e)}")
            return False
    
    def print_summary(self):
        """
        Print ingestion summary statistics
        """
        print("\n" + "="*70)
        print("INGESTION & STANDARDIZATION SUMMARY".center(70))
        print("="*70)
        
        print(f"\n📊 Ingestion Results:")
        print(f"   • Fund Master Records:     {self.stats['funds_ingested']:>6}")
        print(f"   • Performance Records:      {self.stats['performance_ingested']:>6}")
        print(f"   • Regulatory Records:       {self.stats['regulatory_ingested']:>6}")
        print(f"   {'─'*50}")
        print(f"   • Total Records Ingested:   {sum([self.stats['funds_ingested'], self.stats['performance_ingested'], self.stats['regulatory_ingested']]):>6}")
        
        print(f"\n🔧 Standardization Results:")
        print(f"   • Funds Standardized:       {self.stats['funds_standardized']:>6}")
        print(f"   • Performance Standardized: {self.stats['performance_standardized']:>6}")
        
        print("\n✅ All data successfully ingested and standardized!")
        print("="*70)
    
    def close(self):
        """Close database connection"""
        self.conn.close()

def main():
    """
    Main execution function
    """
    print_header("BLOOMBERG DQ SYSTEM - DATA INGESTION")
    
    # Initialize database
    initialize_database()
    
    # Create pipeline
    pipeline = DataIngestionPipeline()
    
    # Execute ingestion steps
    success = True
    success &= pipeline.ingest_fund_master_csv()
    success &= pipeline.ingest_performance_json()
    success &= pipeline.ingest_regulatory_json()
    success &= pipeline.standardize_fund_master()
    success &= pipeline.standardize_performance()
    
    # Print summary
    if success:
        pipeline.print_summary()
    else:
        print("\n❌ Some ingestion steps failed. Check logs for details.")
    
    # Cleanup
    pipeline.close()

if __name__ == '__main__':
    main()
