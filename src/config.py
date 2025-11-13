"""
Configuration and Database Setup for Bloomberg DQ System
"""

import sqlite3
from datetime import datetime
import os

# ============================================================================
# CONFIGURATION SETTINGS
# ============================================================================

# Database path
DB_PATH = os.path.join('..', 'database', 'bloomberg_dq.db')

# Data quality thresholds
QUALITY_THRESHOLDS = {
    'completeness_target': 95.0,  # % of required fields populated
    'accuracy_target': 98.0,       # % of records passing validation
    'timeliness_days': 90,         # Max days since last update
    'variance_threshold': 5.0,     # % variance for cross-source validation
    'critical_alert_threshold': 0  # Alert if any critical issues found
}

# Validation rules configuration
VALIDATION_RULES = {
    'fund_master': {
        'required_fields': ['fund_id', 'fund_name', 'manager_name', 'fund_type', 'vintage_year', 'fund_size_usd_millions', 'target_size_usd_millions'],
        'numeric_ranges': {
            'fund_size_usd_millions': (0, 100000),
            'vintage_year': (1950, datetime.now().year),
            'target_size_usd_millions': (0, 100000)
        },
        'categorical_values': {
            'fund_type': ['Private Equity', 'Hedge Fund', 'Venture Capital'],
            'currency': ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CNY', 'CAD']
        }
    },
    'performance': {
        'required_fields': ['fund_id', 'report_date', 'report_quarter'],
        'numeric_ranges': {
            'irr_net_pct': (-100, 200),
            'dpi': (0, 20),
            'rvpi': (0, 20),
            'tvpi': (0, 30),
            'monthly_return_pct': (-50, 100)
        },
        'mathematical_relationships': {
            'tvpi_equals_dpi_plus_rvpi': {
                'formula': 'tvpi = dpi + rvpi',
                'tolerance': 0.01
            }
        }
    }
}

# Currency conversion rates (as of sample data generation)
# In production, this would come from a live API
CURRENCY_RATES = {
    'USD': 1.0,
    'EUR': 1.08,
    'GBP': 1.27,
    'JPY': 0.0067,
    'CHF': 1.12,
    'CNY': 0.14,
    'CAD': 0.73
}

# Manager quality tier definitions
MANAGER_TIERS = {
    'Tier 1 (Excellent)': (95, 100),
    'Tier 2 (Good)': (85, 95),
    'Tier 3 (Needs Improvement)': (70, 85),
    'Tier 4 (Critical)': (0, 70)
}

# Issue severity definitions
SEVERITY_LEVELS = {
    'Critical': 'Requires immediate action - data unusable',
    'High': 'Significant impact - resolve within 24 hours',
    'Medium': 'Moderate impact - resolve within 1 week',
    'Low': 'Minor impact - track and resolve as possible'
}

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def get_db_connection():
    """
    Create and return database connection
    Creates database file if it doesn't exist
    """
    # Ensure database directory exists
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    conn = sqlite3.connect(DB_PATH)
    return conn

def initialize_database():
    """
    Initialize database schema with all required tables
    """
    print("\n🗄️  Initializing database...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Drop existing tables (fresh start)
    tables = ['raw_fund_master', 'raw_performance', 'raw_regulatory', 
              'standardized_funds', 'standardized_performance',
              'quality_issues', 'quality_metrics', 'quality_alerts',
              'audit_log']
    
    for table in tables:
        cursor.execute(f'DROP TABLE IF EXISTS {table}')
    
    # ========================================================================
    # RAW DATA TABLES (as received from sources)
    # ========================================================================
    
    cursor.execute('''
        CREATE TABLE raw_fund_master (
            fund_id TEXT PRIMARY KEY,
            fund_name TEXT,
            manager_name TEXT,
            fund_type TEXT,
            strategy TEXT,
            vintage_year INTEGER,
            inception_date TEXT,
            fund_size_millions REAL,
            currency TEXT,
            target_size_millions REAL,
            status TEXT,
            geography TEXT,
            sector_focus TEXT,
            administrator TEXT,
            last_updated TEXT,
            ingestion_timestamp TEXT,
            source_file TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE raw_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id TEXT,
            report_date TEXT,
            report_quarter TEXT,
            irr_net_pct REAL,
            moic REAL,
            dpi REAL,
            rvpi REAL,
            tvpi REAL,
            capital_called_millions REAL,
            distributions_millions REAL,
            remaining_value_millions REAL,
            nav_per_share REAL,
            monthly_return_pct REAL,
            ingestion_timestamp TEXT,
            source_file TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE raw_regulatory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id TEXT,
            filing_type TEXT,
            filing_date TEXT,
            filing_quarter TEXT,
            reported_aum_millions REAL,
            reported_strategy TEXT,
            num_investors INTEGER,
            source TEXT,
            ingestion_timestamp TEXT,
            source_file TEXT
        )
    ''')
    
    # ========================================================================
    # STANDARDIZED DATA TABLES (normalized and cleaned)
    # ========================================================================
    
    cursor.execute('''
        CREATE TABLE standardized_funds (
            fund_id TEXT PRIMARY KEY,
            fund_name TEXT,
            manager_name TEXT,
            fund_type TEXT,
            strategy TEXT,
            vintage_year INTEGER,
            inception_date TEXT,
            fund_size_usd_millions REAL,
            original_currency TEXT,
            original_fund_size REAL,
            target_size_usd_millions REAL,
            status TEXT,
            geography TEXT,
            sector_focus TEXT,
            administrator TEXT,
            last_updated TEXT,
            standardization_timestamp TEXT,
            data_quality_passed BOOLEAN
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE standardized_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id TEXT,
            report_date TEXT,
            report_quarter TEXT,
            irr_net_pct REAL,
            moic REAL,
            dpi REAL,
            rvpi REAL,
            tvpi REAL,
            tvpi_calculated REAL,
            capital_called_millions REAL,
            distributions_millions REAL,
            remaining_value_millions REAL,
            nav_per_share REAL,
            monthly_return_pct REAL,
            standardization_timestamp TEXT,
            data_quality_passed BOOLEAN
        )
    ''')
    
    # ========================================================================
    # DATA QUALITY TABLES
    # ========================================================================
    
    cursor.execute('''
        CREATE TABLE quality_issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id TEXT,
            issue_type TEXT,
            severity TEXT,
            field_name TEXT,
            expected_value TEXT,
            actual_value TEXT,
            issue_description TEXT,
            detected_timestamp TEXT,
            status TEXT DEFAULT 'Open',
            resolution_notes TEXT,
            resolved_timestamp TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE quality_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_date TEXT,
            metric_name TEXT,
            metric_value REAL,
            target_value REAL,
            entity_type TEXT,
            entity_name TEXT,
            calculation_timestamp TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE quality_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id TEXT,
            fund_id TEXT,
            rule_violated TEXT,
            severity TEXT,
            description TEXT,
            detected_timestamp TEXT,
            status TEXT DEFAULT 'ACTIVE',
            acknowledged_by TEXT,
            acknowledged_timestamp TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation TEXT,
            table_name TEXT,
            records_affected INTEGER,
            execution_timestamp TEXT,
            execution_duration_seconds REAL,
            status TEXT,
            error_message TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    
    print("   ✅ Database schema created successfully")
    print(f"   📁 Database location: {DB_PATH}")

def log_audit(operation, table_name, records_affected, duration, status='SUCCESS', error_msg=None):
    """
    Log operations to audit table for traceability
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO audit_log (operation, table_name, records_affected, 
                              execution_timestamp, execution_duration_seconds, 
                              status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (operation, table_name, records_affected, 
          datetime.now().isoformat(), duration, status, error_msg))
    
    conn.commit()
    conn.close()

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def convert_to_usd(amount, currency):
    """
    Convert amount from given currency to USD
    """
    if currency not in CURRENCY_RATES:
        print(f"   ⚠️  Unknown currency: {currency}, assuming USD")
        return amount
    
    return amount * CURRENCY_RATES[currency]

def get_manager_tier(quality_score):
    """
    Determine manager quality tier based on score
    """
    for tier_name, (min_score, max_score) in MANAGER_TIERS.items():
        if min_score <= quality_score < max_score:
            return tier_name
    return 'Tier 4 (Critical)'

def print_header(text):
    """
    Print formatted header for console output
    """
    print("\n" + "="*70)
    print(text.center(70))
    print("="*70)

# ============================================================================
# MAIN EXECUTION (only runs if this file is executed directly)
# ============================================================================

if __name__ == '__main__':
    print_header("BLOOMBERG DQ SYSTEM - DATABASE INITIALIZATION")
    initialize_database()
    print("\n✅ Configuration loaded successfully")
    print(f"   Database: {DB_PATH}")
    print(f"   Quality Thresholds: {QUALITY_THRESHOLDS}")
