"""
Data Quality Validation Module
Comprehensive validation across multiple dimensions
"""

import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from config import (get_db_connection, VALIDATION_RULES, QUALITY_THRESHOLDS,
                   log_audit, print_header, get_output_path)

class DataQualityValidator:
    
    def __init__(self):
        self.conn = get_db_connection()
        self.issues = []
        self.alerts = []
        self.validation_timestamp = datetime.now().isoformat()
        self.stats = {
            'total_issues': 0,
            'critical_issues': 0,
            'high_issues': 0,
            'medium_issues': 0,
            'low_issues': 0
        }
    
    def log_issue(self, fund_id, issue_type, severity, field_name, 
                  expected_value, actual_value, description):
        """
        Log a data quality issue
        """
        issue = {
            'fund_id': fund_id,
            'issue_type': issue_type,
            'severity': severity,
            'field_name': field_name,
            'expected_value': str(expected_value) if expected_value is not None else None,
            'actual_value': str(actual_value) if actual_value is not None else None,
            'issue_description': description,
            'detected_timestamp': self.validation_timestamp,
            'status': 'Open',
            'resolution_notes': None,
            'resolved_timestamp': None
        }
        
        self.issues.append(issue)
        self.stats['total_issues'] += 1
        
        # Count by severity
        if severity == 'Critical':
            self.stats['critical_issues'] += 1
            self.create_alert(fund_id, issue_type, severity, description)
        elif severity == 'High':
            self.stats['high_issues'] += 1
        elif severity == 'Medium':
            self.stats['medium_issues'] += 1
        else:
            self.stats['low_issues'] += 1
    
    def create_alert(self, fund_id, rule_violated, severity, description):
        """
        Create critical alert for immediate attention
        """
        alert = {
            'alert_id': f'ALERT-{len(self.alerts)+1:04d}',
            'fund_id': fund_id,
            'rule_violated': rule_violated,
            'severity': severity,
            'description': description,
            'detected_timestamp': self.validation_timestamp,
            'status': 'ACTIVE',
            'acknowledged_by': None,
            'acknowledged_timestamp': None
        }
        
        self.alerts.append(alert)
    
    def validate_completeness(self):
        """
        Rule 1: Check for missing required fields
        """
        print("\n🔍 Rule 1: Completeness Validation")
        start_time = time.time()
        issues_found = 0
        
        # Fund master completeness
        df = pd.read_sql('SELECT * FROM standardized_funds', self.conn)
        required_fields = VALIDATION_RULES['fund_master']['required_fields']
        
        for field in required_fields:
            missing_count = df[field].isna().sum()
            if missing_count > 0:
                for _, row in df[df[field].isna()].iterrows():
                    self.log_issue(
                        row['fund_id'],
                        'Completeness',
                        'High' if field in ['fund_id', 'fund_name', 'fund_type'] else 'Medium',
                        field,
                        'Not Null',
                        None,
                        f"Missing required field: {field}"
                    )
                    issues_found += 1
        
        # Check for missing administrator (specific business rule)
        missing_admin = df[df['administrator'].isna()]
        for _, row in missing_admin.iterrows():
            self.log_issue(
                row['fund_id'],
                'Completeness',
                'Medium',
                'administrator',
                'Valid Administrator',
                None,
                "Missing administrator - potential self-administration risk"
            )
            issues_found += 1
        
        duration = time.time() - start_time
        print(f"   ✅ Completed in {duration:.2f}s - Found {issues_found} issues")
        
        return issues_found
    
    def validate_accuracy(self):
        """
        Rule 2: Check data accuracy (ranges, formats, business logic)
        """
        print("\n🔍 Rule 2: Accuracy Validation")
        start_time = time.time()
        issues_found = 0
        
        # === Fund Master Accuracy ===
        df = pd.read_sql('SELECT * FROM standardized_funds', self.conn)
        
        # Check numeric ranges
        numeric_ranges = VALIDATION_RULES['fund_master']['numeric_ranges']
        
        for field, (min_val, max_val) in numeric_ranges.items():
            if field in df.columns:
                invalid = df[(df[field].notna()) & 
                           ((df[field] < min_val) | (df[field] > max_val))]
                
                for _, row in invalid.iterrows():
                    severity = 'Critical' if row[field] < 0 else 'High'
                    self.log_issue(
                        row['fund_id'],
                        'Accuracy',
                        severity,
                        field,
                        f'Between {min_val} and {max_val}',
                        row[field],
                        f"{field} out of valid range: {row[field]}"
                    )
                    issues_found += 1
        
        # Check categorical values
        categorical_rules = VALIDATION_RULES['fund_master']['categorical_values']
        
        for field, valid_values in categorical_rules.items():
            if field in df.columns:
                invalid = df[(df[field].notna()) & 
                           (~df[field].isin(valid_values))]
                
                for _, row in invalid.iterrows():
                    self.log_issue(
                        row['fund_id'],
                        'Accuracy',
                        'Medium',
                        field,
                        f'One of: {valid_values}',
                        row[field],
                        f"Invalid {field}: '{row[field]}' not in allowed values"
                    )
                    issues_found += 1
        
        # === Performance Data Accuracy ===
        perf_df = pd.read_sql('SELECT * FROM standardized_performance', self.conn)
        
        # Check performance metric ranges
        perf_ranges = VALIDATION_RULES['performance']['numeric_ranges']
        
        for field, (min_val, max_val) in perf_ranges.items():
            if field in perf_df.columns:
                invalid = perf_df[(perf_df[field].notna()) & 
                                ((perf_df[field] < min_val) | (perf_df[field] > max_val))]
                
                for _, row in invalid.iterrows():
                    # IRR issues are critical
                    severity = 'Critical' if 'irr' in field.lower() else 'High'
                    self.log_issue(
                        row['fund_id'],
                        'Accuracy',
                        severity,
                        field,
                        f'Between {min_val} and {max_val}',
                        row[field],
                        f"Implausible {field}: {row[field]}"
                    )
                    issues_found += 1
        
        duration = time.time() - start_time
        print(f"   ✅ Completed in {duration:.2f}s - Found {issues_found} issues")
        
        return issues_found
    
    def validate_consistency(self):
        """
        Rule 3: Check mathematical consistency and relationships
        """
        print("\n🔍 Rule 3: Consistency Validation")
        start_time = time.time()
        issues_found = 0
        
        # === TVPI = DPI + RVPI ===
        perf_df = pd.read_sql('SELECT * FROM standardized_performance', self.conn)
        
        # Filter records with all required fields
        complete_records = perf_df[
            perf_df['tvpi'].notna() & 
            perf_df['dpi'].notna() & 
            perf_df['rvpi'].notna()
        ].copy()
        
        # Calculate variance
        complete_records['variance'] = abs(
            complete_records['tvpi'] - complete_records['tvpi_calculated']
        )
        
        # Flag inconsistencies (tolerance: 0.01)
        tolerance = VALIDATION_RULES['performance']['mathematical_relationships']['tvpi_equals_dpi_plus_rvpi']['tolerance']
        inconsistent = complete_records[complete_records['variance'] > tolerance]
        
        for _, row in inconsistent.iterrows():
            self.log_issue(
                row['fund_id'],
                'Consistency',
                'High',
                'tvpi',
                f'{row["tvpi_calculated"]:.2f} (DPI + RVPI)',
                row['tvpi'],
                f"TVPI calculation error: Reported {row['tvpi']:.2f}, Expected {row['tvpi_calculated']:.2f} (DPI {row['dpi']:.2f} + RVPI {row['rvpi']:.2f})"
            )
            issues_found += 1
        
        # === Fund size vs Target size ===
        funds_df = pd.read_sql('SELECT * FROM standardized_funds', self.conn)
        
        size_issues = funds_df[
            (funds_df['fund_size_usd_millions'].notna()) &
            (funds_df['target_size_usd_millions'].notna()) &
            (funds_df['fund_size_usd_millions'] > funds_df['target_size_usd_millions'])
        ]
        
        for _, row in size_issues.iterrows():
            self.log_issue(
                row['fund_id'],
                'Consistency',
                'Medium',
                'fund_size_usd_millions',
                f'<= {row["target_size_usd_millions"]:.2f}',
                row['fund_size_usd_millions'],
                f"Fund size (${row['fund_size_usd_millions']:.2f}M) exceeds target (${row['target_size_usd_millions']:.2f}M)"
            )
            issues_found += 1
        
        duration = time.time() - start_time
        print(f"   ✅ Completed in {duration:.2f}s - Found {issues_found} issues")
        
        return issues_found
    
    def validate_timeliness(self):
        """
        Rule 4: Check data freshness and reporting timeliness
        """
        print("\n🔍 Rule 4: Timeliness Validation")
        start_time = time.time()
        issues_found = 0
        
        df = pd.read_sql('SELECT * FROM standardized_funds', self.conn)
        df['last_updated'] = pd.to_datetime(df['last_updated'])
        
        # Check for stale data
        cutoff_date = datetime.now() - timedelta(days=QUALITY_THRESHOLDS['timeliness_days'])
        stale = df[df['last_updated'] < cutoff_date]
        
        for _, row in stale.iterrows():
            days_old = (datetime.now() - row['last_updated']).days
            if days_old > 365:
                severity = 'Critical'
            elif days_old > 180:
                severity = 'High'
            else:
                severity = 'Medium'
            
            self.log_issue(
                row['fund_id'],
                'Timeliness',
                severity,
                'last_updated',
                f'Within {QUALITY_THRESHOLDS["timeliness_days"]} days',
                row['last_updated'].strftime('%Y-%m-%d'),
                f"Stale data: Last updated {days_old} days ago"
            )
            issues_found += 1
        
        duration = time.time() - start_time
        print(f"   ✅ Completed in {duration:.2f}s - Found {issues_found} issues")
        
        return issues_found
    
    def validate_duplicates(self):
        """
        Rule 5: Check for duplicate records
        """
        print("\n🔍 Rule 5: Duplicate Detection")
        start_time = time.time()
        issues_found = 0
        
        df = pd.read_sql('SELECT * FROM standardized_funds', self.conn)
        
        # Check for exact duplicate fund names within same manager
        duplicates = df.groupby(['manager_name', 'fund_name']).size().reset_index(name='count')
        duplicates = duplicates[duplicates['count'] > 1]
        
        for _, dup in duplicates.iterrows():
            matching_funds = df[
                (df['manager_name'] == dup['manager_name']) & 
                (df['fund_name'] == dup['fund_name'])
            ]
            
            for _, row in matching_funds.iterrows():
                self.log_issue(
                    row['fund_id'],
                    'Duplicates',
                    'High',
                    'fund_name',
                    'Unique within manager',
                    row['fund_name'],
                    f"Duplicate fund name: {dup['count']} funds named '{dup['fund_name']}' from {dup['manager_name']}"
                )
                issues_found += 1
        
        duration = time.time() - start_time
        print(f"   ✅ Completed in {duration:.2f}s - Found {issues_found} issues")
        
        return issues_found
    
    def validate_referential_integrity(self):
        """
        Rule 6: Check foreign key relationships
        """
        print("\n🔍 Rule 6: Referential Integrity")
        start_time = time.time()
        issues_found = 0
        
        # Check that all performance fund_ids exist in fund master
        funds_df = pd.read_sql('SELECT fund_id FROM standardized_funds', self.conn)
        perf_df = pd.read_sql('SELECT DISTINCT fund_id FROM standardized_performance', self.conn)
        
        valid_fund_ids = set(funds_df['fund_id'])
        perf_fund_ids = set(perf_df['fund_id'])
        
        orphaned_records = perf_fund_ids - valid_fund_ids
        
        if orphaned_records:
            for fund_id in orphaned_records:
                self.log_issue(
                    fund_id,
                    'Referential Integrity',
                    'High',
                    'fund_id',
                    'Exists in fund master',
                    fund_id,
                    f"Performance records exist for fund_id '{fund_id}' but fund not in master data"
                )
                issues_found += 1
        
        duration = time.time() - start_time
        print(f"   ✅ Completed in {duration:.2f}s - Found {issues_found} issues")
        
        return issues_found
    
    def validate_cross_source(self):
        """
        Rule 7: Cross-source validation (manager vs regulatory data)
        """
        print("\n🔍 Rule 7: Cross-Source Validation")
        start_time = time.time()
        issues_found = 0
        
        # Compare manager-reported AUM with regulatory filings
        funds_df = pd.read_sql('SELECT fund_id, fund_size_usd_millions FROM standardized_funds', self.conn)
        reg_df = pd.read_sql('SELECT fund_id, reported_aum_millions FROM raw_regulatory', self.conn)
        
        # Get latest regulatory filing per fund
        reg_latest = reg_df.groupby('fund_id')['reported_aum_millions'].first().reset_index()
        
        # Merge and compare
        comparison = funds_df.merge(reg_latest, on='fund_id', how='inner')
        comparison['variance_pct'] = abs(
            (comparison['fund_size_usd_millions'] - comparison['reported_aum_millions']) / 
            comparison['reported_aum_millions']
        ) * 100
        
        # Flag significant variances
        threshold = QUALITY_THRESHOLDS['variance_threshold']
        significant_variance = comparison[comparison['variance_pct'] > threshold]
        
        for _, row in significant_variance.iterrows():
            if row['variance_pct'] > 30:
                severity = 'Critical'
            elif row['variance_pct'] > 15:
                severity = 'High'
            else:
                severity = 'Medium'
            
            self.log_issue(
                row['fund_id'],
                'Cross-Source Variance',
                severity,
                'fund_size_usd_millions',
                f'${row["reported_aum_millions"]:.2f}M (regulatory)',
                f'${row["fund_size_usd_millions"]:.2f}M (manager)',
                f"Significant variance between manager-reported (${row['fund_size_usd_millions']:.2f}M) and regulatory filing (${row['reported_aum_millions']:.2f}M): {row['variance_pct']:.1f}%"
            )
            issues_found += 1
        
        duration = time.time() - start_time
        print(f"   ✅ Completed in {duration:.2f}s - Found {issues_found} issues")
        
        return issues_found
    
    def save_issues(self):
        """
        Save all identified issues to database
        """
        print("\n💾 Saving validation results...")
        start_time = time.time()
        
        if self.issues:
            issues_df = pd.DataFrame(self.issues)
            issues_df.to_sql('quality_issues', self.conn, if_exists='replace', index=False)
            duration = time.time() - start_time
            log_audit('VALIDATE', 'quality_issues', len(self.issues), duration, 'SUCCESS')
            print(f"   ✅ Saved {len(self.issues)} issues to database")
        else:
            print("   ℹ️  No issues to save")
        
        if self.alerts:
            alerts_df = pd.DataFrame(self.alerts)
            alerts_df.to_sql('quality_alerts', self.conn, if_exists='replace', index=False)
            print(f"   🚨 Created {len(self.alerts)} critical alerts")
            
            # Write alerts to CSV for immediate visibility
            alerts_path = get_output_path('CRITICAL_ALERTS.csv')
            alerts_df.to_csv(alerts_path, index=False)
            print(f"   📄 Critical alerts exported to {alerts_path}")
    
    def print_summary(self):
        """
        Print validation summary
        """
        print("\n" + "="*70)
        print("DATA QUALITY VALIDATION SUMMARY".center(70))
        print("="*70)
        
        print(f"\n📊 Issues by Severity:")
        print(f"   • Critical:  {self.stats['critical_issues']:>6}  🔴")
        print(f"   • High:      {self.stats['high_issues']:>6}  🟠")
        print(f"   • Medium:    {self.stats['medium_issues']:>6}  🟡")
        print(f"   • Low:       {self.stats['low_issues']:>6}  🟢")
        print(f"   {'─'*50}")
        print(f"   • Total:     {self.stats['total_issues']:>6}")
        
        if self.alerts:
            print(f"\n🚨 Critical Alerts Generated: {len(self.alerts)}")
            print("   ⚠️  Requires immediate attention!")
        
        # Calculate overall pass rate
        total_funds = pd.read_sql('SELECT COUNT(*) as count FROM standardized_funds', self.conn).iloc[0]['count']
        funds_with_issues = len(set([issue['fund_id'] for issue in self.issues]))
        clean_funds = total_funds - funds_with_issues
        pass_rate = (clean_funds / total_funds) * 100 if total_funds > 0 else 0
        
        print(f"\n📈 Overall Quality Metrics:")
        print(f"   • Total Funds:           {total_funds:>6}")
        print(f"   • Funds with Issues:     {funds_with_issues:>6}")
        print(f"   • Clean Funds:           {clean_funds:>6}")
        print(f"   • Overall Pass Rate:     {pass_rate:>6.2f}%")
        
        print("\n" + "="*70)
    
    def close(self):
        """Close database connection"""
        self.conn.close()

def main():
    """
    Main validation execution
    """
    print_header("BLOOMBERG DQ SYSTEM - DATA QUALITY VALIDATION")
    
    # Create validator
    validator = DataQualityValidator()
    
    # Run all validation rules
    print("\n🎯 Executing Data Quality Rules...")
    print("="*70)
    
    validator.validate_completeness()
    validator.validate_accuracy()
    validator.validate_consistency()
    validator.validate_timeliness()
    validator.validate_duplicates()
    validator.validate_referential_integrity()
    validator.validate_cross_source()
    
    # Save results
    validator.save_issues()
    
    # Print summary
    validator.print_summary()
    
    # Cleanup
    validator.close()

if __name__ == '__main__':
    main()
