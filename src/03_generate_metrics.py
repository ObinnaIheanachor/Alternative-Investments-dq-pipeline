"""
Data Quality Metrics Generation Module
Calculates and tracks quality KPIs over time
"""

import pandas as pd
import numpy as np
import time
from datetime import datetime
from config import (get_db_connection, QUALITY_THRESHOLDS, MANAGER_TIERS,
                   get_manager_tier, log_audit, print_header)

class MetricsGenerator:
    
    def __init__(self):
        self.conn = get_db_connection()
        self.metrics = []
        self.metric_date = datetime.now().date().isoformat()
        self.calculation_timestamp = datetime.now().isoformat()
    
    def log_metric(self, metric_name, metric_value, target_value, 
                   entity_type='System', entity_name='Overall'):
        """
        Log a quality metric
        """
        self.metrics.append({
            'metric_date': self.metric_date,
            'metric_name': metric_name,
            'metric_value': round(metric_value, 2),
            'target_value': round(target_value, 2),
            'entity_type': entity_type,
            'entity_name': entity_name,
            'calculation_timestamp': self.calculation_timestamp
        })
    
    def calculate_completeness_score(self):
        """
        Calculate overall data completeness score
        """
        print("\n📊 Calculating Completeness Metrics...")
        start_time = time.time()
        
        df = pd.read_sql('SELECT * FROM standardized_funds', self.conn)
        
        # Define required fields (critical for business operations)
        required_fields = [
            'fund_id', 'fund_name', 'manager_name', 'fund_type', 
            'vintage_year', 'fund_size_usd_millions', 'administrator',
            'strategy', 'geography', 'sector_focus'
        ]
        
        total_cells = len(df) * len(required_fields)
        populated_cells = sum(df[field].notna().sum() for field in required_fields)
        completeness_score = (populated_cells / total_cells) * 100 if total_cells > 0 else 0
        
        # Log system-level metric
        self.log_metric(
            'Completeness Score',
            completeness_score,
            QUALITY_THRESHOLDS['completeness_target']
        )
        
        # Calculate by fund type
        for fund_type in df['fund_type'].unique():
            type_df = df[df['fund_type'] == fund_type]
            type_total = len(type_df) * len(required_fields)
            type_populated = sum(type_df[field].notna().sum() for field in required_fields)
            type_score = (type_populated / type_total) * 100 if type_total > 0 else 0
            
            self.log_metric(
                'Completeness Score',
                type_score,
                QUALITY_THRESHOLDS['completeness_target'],
                entity_type='Fund Type',
                entity_name=fund_type
            )
        
        duration = time.time() - start_time
        print(f"   ✅ Completeness Score: {completeness_score:.2f}% (Target: {QUALITY_THRESHOLDS['completeness_target']}%)")
        print(f"   ⏱️  Duration: {duration:.2f}s")
        
        return completeness_score
    
    def calculate_accuracy_score(self):
        """
        Calculate overall data accuracy score
        """
        print("\n📊 Calculating Accuracy Metrics...")
        start_time = time.time()
        
        # Get total records
        total_funds = pd.read_sql('SELECT COUNT(*) as count FROM standardized_funds', self.conn).iloc[0]['count']
        
        # Get records with accuracy/consistency issues
        issues_df = pd.read_sql(
            "SELECT DISTINCT fund_id FROM quality_issues WHERE issue_type IN ('Accuracy', 'Consistency')", 
            self.conn
        )
        
        records_with_issues = len(issues_df) if len(issues_df) > 0 else 0
        records_passing = total_funds - records_with_issues
        accuracy_score = (records_passing / total_funds) * 100 if total_funds > 0 else 100
        
        # Log system-level metric
        self.log_metric(
            'Accuracy Score',
            accuracy_score,
            QUALITY_THRESHOLDS['accuracy_target']
        )
        
        # Calculate by fund type
        funds_df = pd.read_sql('SELECT fund_id, fund_type FROM standardized_funds', self.conn)
        
        for fund_type in funds_df['fund_type'].unique():
            type_funds = funds_df[funds_df['fund_type'] == fund_type]
            type_total = len(type_funds)
            
            if len(issues_df) > 0:
                type_issues = len(issues_df[issues_df['fund_id'].isin(type_funds['fund_id'])])
            else:
                type_issues = 0
            
            type_passing = type_total - type_issues
            type_score = (type_passing / type_total) * 100 if type_total > 0 else 100
            
            self.log_metric(
                'Accuracy Score',
                type_score,
                QUALITY_THRESHOLDS['accuracy_target'],
                entity_type='Fund Type',
                entity_name=fund_type
            )
        
        duration = time.time() - start_time
        print(f"   ✅ Accuracy Score: {accuracy_score:.2f}% (Target: {QUALITY_THRESHOLDS['accuracy_target']}%)")
        print(f"   ⏱️  Duration: {duration:.2f}s")
        
        return accuracy_score
    
    def calculate_timeliness_score(self):
        """
        Calculate data timeliness score
        """
        print("\n📊 Calculating Timeliness Metrics...")
        start_time = time.time()
        
        df = pd.read_sql('SELECT * FROM standardized_funds', self.conn)
        df['last_updated'] = pd.to_datetime(df['last_updated'])
        
        current_date = pd.Timestamp.now()
        df['days_old'] = (current_date - df['last_updated']).dt.days
        
        timely_records = len(df[df['days_old'] <= QUALITY_THRESHOLDS['timeliness_days']])
        timeliness_score = (timely_records / len(df)) * 100 if len(df) > 0 else 100
        
        # Log system-level metric
        self.log_metric(
            'Timeliness Score',
            timeliness_score,
            95.0  # Target: 95% of data updated within threshold
        )
        
        # Calculate by fund type
        for fund_type in df['fund_type'].unique():
            type_df = df[df['fund_type'] == fund_type]
            type_timely = len(type_df[type_df['days_old'] <= QUALITY_THRESHOLDS['timeliness_days']])
            type_score = (type_timely / len(type_df)) * 100 if len(type_df) > 0 else 100
            
            self.log_metric(
                'Timeliness Score',
                type_score,
                95.0,
                entity_type='Fund Type',
                entity_name=fund_type
            )
        
        duration = time.time() - start_time
        print(f"   ✅ Timeliness Score: {timeliness_score:.2f}% (Target: 95%)")
        print(f"   ⏱️  Duration: {duration:.2f}s")
        
        return timeliness_score
    
    def calculate_manager_quality_scores(self):
        """
        Calculate quality score for each manager
        """
        print("\n📊 Calculating Manager Quality Scores...")
        start_time = time.time()
        
        funds_df = pd.read_sql('SELECT fund_id, manager_name FROM standardized_funds', self.conn)
        issues_df = pd.read_sql('SELECT fund_id FROM quality_issues', self.conn)
        
        managers_processed = 0
        
        for manager in funds_df['manager_name'].unique():
            manager_funds = funds_df[funds_df['manager_name'] == manager]
            total_funds = len(manager_funds)
            
            if len(issues_df) > 0:
                manager_issues = len(issues_df[issues_df['fund_id'].isin(manager_funds['fund_id'])])
            else:
                manager_issues = 0
            
            clean_funds = total_funds - manager_issues
            quality_score = (clean_funds / total_funds) * 100 if total_funds > 0 else 100
            
            # Determine tier
            tier = get_manager_tier(quality_score)
            
            # Log metric
            self.log_metric(
                'Manager Quality Score',
                quality_score,
                85.0,  # Target: Tier 2 minimum
                entity_type='Manager',
                entity_name=manager
            )
            
            # Log tier
            self.log_metric(
                'Manager Quality Tier',
                quality_score,  # Store score, tier derived in visualization
                85.0,
                entity_type='Manager',
                entity_name=manager
            )
            
            managers_processed += 1
        
        duration = time.time() - start_time
        print(f"   ✅ Processed {managers_processed} managers")
        print(f"   ⏱️  Duration: {duration:.2f}s")
        
        return managers_processed
    
    def calculate_overall_dq_score(self):
        """
        Calculate weighted overall data quality score
        """
        print("\n📊 Calculating Overall DQ Score...")
        start_time = time.time()
        
        # Get component scores from already calculated metrics
        completeness = next((m['metric_value'] for m in self.metrics 
                           if m['metric_name'] == 'Completeness Score' 
                           and m['entity_type'] == 'System'), 100)
        
        accuracy = next((m['metric_value'] for m in self.metrics 
                       if m['metric_name'] == 'Accuracy Score' 
                       and m['entity_type'] == 'System'), 100)
        
        timeliness = next((m['metric_value'] for m in self.metrics 
                         if m['metric_name'] == 'Timeliness Score' 
                         and m['entity_type'] == 'System'), 100)
        
        # Weighted average (accuracy most important for Bloomberg)
        overall_score = (
            completeness * 0.30 +  # 30% weight
            accuracy * 0.50 +      # 50% weight (most critical)
            timeliness * 0.20      # 20% weight
        )
        
        # Log metric
        self.log_metric(
            'Overall Data Quality Score',
            overall_score,
            90.0  # Target: 90%
        )
        
        duration = time.time() - start_time
        print(f"   ✅ Overall DQ Score: {overall_score:.2f}% (Target: 90%)")
        print(f"      Components: Completeness {completeness:.1f}% | Accuracy {accuracy:.1f}% | Timeliness {timeliness:.1f}%")
        print(f"   ⏱️  Duration: {duration:.2f}s")
        
        return overall_score
    
    def calculate_issue_metrics(self):
        """
        Calculate metrics about issues found
        """
        print("\n📊 Calculating Issue Metrics...")
        start_time = time.time()
        
        issues_df = pd.read_sql('SELECT * FROM quality_issues', self.conn)
        
        if len(issues_df) == 0:
            print("   ℹ️  No issues to analyze")
            return
        
        # Total issues
        self.log_metric('Total Issues', len(issues_df), 0)
        
        # Issues by severity
        for severity in ['Critical', 'High', 'Medium', 'Low']:
            count = len(issues_df[issues_df['severity'] == severity])
            self.log_metric(f'{severity} Issues', count, 0)
        
        # Issues by type
        for issue_type in issues_df['issue_type'].unique():
            count = len(issues_df[issues_df['issue_type'] == issue_type])
            self.log_metric(f'Issues - {issue_type}', count, 0)
        
        duration = time.time() - start_time
        print(f"   ✅ Issue metrics calculated")
        print(f"   ⏱️  Duration: {duration:.2f}s")
    
    def save_metrics(self):
        """
        Save all metrics to database
        """
        print("\n💾 Saving metrics to database...")
        start_time = time.time()
        
        if self.metrics:
            metrics_df = pd.DataFrame(self.metrics)
            metrics_df.to_sql('quality_metrics', self.conn, if_exists='append', index=False)
            duration = time.time() - start_time
            
            log_audit('METRICS', 'quality_metrics', len(self.metrics), duration, 'SUCCESS')
            print(f"   ✅ Saved {len(self.metrics)} metrics")
            print(f"   ⏱️  Duration: {duration:.2f}s")
        else:
            print("   ℹ️  No metrics to save")
    
    def generate_summary_report(self):
        """
        Generate executive summary report for export
        """
        print("\n📄 Generating Executive Summary Report...")
        start_time = time.time()
        
        # Get key metrics
        metrics_df = pd.DataFrame(self.metrics)
        issues_df = pd.read_sql('SELECT * FROM quality_issues', self.conn)
        
        overall_score = metrics_df[
            (metrics_df['metric_name'] == 'Overall Data Quality Score') &
            (metrics_df['entity_type'] == 'System')
        ]['metric_value'].values[0]
        
        completeness = metrics_df[
            (metrics_df['metric_name'] == 'Completeness Score') &
            (metrics_df['entity_type'] == 'System')
        ]['metric_value'].values[0]
        
        accuracy = metrics_df[
            (metrics_df['metric_name'] == 'Accuracy Score') &
            (metrics_df['entity_type'] == 'System')
        ]['metric_value'].values[0]
        
        timeliness = metrics_df[
            (metrics_df['metric_name'] == 'Timeliness Score') &
            (metrics_df['entity_type'] == 'System')
        ]['metric_value'].values[0]
        
        # Create summary
        summary = {
            'Report Date': self.metric_date,
            'Report Time': datetime.now().strftime('%H:%M:%S'),
            'Overall DQ Score': f"{overall_score:.2f}%",
            'Completeness Score': f"{completeness:.2f}%",
            'Accuracy Score': f"{accuracy:.2f}%",
            'Timeliness Score': f"{timeliness:.2f}%",
            'Total Issues': len(issues_df),
            'Critical Issues': len(issues_df[issues_df['severity'] == 'Critical']),
            'High Issues': len(issues_df[issues_df['severity'] == 'High']),
            'Medium Issues': len(issues_df[issues_df['severity'] == 'Medium']),
            'Low Issues': len(issues_df[issues_df['severity'] == 'Low']),
            'Funds Analyzed': pd.read_sql('SELECT COUNT(*) as c FROM standardized_funds', self.conn).iloc[0]['c'],
            'Performance Records': pd.read_sql('SELECT COUNT(*) as c FROM standardized_performance', self.conn).iloc[0]['c'],
            'Status': '🟢 PASS' if overall_score >= 90 else '🟡 WARNING' if overall_score >= 75 else '🔴 FAIL'
        }
        
        # Save to CSV
        summary_df = pd.DataFrame([summary])
        summary_df.to_csv('../data/output/executive_summary.csv', index=False)
        
        duration = time.time() - start_time
        print(f"   ✅ Summary report saved to data/output/executive_summary.csv")
        print(f"   ⏱️  Duration: {duration:.2f}s")
        
        return summary
    
    def print_summary(self):
        """
        Print metrics generation summary
        """
        print("\n" + "="*70)
        print("METRICS GENERATION SUMMARY".center(70))
        print("="*70)
        
        # Get overall score
        overall_score = next((m['metric_value'] for m in self.metrics 
                            if m['metric_name'] == 'Overall Data Quality Score'), 0)
        
        print(f"\n🎯 Overall Data Quality Score: {overall_score:.2f}%")
        
        # Status indicator
        if overall_score >= 90:
            status = "🟢 EXCELLENT - Meets Bloomberg standards"
        elif overall_score >= 80:
            status = "🟡 GOOD - Minor improvements needed"
        elif overall_score >= 70:
            status = "🟠 FAIR - Significant improvements required"
        else:
            status = "🔴 POOR - Critical improvements required"
        
        print(f"   Status: {status}")
        
        # Component scores
        print(f"\n📊 Component Scores:")
        for metric_name in ['Completeness Score', 'Accuracy Score', 'Timeliness Score']:
            score = next((m['metric_value'] for m in self.metrics 
                        if m['metric_name'] == metric_name and m['entity_type'] == 'System'), 0)
            target = next((m['target_value'] for m in self.metrics 
                         if m['metric_name'] == metric_name and m['entity_type'] == 'System'), 0)
            
            status_icon = "✅" if score >= target else "⚠️"
            print(f"   {status_icon} {metric_name}: {score:.2f}% (Target: {target:.0f}%)")
        
        # Total metrics generated
        print(f"\n📈 Metrics Generated:")
        print(f"   • Total Metrics:        {len(self.metrics):>6}")
        print(f"   • System-level:         {len([m for m in self.metrics if m['entity_type'] == 'System']):>6}")
        print(f"   • Manager-level:        {len([m for m in self.metrics if m['entity_type'] == 'Manager']):>6}")
        print(f"   • Fund Type-level:      {len([m for m in self.metrics if m['entity_type'] == 'Fund Type']):>6}")
        
        print("\n" + "="*70)
    
    def close(self):
        """Close database connection"""
        self.conn.close()

def main():
    """
    Main metrics generation execution
    """
    print_header("BLOOMBERG DQ SYSTEM - METRICS GENERATION")
    
    # Create generator
    generator = MetricsGenerator()
    
    # Calculate all metrics
    print("\n🎯 Calculating Data Quality Metrics...")
    print("="*70)
    
    generator.calculate_completeness_score()
    generator.calculate_accuracy_score()
    generator.calculate_timeliness_score()
    generator.calculate_manager_quality_scores()
    generator.calculate_overall_dq_score()
    generator.calculate_issue_metrics()
    
    # Save results
    generator.save_metrics()
    generator.generate_summary_report()
    
    # Print summary
    generator.print_summary()
    
    # Cleanup
    generator.close()

if __name__ == '__main__':
    main()
