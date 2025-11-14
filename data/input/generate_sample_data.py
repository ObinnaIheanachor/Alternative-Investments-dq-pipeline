"""
Generate Sample Alternative Investment Data
Includes intentional data quality issues for demonstration
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import random

print("="*70)
print("GENERATING SAMPLE DATA FOR BLOOMBERG DQ SYSTEM")
print("="*70)

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Tunable dataset sizes for richer visuals
NUM_PE_FUNDS = 75
NUM_HF_FUNDS = 45
NUM_VC_FUNDS = 40
TOTAL_FUNDS = NUM_PE_FUNDS + NUM_HF_FUNDS + NUM_VC_FUNDS

# Spotlight managers to demonstrate quality improvements
ELITE_MANAGERS = ['Sequoia Capital', 'Andreessen Horowitz', 'Acme Capital Partners']
ELITE_ADMINISTRATOR = 'Northern Trust - Elite Service'
elite_fund_ids = set()

# ============================================================================
# DATASET 1: FUND MASTER DATA (CSV FORMAT) - 150+ records
# ============================================================================
print("\n📊 Generating Fund Master Data (CSV)...")

# Generate fund IDs
pe_funds = [f'PE{str(i).zfill(3)}' for i in range(1, NUM_PE_FUNDS + 1)]
hf_funds = [f'HF{str(i).zfill(3)}' for i in range(1, NUM_HF_FUNDS + 1)]
vc_funds = [f'VC{str(i).zfill(3)}' for i in range(1, NUM_VC_FUNDS + 1)]
all_fund_ids = pe_funds + hf_funds + vc_funds

# Manager names (realistic)
managers = [
    'Acme Capital Partners', 'Vista Equity Partners', 'KKR & Co',
    'Blackstone Group', 'Apollo Global Management', 'Carlyle Group',
    'TPG Capital', 'Warburg Pincus', 'Silver Lake Partners',
    'Sequoia Capital', 'Andreessen Horowitz', 'Benchmark Capital',
    'Tiger Global Management', 'Bridgewater Associates', 'Renaissance Technologies'
]

# Generate fund data
fund_data = {
    'fund_id': all_fund_ids,
    'fund_name': [f'{random.choice(["Alpha", "Beta", "Gamma", "Delta", "Omega", "Prime", "Summit", "Peak"])} Fund {random.choice(["I", "II", "III", "IV", "V", "VI"])}' for _ in all_fund_ids],
    'manager_name': [random.choice(managers) for _ in all_fund_ids],
    'fund_type': ['Private Equity'] * NUM_PE_FUNDS + ['Hedge Fund'] * NUM_HF_FUNDS + ['Venture Capital'] * NUM_VC_FUNDS,
    'strategy': [],
    'vintage_year': [],
    'inception_date': [],
    'fund_size_millions': [],
    'currency': [],
    'target_size_millions': [],
    'status': [],
    'geography': [],
    'sector_focus': [],
    'administrator': [],
    'last_updated': []
}

# Strategy mapping by fund type
strategy_map = {
    'Private Equity': ['Buyout', 'Growth Equity', 'Distressed', 'Mezzanine'],
    'Hedge Fund': ['Long/Short Equity', 'Global Macro', 'Event Driven', 'Quantitative', 'Multi-Strategy'],
    'Venture Capital': ['Early Stage', 'Late Stage', 'Seed', 'Growth']
}

geography_options = ['North America', 'Europe', 'Asia Pacific', 'Global', 'Latin America', 'Middle East']
sector_options = ['Technology', 'Healthcare', 'Financial Services', 'Industrials', 'Consumer', 'Energy', 'Real Estate', 'Multi-Sector']
admin_options = ['SS&C Technologies', 'Citco', 'Northern Trust', 'State Street', 'BNY Mellon', None]
status_options = ['Fundraising', 'Investing', 'Harvesting', 'Liquidated']
currency_options = ['USD', 'EUR', 'GBP', 'JPY', 'CHF']

# Populate fields
for i, fund_id in enumerate(all_fund_ids):
    fund_type = fund_data['fund_type'][i]
    
    # Strategy
    fund_data['strategy'].append(random.choice(strategy_map[fund_type]))
    
    # Vintage year
    vintage = random.randint(2015, 2024)
    fund_data['vintage_year'].append(vintage)
    
    # Inception date (around vintage year)
    inception = datetime(vintage, random.randint(1, 12), random.randint(1, 28))
    fund_data['inception_date'].append(inception.strftime('%Y-%m-%d'))
    
    # Fund size (varies by type)
    if fund_type == 'Private Equity':
        size = round(random.uniform(500, 5000), 2)
    elif fund_type == 'Hedge Fund':
        size = round(random.uniform(1000, 15000), 2)
    else:  # VC
        size = round(random.uniform(100, 2000), 2)
    
    fund_data['fund_size_millions'].append(size)
    
    # Currency (mostly USD, some others)
    currency = random.choices(currency_options, weights=[0.70, 0.15, 0.10, 0.03, 0.02])[0]
    fund_data['currency'].append(currency)
    
    # Target size (usually same or slightly higher)
    if random.random() > 0.25:
        target = round(size * random.uniform(1.0, 1.2), 2)
    else:
        target = None
    fund_data['target_size_millions'].append(target)
    
    # Status
    fund_data['status'].append(random.choice(status_options))
    
    # Geography
    fund_data['geography'].append(random.choice(geography_options))
    
    # Sector
    fund_data['sector_focus'].append(random.choice(sector_options))
    
    # Administrator (some missing - data quality issue)
    admin = random.choices(admin_options, weights=[0.25, 0.20, 0.15, 0.15, 0.15, 0.10])[0]
    fund_data['administrator'].append(admin)
    
    # Last updated (recent dates, some stale)
    if random.random() > 0.20:  # 80% recent
        days_ago = random.randint(1, 90)
    else:  # 20% stale (data quality issue)
        days_ago = random.randint(180, 540)
    
    last_update = datetime.now() - timedelta(days=days_ago)
    fund_data['last_updated'].append(last_update.strftime('%Y-%m-%d'))

# Create DataFrame
funds_df = pd.DataFrame(fund_data)

# ============================================================================
# INJECT DATA QUALITY ISSUES INTO FUND MASTER
# ============================================================================
print("   💉 Injecting data quality issues...")

# Issue 1: Negative fund size (Critical - Accuracy)
funds_df.loc[7, 'fund_size_millions'] = -250.00
funds_df.loc[23, 'fund_size_millions'] = -1500.00

# Issue 2: Future vintage year (High - Accuracy)
funds_df.loc[15, 'vintage_year'] = 2027
funds_df.loc[42, 'vintage_year'] = 2026

# Issue 3: Missing administrators (Medium - Completeness)
funds_df.loc[[12, 28, 35, 49, 67, 88], 'administrator'] = None

# Issue 4: Duplicate fund names (High - Duplicates)
funds_df.loc[50, 'fund_name'] = funds_df.loc[49, 'fund_name']
funds_df.loc[51, 'fund_name'] = funds_df.loc[49, 'fund_name']

# Issue 5: Fund size exceeds target (Medium - Consistency)
funds_df.loc[30, 'fund_size_millions'] = 5000
funds_df.loc[30, 'target_size_millions'] = 3000

# Issue 6: Inception date before realistic period (High - Accuracy)
funds_df.loc[18, 'inception_date'] = '1985-01-15'
funds_df.loc[18, 'vintage_year'] = 1985

# Issue 7: Regional timeliness problem (Low geography coverage → Timeliness)
latam_indexes = funds_df[funds_df['geography'] == 'Latin America'].head(5).index
for idx in latam_indexes:
    stale_days = random.randint(365, 720)
    last_update = datetime.now() - timedelta(days=stale_days)
    funds_df.loc[idx, 'last_updated'] = last_update.strftime('%Y-%m-%d')

# Issue 8: Administrator gaps for smaller managers
small_funds = funds_df[funds_df['fund_size_millions'] < 300]
if not small_funds.empty:
    sample_size = min(5, len(small_funds))
    smaller_managers = small_funds.sample(sample_size, random_state=1).index
    funds_df.loc[smaller_managers, 'administrator'] = None

# Issue 9: Duplicate manager + sector combos (helps duplicate visuals)
funds_df.loc[TOTAL_FUNDS - 3, 'manager_name'] = funds_df.loc[TOTAL_FUNDS - 4, 'manager_name']
funds_df.loc[TOTAL_FUNDS - 3, 'sector_focus'] = funds_df.loc[TOTAL_FUNDS - 4, 'sector_focus']
funds_df.loc[TOTAL_FUNDS - 2, 'manager_name'] = funds_df.loc[TOTAL_FUNDS - 4, 'manager_name']
funds_df.loc[TOTAL_FUNDS - 2, 'sector_focus'] = funds_df.loc[TOTAL_FUNDS - 5, 'sector_focus']

# Curate high-quality elite managers for higher scores
elite_mask = funds_df['manager_name'].isin(ELITE_MANAGERS)
if elite_mask.any():
    for idx, (row_idx, row) in enumerate(funds_df[elite_mask].iterrows(), start=1):
        fund_size = abs(row['fund_size_millions']) if pd.notna(row['fund_size_millions']) else random.uniform(500, 2000)
        funds_df.at[row_idx, 'fund_size_millions'] = round(fund_size, 2)
        funds_df.at[row_idx, 'target_size_millions'] = round(fund_size * random.uniform(1.05, 1.2), 2)
        funds_df.at[row_idx, 'administrator'] = ELITE_ADMINISTRATOR
        funds_df.at[row_idx, 'currency'] = 'USD'
        funds_df.at[row_idx, 'last_updated'] = (datetime.now() - timedelta(days=random.randint(5, 45))).strftime('%Y-%m-%d')
        # Ensure fund names stay unique for these managers
        funds_df.at[row_idx, 'fund_name'] = f"{row['fund_name'].split(' Fund')[0]} Elite {idx}"
    elite_fund_ids = set(funds_df.loc[elite_mask, 'fund_id'])
else:
    elite_fund_ids = set()

# Save to CSV
funds_df.to_csv('fund_master.csv', index=False)
print(f"   ✅ Created fund_master.csv: {len(funds_df)} records")
print(f"   ⚠️  Injected 15+ data quality issues")

# ============================================================================
# DATASET 2: FUND PERFORMANCE DATA (JSON FORMAT) - 400+ records
# ============================================================================
print("\n📊 Generating Fund Performance Data (JSON)...")

performance_records = []

# Generate quarterly performance for PE and VC funds
pe_vc_funds = pe_funds + vc_funds  # broader coverage
quarters = ['2023-Q3', '2023-Q4', '2024-Q1', '2024-Q2', '2024-Q3']
quarter_dates = {
    '2023-Q3': '2023-09-30',
    '2023-Q4': '2023-12-31',
    '2024-Q1': '2024-03-31',
    '2024-Q2': '2024-06-30',
    '2024-Q3': '2024-09-30'
}

for fund_id in pe_vc_funds:
    fund_type = 'PE' if fund_id.startswith('PE') else 'VC'
    
    # Not all funds report all quarters (missing data issue)
    num_quarters = random.choice([3, 4, 5])
    reporting_quarters = random.sample(quarters, num_quarters)
    
    for quarter in reporting_quarters:
        # Generate performance metrics
        if fund_type == 'PE':
            irr = round(random.uniform(-5, 35), 2)
            dpi = round(random.uniform(0, 2.5), 2)
            rvpi = round(random.uniform(0.3, 2.0), 2)
            tvpi = round(dpi + rvpi, 2)  # Should be DPI + RVPI
        else:  # VC
            irr = round(random.uniform(-15, 60), 2)  # Higher variance for VC
            dpi = round(random.uniform(0, 1.5), 2)
            rvpi = round(random.uniform(0.5, 3.0), 2)
            tvpi = round(dpi + rvpi, 2)
        
        capital_called = round(random.uniform(50, 500), 2)
        distributions = round(capital_called * dpi, 2)
        remaining_value = round(capital_called * rvpi, 2)
        
        record = {
            'fund_id': fund_id,
            'report_date': quarter_dates[quarter],
            'report_quarter': quarter,
            'irr_net_pct': irr,
            'moic': round(tvpi, 2),
            'dpi': dpi,
            'rvpi': rvpi,
            'tvpi': tvpi,
            'capital_called_millions': capital_called,
            'distributions_millions': distributions,
            'remaining_value_millions': remaining_value
        }
        
        performance_records.append(record)

# Generate monthly NAV for Hedge Funds
for fund_id in hf_funds:
    # HF report monthly
    months = pd.date_range(start='2023-12-31', end='2024-10-31', freq='M')
    
    # Not all HF report all months (missing periods issue)
    num_months = random.randint(6, len(months))
    reporting_months = random.sample(list(months), num_months)
    reporting_months.sort()
    
    base_nav = random.uniform(1000, 5000)
    
    for month_date in reporting_months:
        # Monthly return
        monthly_return = round(random.uniform(-3, 5), 2)
        base_nav = base_nav * (1 + monthly_return/100)
        
        quarter = f"{month_date.year}-Q{month_date.quarter}"
        
        record = {
            'fund_id': fund_id,
            'report_date': month_date.strftime('%Y-%m-%d'),
            'report_quarter': quarter,
            'nav_per_share': round(base_nav, 2),
            'monthly_return_pct': monthly_return,
            'irr_net_pct': None,
            'moic': None,
            'dpi': None,
            'rvpi': None,
            'tvpi': None,
            'capital_called_millions': None,
            'distributions_millions': None,
            'remaining_value_millions': None
        }
        
        performance_records.append(record)

# ============================================================================
# INJECT DATA QUALITY ISSUES INTO PERFORMANCE DATA
# ============================================================================
print("   💉 Injecting data quality issues...")

# Issue 1: TVPI calculation errors (High - Consistency)
performance_records[10]['tvpi'] = performance_records[10]['dpi'] + performance_records[10]['rvpi'] + 0.75
performance_records[25]['tvpi'] = performance_records[25]['dpi'] + performance_records[25]['rvpi'] - 0.50
performance_records[40]['tvpi'] = 10.0  # Completely wrong

# Issue 2: Impossible IRR values (Critical - Accuracy)
performance_records[15]['irr_net_pct'] = 250.0  # 250% IRR (implausible)
performance_records[33]['irr_net_pct'] = -150.0  # -150% IRR (impossible)

# Issue 3: Negative values (Critical - Accuracy)
performance_records[20]['dpi'] = -0.5
performance_records[55]['rvpi'] = -1.2

# Issue 4: Missing required fields (Medium - Completeness)
performance_records[8]['irr_net_pct'] = None
performance_records[8]['tvpi'] = None

# Issue 5: Hedge fund outliers (Timeliness + Accuracy)
hf_outliers = [rec for rec in performance_records if rec['fund_id'].startswith('HF')][:6]
for rec in hf_outliers:
    rec['monthly_return_pct'] = random.choice([18.5, -12.3, 22.7])
    if random.random() > 0.5:
        rec['nav_per_share'] = None  # missing NAV for completeness issue

# Keep elite manager performance metrics clean for higher scores
if elite_fund_ids:
    for rec in performance_records:
        if rec['fund_id'] in elite_fund_ids:
            if rec.get('irr_net_pct') is not None:
                rec['irr_net_pct'] = round(min(max(rec['irr_net_pct'], 8.0), 25.0), 2)
            if rec.get('dpi') is not None:
                rec['dpi'] = round(abs(rec['dpi']), 2)
            if rec.get('rvpi') is not None:
                rec['rvpi'] = round(abs(rec['rvpi']), 2)
            if rec.get('dpi') is not None and rec.get('rvpi') is not None:
                rec['tvpi'] = round(rec['dpi'] + rec['rvpi'], 2)
            if rec.get('monthly_return_pct') is not None:
                rec['monthly_return_pct'] = round(min(max(rec['monthly_return_pct'], -3.0), 6.0), 2)

# Save to JSON
with open('fund_performance.json', 'w') as f:
    json.dump(performance_records, f, indent=2)

print(f"   ✅ Created fund_performance.json: {len(performance_records)} records")
print(f"   ⚠️  Injected 10+ data quality issues")

# ============================================================================
# DATASET 3: REGULATORY FILINGS (JSON FORMAT) - 80+ records
# ============================================================================
print("\n📊 Generating Regulatory Filings Data (JSON)...")

regulatory_records = []
filing_dates = ['2024-09-15', '2024-08-15', '2024-05-15', '2024-02-15', '2023-11-15']

# Select random funds that should have regulatory filings (some funds have two filings)
filing_funds = random.sample(all_fund_ids, min(80, len(all_fund_ids)))

for fund_id in filing_funds:
    submission_count = random.choice([1, 2])  # multiple filings enable timeliness/duplicate visuals
    
    for _ in range(submission_count):
        filing_date = random.choice(filing_dates)
        dt = datetime.strptime(filing_date, '%Y-%m-%d')
        quarter = (dt.month - 1) // 3 + 1
        filing_quarter = f"{dt.year}-Q{quarter}"
        
        fund_row = funds_df[funds_df['fund_id'] == fund_id].iloc[0]
        reported_size = fund_row['fund_size_millions']
        
        # Reported AUM (vary variance more aggressively)
        if random.random() > 0.25:
            reported_aum = round(reported_size * random.uniform(0.95, 1.05), 2)
        else:
            reported_aum = round(reported_size * random.uniform(0.60, 1.40), 2)
        
        record = {
            'fund_id': fund_id,
            'filing_type': random.choice(['Form PF', 'Form ADV', 'AIFMD']),
            'filing_date': filing_date,
            'filing_quarter': filing_quarter,
            'reported_aum_millions': reported_aum,
            'reported_strategy': fund_row['strategy'],
            'num_investors': random.randint(10, 500),
            'source': random.choice(['SEC EDGAR', 'ESMA', 'FCA'])
        }
        
        regulatory_records.append(record)

# Introduce strategy mismatches for cross-source variance visuals
mismatch_count = max(5, len(regulatory_records) // 8)
strategy_values = sum(strategy_map.values(), [])

for idx in random.sample(range(len(regulatory_records)), mismatch_count):
    record = regulatory_records[idx]
    valid_choices = [s for s in strategy_values if s != record['reported_strategy']]
    record['reported_strategy'] = random.choice(valid_choices)

# Keep elite manager filings aligned with master data
if elite_fund_ids:
    fund_lookup = funds_df.set_index('fund_id')
    for record in regulatory_records:
        if record['fund_id'] in elite_fund_ids and record['fund_id'] in fund_lookup.index:
            base_size = fund_lookup.loc[record['fund_id'], 'fund_size_millions']
            record['reported_aum_millions'] = round(base_size * random.uniform(0.98, 1.03), 2)
            record['reported_strategy'] = fund_lookup.loc[record['fund_id'], 'strategy']

# Save to JSON
with open('regulatory_filings.json', 'w') as f:
    json.dump(regulatory_records, f, indent=2)

print(f"   ✅ Created regulatory_filings.json: {len(regulatory_records)} records")
print(f"   ⚠️  Includes cross-source variance issues")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("✅ DATA GENERATION COMPLETE!")
print("="*70)
print(f"\nFiles created:")
print(f"  1. fund_master.csv           : {len(funds_df)} funds")
print(f"  2. fund_performance.json     : {len(performance_records)} performance records")
print(f"  3. regulatory_filings.json   : {len(regulatory_records)} regulatory filings")
print(f"\nTotal Records: {len(funds_df) + len(performance_records) + len(regulatory_records)}")
print(f"\nData Quality Issues Injected:")
print(f"  • Completeness issues (missing administrators, null NAV, null IRR)")
print(f"  • Accuracy issues (negative values, impossible IRRs, strategy mismatches)")
print(f"  • Consistency issues (TVPI vs DPI/RVPI and fund size vs target)")
print(f"  • Timeliness issues (stale last_updated dates, sparse filings)")
print(f"  • Duplicate issues (fund names, manager/sector combos)")
print(f"  • Cross-source variance (regulatory vs master data, AUM swings)")
print("\n" + "="*70)
