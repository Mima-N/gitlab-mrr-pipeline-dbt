import pandas as pd
import numpy as np
from faker import Faker
import uuid
from datetime import timedelta

# Initialize Faker
fake = Faker()
Faker.seed(42)
np.random.seed(42)

# Configuration
NUM_ACCOUNTS = 10000
PCT_UPSELL = 0.2 

print(f"Generating GitLab-style simulation data for {NUM_ACCOUNTS} accounts...")

# --- 1. Generate Accounts (The "Dimension") ---
accounts = []
for _ in range(NUM_ACCOUNTS):
    accounts.append({
        'account_id': str(uuid.uuid4()),
        'account_name': fake.company(),
        'segment': np.random.choice(['SMB', 'Mid-Market', 'Large Enterprise'], p=[0.5, 0.3, 0.2]),
        'region': np.random.choice(['AMER', 'EMEA', 'APAC'])
    })
df_accounts = pd.DataFrame(accounts)

# --- 2. Generate Salesforce Opportunities (The "Booking") ---
opportunities = []
for _, account in df_accounts.iterrows():
    # Simulate initial deal
    close_date = fake.date_between(start_date='-2y', end_date='today')
    opp_id = str(uuid.uuid4())
    
    # Contract Term Logic: SEGMENT-BASED (more realistic than random)
    if account['segment'] == 'Large Enterprise':
        contract_term_months = np.random.choice([24, 36], p=[0.4, 0.6])  # Mostly 3-year
    elif account['segment'] == 'Mid-Market':
        contract_term_months = np.random.choice([12, 24], p=[0.7, 0.3])  # Mostly 1-year
    else:  # SMB
        contract_term_months = 12  # Always 1-year
    
    # Pricing Logic: Base annual price, then multiply by term
    base_annual_price = 10000 if account['segment'] == 'SMB' else 50000 if account['segment'] == 'Mid-Market' else 150000
    annual_amount = np.random.uniform(base_annual_price, base_annual_price * 1.5)
    # Total Contract Value (TCV) = Annual Amount × (Contract Term / 12)
    amount = round(annual_amount * (contract_term_months / 12), 2)

    # Win/Loss Logic (NEW: Some deals are lost - creates Zombie Subscriptions)
    stage_name = 'Closed Won'
    if np.random.random() < 0.05:  # 5% of deals are lost/churned
        stage_name = 'Closed Lost - Churn'

    opportunities.append({
        'opportunity_id': opp_id,
        'account_id': account['account_id'],
        'stage_name': stage_name,
        'close_date': close_date,
        'amount': amount,  # This is TCV (Total Contract Value)
        'type': 'New Business',
        'contract_term_months': contract_term_months
    })

    # Simulate Upsells (Expansion Revenue)
    if stage_name == 'Closed Won' and np.random.random() < PCT_UPSELL:
        upsell_date = close_date + timedelta(days=np.random.randint(90, 300))
        if upsell_date < pd.Timestamp.now().date():
            # Upsells are typically co-termed (12 months)
            upsell_term = 12
            upsell_amount = round(annual_amount * 0.2 * (upsell_term / 12), 2)
            
            opportunities.append({
                'opportunity_id': str(uuid.uuid4()),
                'account_id': account['account_id'],
                'stage_name': 'Closed Won',
                'close_date': upsell_date,
                'amount': upsell_amount,
                'type': 'Add-On Business',
                'contract_term_months': upsell_term
            })

df_sfdc = pd.DataFrame(opportunities)

# --- 3. Generate Zuora Subscriptions (The "Revenue") ---
subscriptions = []

for _, opp in df_sfdc.iterrows():
    # DATA DISCREPANCY 1: Zombie Subscriptions (NEW!)
    # If Sales says "Churned", Finance usually catches it... but not always
    if opp['stage_name'] == 'Closed Lost - Churn':
        if np.random.random() > 0.2:  # 80% of time Finance correctly skips billing
            continue
        # 20% of time: Finance STILL bills them (Zombie Subscription!)
    
    # DATA DISCREPANCY 2: Missing Subscriptions (Orphaned Opportunities)
    # 2% of Won deals fail to sync to billing
    if opp['stage_name'] == 'Closed Won' and np.random.random() < 0.02:
        continue 

    # DATA DISCREPANCY 3: Date Slips
    # Subscription usually starts same day, but sometimes 1-5 days later
    start_date_drift = np.random.randint(0, 5)
    sub_start_date = opp['close_date'] + timedelta(days=start_date_drift)
    
    # DATA DISCREPANCY 4: Price Variances
    # Sometimes billed amount differs from sales amount (penny errors or manual entry)
    billed_amount = opp['amount'] if np.random.random() > 0.05 else opp['amount'] + np.random.choice([-0.01, 0.01, 100])

    # Calculate subscription end date based on contract term
    contract_days = int((opp['contract_term_months'] / 12) * 365)
    sub_end_date = sub_start_date + timedelta(days=contract_days)
    
    # DISCREPANCY 5: Churn (Cancellations)
    status = 'Active'
    if np.random.random() < 0.10:  # 10% of subscriptions cancel early
        status = 'Canceled'
        # Customer cancels somewhere between day 30 and 80% through the contract
        days_to_cancel = np.random.randint(30, int(contract_days * 0.8))
        sub_end_date = sub_start_date + timedelta(days=days_to_cancel)

    # Calculate MRR based on contract term (FIXED: No longer hardcoded /12)
    mrr = round(billed_amount / opp['contract_term_months'], 2)

    subscriptions.append({
        'subscription_id': str(uuid.uuid4()),
        'crm_opportunity_id': opp['opportunity_id'],  # KEY JOIN COLUMN
        'account_id': opp['account_id'],
        'subscription_start_date': sub_start_date,
        'subscription_end_date': sub_end_date,  # FIXED: Uses calculated value
        'mrr': mrr,
        'currency': 'USD',
        'status': status  # FIXED: Uses calculated status
    })

df_zuora = pd.DataFrame(subscriptions)

# --- 4. Export ---
df_sfdc.to_csv('raw_sfdc_opportunity.csv', index=False)
df_zuora.to_csv('raw_zuora_subscription.csv', index=False)
df_accounts.to_csv('raw_sfdc_account.csv', index=False)
print("\n" + "="*60)
print("Files Generated Successfully!")
print("="*60)
print(f"\n📊 SALESFORCE OPPORTUNITIES:")
print(f"   Total Rows: {len(df_sfdc):,}")
print(f"   Closed Won: {len(df_sfdc[df_sfdc['stage_name'] == 'Closed Won']):,}")
print(f"   Closed Lost (Churn): {len(df_sfdc[df_sfdc['stage_name'] == 'Closed Lost - Churn']):,}")
print(f"   Multi-year Contracts (>12mo): {len(df_sfdc[df_sfdc['contract_term_months'] > 12]):,} ({len(df_sfdc[df_sfdc['contract_term_months'] > 12])/len(df_sfdc)*100:.1f}%)")

print(f"\n💰 ZUORA SUBSCRIPTIONS:")
print(f"   Total Rows: {len(df_zuora):,}")
print(f"   Active: {len(df_zuora[df_zuora['status'] == 'Active']):,}")
print(f"   Canceled: {len(df_zuora[df_zuora['status'] == 'Canceled']):,}")

print(f"\n🔍 DATA QUALITY ISSUES (Intentional):")
print(f"   Expected Orphaned Opportunities: ~{int(len(df_sfdc[df_sfdc['stage_name'] == 'Closed Won']) * 0.02):,}")
print(f"   Expected Zombie Subscriptions: ~{int(len(df_sfdc[df_sfdc['stage_name'] == 'Closed Lost - Churn']) * 0.2):,}")
print(f"   Expected Price Mismatches: ~{int(len(df_zuora) * 0.05):,}")
print(f"   Expected Date Slips: ~{int(len(df_zuora) * 0.8):,}")  # 80% have 1-5 day drift

print("\n✅ Ready for Snowflake and dbt!")
print("="*60 + "\n")
