-- models/marts/finance/fct_zombie_subscriptions.sql
{{
    config(
        materialized='table',
        tags=['finance', 'data_quality', 'alerting']
    )
}}

with zombies as (
    select * from {{ ref('int_opportunity_subscription_reconciled') }}
    where is_zombie_subscription = true  -- flag comes from intermediate now
),

final as (
    select
        -- Keys
        opportunity_id,
        subscription_id,
        crm_account_id,

        -- Context
        close_date as churned_date,
        subscription_start_date,
        subscription_end_date,
        opportunity_type,
        total_contract_value,
        
        -- Financial Impact (already calculated in intermediate)
        mrr_amount as monthly_revenue_leakage,
        annual_financial_impact as annual_revenue_at_risk,
        
        -- Duration Metrics
        datediff('day', close_date, current_date()) as days_billing_after_churn,
        
        -- Total Leakage Since Churn
        cast(
            mrr_amount * (datediff('day', close_date, current_date()) / 30.0)
        as numeric(16,2)) as total_revenue_leaked,
        
        -- Metadata
        current_timestamp() as dbt_updated_at

    from zombies
)

select * from final