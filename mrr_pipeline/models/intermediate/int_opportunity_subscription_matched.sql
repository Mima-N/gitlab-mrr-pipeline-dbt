-- models/intermediate/int_opportunity_subscription_matched.sql
{{
    config(
        materialized='view',
        tags=['intermediate', 'reconciliation']
    )
}}

with opportunities as (
    select * from {{ ref('stg_sfdc_opportunities') }}
),

subscriptions as (
    select * from {{ ref('stg_zuora_subscriptions') }}
),

joined as (
    select
        -- Keys
        opportunities.opportunity_id,
        opportunities.crm_account_id,
        subscriptions.subscription_id,

        -- Opportunity Attributes
        opportunities.stage_name,
        opportunities.opportunity_type,
        opportunities.close_date,
        opportunities.total_contract_value,
        opportunities.contract_term_months,
        opportunities.is_multi_year_contract,
        opportunities.is_won,
        opportunities.is_churned,

        -- Subscription Attributes  
        subscriptions.subscription_status,
        subscriptions.subscription_start_date,
        subscriptions.subscription_end_date,
        subscriptions.mrr_amount,
        subscriptions.actual_term_months,
        subscriptions.is_active,
        subscriptions.is_canceled,

        -- Calculated Metrics with Macro
        {{ calculate_mrr('opportunities.total_contract_value', 'opportunities.contract_term_months') }} as crm_expected_mrr,
        
        -- Variance
        (
            {{ calculate_mrr('opportunities.total_contract_value', 'opportunities.contract_term_months') }} 
            - coalesce(subscriptions.mrr_amount, 0)
        ) as mrr_variance,
        
        -- Absolute variance
        abs(
            {{ calculate_mrr('opportunities.total_contract_value', 'opportunities.contract_term_months') }} 
            - coalesce(subscriptions.mrr_amount, 0)
        ) as mrr_variance_abs,

        -- Date Metrics (NULL for orphaned opportunities - appropriate)
        datediff('day', opportunities.close_date, subscriptions.subscription_start_date) as days_to_billing_start,
        
        -- Term Validation (NULL for orphaned opportunities)
        abs(opportunities.contract_term_months - coalesce(subscriptions.actual_term_months, opportunities.contract_term_months)) as term_variance_months

    from opportunities
    left join subscriptions 
        on opportunities.opportunity_id = subscriptions.sfdc_opportunity_id
)

select * from joined