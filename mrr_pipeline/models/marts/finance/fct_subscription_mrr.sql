-- models/marts/finance/fct_subscription_mrr.sql
{{
    config(
        materialized='table',
        tags=['finance', 'revenue', 'core']
    )
}}

with reconciled_data as (
    select * from {{ ref('int_opportunity_subscription_reconciled') }}
),

final as (
    select
        -- Keys
        subscription_id,
        opportunity_id,
        crm_account_id,

        -- Dates
        subscription_start_date,
        subscription_end_date,
        close_date,

        -- Revenue Metrics
        mrr_amount,
        mrr_amount * 12 as arr_amount,
        total_contract_value,
        contract_term_months,

        -- Status
        subscription_status,
        is_active,
        is_canceled,

        -- Context
        opportunity_type,
        is_multi_year_contract,
        stage_name,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from reconciled_data
    where subscription_id is not null  -- Only subscriptions that exist (no orphans)
)

select * from final