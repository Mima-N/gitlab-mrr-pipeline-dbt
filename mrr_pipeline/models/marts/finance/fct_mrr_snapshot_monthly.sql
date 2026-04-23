-- models/mart/finance/fct_mrr_snapshot_monthly.sql
{{
    config(
        materialized='table',
        tags=['finance', 'revenue', 'time_series']
    )
}}

with date_spine_raw as (
    {{
        dbt_utils.date_spine(
            datepart="month",
            start_date="dateadd('month', -" ~ var('mrr_lookback_months') ~ ", date_trunc('month', current_date()))",
            end_date="dateadd('month', 1, date_trunc('month', current_date()))"
        )
    }}
),

-- Wrap in a second CTE to safely alias and cast the column
date_spine as (
    select
        cast(date_month as date) as report_month
    from date_spine_raw
),

reconciled_data as (
    select * from {{ ref('int_opportunity_subscription_reconciled') }}
    where subscription_id is not null
),

mrr_snapshot as (
    select
        -- Surrogate Primary Key
        {{ dbt_utils.generate_surrogate_key([
            'reconciled_data.subscription_id',
            'date_spine.report_month'
        ]) }}  as mrr_snapshot_id,

        -- Time Dimension
        date_spine.report_month,

        -- Keys
        reconciled_data.subscription_id,
        reconciled_data.crm_account_id,
        reconciled_data.opportunity_id,

        -- Context
        reconciled_data.opportunity_type,
        reconciled_data.is_multi_year_contract,
        reconciled_data.contract_term_months,

        -- Revenue Metrics
        reconciled_data.mrr_amount,
        reconciled_data.mrr_amount * 12 as arr_amount,

        -- Status
        reconciled_data.subscription_status,

        -- Data Quality Context
        reconciled_data.reconciliation_status,
        reconciled_data.data_quality_severity,
        reconciled_data.is_zombie_subscription,
        reconciled_data.has_mrr_mismatch,
        reconciled_data.annual_financial_impact,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from date_spine
    inner join reconciled_data
        on date_spine.report_month >= date_trunc('month', reconciled_data.subscription_start_date)
        and date_spine.report_month < date_trunc('month', reconciled_data.subscription_end_date)
)

select * from mrr_snapshot
