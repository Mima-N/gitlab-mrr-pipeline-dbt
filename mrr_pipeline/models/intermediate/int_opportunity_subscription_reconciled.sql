-- models/intermediate/int_opportunity_subscription_reconciled.sql
{{
    config(
        materialized='view',
        tags=['intermediate', 'reconciliation']
    )
}}

with matched_data as (
    select * from {{ ref('int_opportunity_subscription_matched') }}
),

reconciled as (
    select
        *,

        -- Data Quality Flags
        subscription_id is null and is_won = true as is_orphaned_opportunity,
        subscription_start_date > close_date  as has_date_slip,
        mrr_variance_abs > 0.05    as has_mrr_mismatch,
        term_variance_months > 0   as has_term_mismatch,
        is_churned = true
            and subscription_id is not null
            and is_active = true  as is_zombie_subscription,

        -- Reconciliation Status
        case
            when is_churned = true
                and subscription_id is not null
                and is_active = true        then 'Zombie Subscription'
            when subscription_id is null
                and is_won = true           then 'Missing Subscription'
            when mrr_variance_abs > 0.05    then 'MRR Mismatch'
            when subscription_start_date
                > close_date                then 'Date Slip'
            when term_variance_months > 0   then 'Term Mismatch'
            else 'Matched'
        end as reconciliation_status,

        -- Data Quality Severity
        case
            when is_churned = true
                and subscription_id is not null
                and is_active = true        then 'Critical'
            when subscription_id is null
                and is_won = true           then 'Critical'
            when mrr_variance_abs > 100     then 'High'
            when mrr_variance_abs > 10      then 'Medium'
            when subscription_start_date
                > close_date                then 'Medium'
            when mrr_variance_abs > 0.05    then 'Low'
            else 'None'
        end as data_quality_severity,

        -- Financial Impact
        cast(
            case
                when is_churned = true
                    and subscription_id is not null
                    and is_active = true    then mrr_amount * 12
                when subscription_id is null
                    and is_won = true       then crm_expected_mrr * 12
                when mrr_variance_abs > 0.05 then mrr_variance_abs * 12
                else 0
            end
        as numeric(16,2)) as annual_financial_impact

    from matched_data
)

select * from reconciled
