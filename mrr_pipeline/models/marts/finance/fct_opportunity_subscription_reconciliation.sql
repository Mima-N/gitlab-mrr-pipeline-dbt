-- models/marts/finance/fct_opportunity_subscription_reconciliation.sql
{{
    config(
        materialized='table',
        tags=['finance', 'reconciliation', 'core']
    )
}}

with reconciled_data as (
    select * from {{ ref('int_opportunity_subscription_reconciled') }}
),

final as (
    select
        -- Surrogate Primary Key
        {{ dbt_utils.generate_surrogate_key(['opportunity_id']) }} as reconciliation_id,

        -- Natural Keys
        opportunity_id,
        subscription_id,
        crm_account_id,

        -- Dates
        close_date,
        subscription_start_date,
        subscription_end_date,

        -- Attributes
        stage_name,
        opportunity_type,
        subscription_status,
        is_multi_year_contract,
        is_won,
        is_churned,
        is_active,
        is_canceled,

        -- Financial Metrics
        total_contract_value,
        contract_term_months,
        crm_expected_mrr,
        mrr_amount,
        mrr_variance,
        mrr_variance_abs,

        -- Data Quality Metrics
        days_to_billing_start,
        term_variance_months,
        annual_financial_impact,

        -- Flags
        is_orphaned_opportunity,
        has_date_slip,
        has_mrr_mismatch,
        has_term_mismatch,
        is_zombie_subscription,

        -- Status
        reconciliation_status,
        data_quality_severity,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from reconciled_data
)

select * from final