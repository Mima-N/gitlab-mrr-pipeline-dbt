with source as (
    select * from {{ source('zuora', 'raw_zuora_subscription') }}
),

renamed as (
    select
        -- IDs
        subscription_id,
        crm_opportunity_id as sfdc_opportunity_id,  -- Renamed for clarity
        account_id,
        
        -- Attributes
        status as subscription_status,
        currency as currency_code,
        
        -- Financials
        mrr as mrr_amount,
        
        -- Dates
        subscription_start_date::date as subscription_start_date,
        subscription_end_date::date as subscription_end_date,
        
        -- Derived Metrics
        datediff('month', subscription_start_date, subscription_end_date) as actual_term_months,
        
        -- Derived Flags (for filtering and analysis)
        subscription_status = 'Active' as is_active,
        subscription_status = 'Canceled' as is_canceled

    from source
)

select * from renamed
