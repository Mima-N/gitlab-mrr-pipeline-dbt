with source as (
    select * from {{ source('salesforce', 'raw_sfdc_opportunity') }}
),

renamed as (
    select
        -- IDs
        opportunity_id,
        account_id as crm_account_id,
        
        -- Attributes
        stage_name, 
        type as opportunity_type,
        
        -- Financials
        amount::numeric(16, 2) as total_contract_value,  -- TCV (not ARR for multi-year deals)
        contract_term_months::int as contract_term_months,
        
        -- Dates
        close_date::date as close_date,
        
        -- Derived Flags (for filtering and analysis)
        contract_term_months > 12 as is_multi_year_contract,
        stage_name = 'Closed Won' as is_won,
        stage_name = 'Closed Lost - Churn' as is_churned

    from source
)

select * from renamed
