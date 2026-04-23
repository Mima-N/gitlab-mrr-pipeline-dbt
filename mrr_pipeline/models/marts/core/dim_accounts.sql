-- models/marts/core/dim_accounts.sql
{{
    config(
        materialized='table',
        tags=['dimension', 'core']
    )
}}

with accounts as (
    select * from {{ ref('stg_sfdc_accounts') }}
),

final as (
    select
        -- Primary Key
        account_id,

        -- Attributes
        account_name,
        segment,
        region,

        -- Segment Flags
        segment = 'Large Enterprise'   as is_enterprise,
        segment = 'Mid-Market'         as is_mid_market,
        segment = 'SMB'                as is_smb,

        -- Region Flags
        region = 'AMER'                as is_amer,
        region = 'EMEA'                as is_emea,
        region = 'APAC'                as is_apac,

        -- Metadata
        current_timestamp()            as dbt_updated_at

    from accounts
)

select * from final