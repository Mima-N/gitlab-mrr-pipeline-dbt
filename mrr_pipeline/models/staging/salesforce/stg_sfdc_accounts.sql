-- models/staging/salesforce/stg_sfdc_accounts.sql
with source as (
    select * from {{ source('salesforce', 'raw_sfdc_account') }}
),

renamed as (
    select
        account_id,
        account_name,
        segment,
        region
    from source
)

select * from renamed