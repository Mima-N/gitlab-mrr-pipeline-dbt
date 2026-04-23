-- tests/assert_mrr_variance_within_tolerance.sql
-- Ensure no MRR variance exceeds $1000 (would indicate major issue)

{{ config(
    severity = 'warn'
) }}

SELECT 
    opportunity_id,
    mrr_variance_abs,
    reconciliation_status
FROM {{ ref('fct_opportunity_subscription_reconciliation') }}
WHERE mrr_variance_abs > 1000
  AND reconciliation_status != 'Zombie Subscription'  -- Zombies expected to be large