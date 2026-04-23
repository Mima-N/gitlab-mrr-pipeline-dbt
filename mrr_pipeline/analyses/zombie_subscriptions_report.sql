-- Zombie subscription leakage report for RevOps daily alert
-- Shows all active zombies sorted by revenue at risk

SELECT
    subscription_id,
    crm_account_id,
    churned_date,
    monthly_revenue_leakage,
    annual_revenue_at_risk,
    days_billing_after_churn,
    total_revenue_leaked
FROM {{ ref('fct_zombie_subscriptions') }}
ORDER BY annual_revenue_at_risk DESC