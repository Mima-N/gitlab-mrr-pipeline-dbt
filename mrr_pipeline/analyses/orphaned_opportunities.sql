-- Orphaned opportunities: won deals with no billing subscription created
SELECT
    COUNT(*) as orphaned_opportunities,
    SUM(crm_expected_mrr) as total_monthly_revenue_loss,
    SUM(annual_financial_impact) as total_annual_revenue_loss,
    ROUND(AVG(crm_expected_mrr), 2) as avg_mrr_per_orphan,
    MIN(close_date) as earliest_orphan_date,
    MAX(close_date) as latest_orphan_date
FROM {{ ref('fct_opportunity_subscription_reconciliation') }}
WHERE reconciliation_status = 'Missing Subscription';