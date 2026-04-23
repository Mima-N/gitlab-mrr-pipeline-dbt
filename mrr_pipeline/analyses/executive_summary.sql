-- analyses/executive_summary.sql
-- Reconciliation health summary for executive dashboard
-- Shows breakdown of all issues by status and severity with financial impact

SELECT 
    reconciliation_status,
    data_quality_severity,
    COUNT(*) as issue_count,
    SUM(annual_financial_impact) as total_financial_impact
    ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (), 2) as pct_of_total
FROM {{ ref('fct_opportunity_subscription_reconciliation') }}
GROUP BY 1, 2
ORDER BY total_annual_impact DESC;
