-- Monthly MRR trend with clean vs at-risk split
-- Powers revenue dashboard time-series charts

SELECT
    report_month,
    COUNT(DISTINCT subscription_id) as active_subscriptions,
    SUM(mrr_amount) as total_mrr,
    SUM(arr_amount) as total_arr,
    SUM(CASE WHEN reconciliation_status = 'Matched'
        THEN mrr_amount ELSE 0 END) as clean_mrr,
    SUM(CASE WHEN data_quality_severity = 'Critical'
        THEN mrr_amount ELSE 0 END)  as at_risk_mrr
FROM {{ ref('fct_mrr_snapshot_monthly') }}
GROUP BY report_month
ORDER BY report_month DESC