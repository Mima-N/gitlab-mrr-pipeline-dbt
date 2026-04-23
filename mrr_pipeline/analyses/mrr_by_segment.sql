-- Current month MRR by customer segment and region
-- Powers revenue breakdown charts

SELECT
    a.segment,
    a.region,
    COUNT(DISTINCT m.subscription_id) as active_subscriptions,
    SUM(m.mrr_amount) as total_mrr,
    SUM(m.arr_amount) as total_arr
FROM {{ ref('fct_mrr_snapshot_monthly') }} m
JOIN {{ ref('dim_accounts') }} a
    ON m.crm_account_id = a.account_id
WHERE m.report_month = DATE_TRUNC('month', CURRENT_DATE())
GROUP BY 1, 2
ORDER BY total_mrr DESC