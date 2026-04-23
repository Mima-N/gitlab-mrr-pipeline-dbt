{% docs reconciliation_status %}
Outcome of CRM-to-billing reconciliation between Salesforce and Zuora.

| Value                  | Meaning                                      | Severity            |
|------------------------|----------------------------------------------|---------------------|
| `Matched`              | MRR, dates, and term align                   | None                |
| `Missing Subscription` | Closed Won with no Zuora subscription        | Critical            |
| `Zombie Subscription`  | Churned in Salesforce, Active in Zuora       | Critical            |
| `MRR Mismatch`         | Billed MRR differs from expected by > $0.05  | High / Medium / Low |
| `Date Slip`            | Billing started after opportunity close date | Medium              |
| `Term Mismatch`        | Contract term differs between systems        | None                |
{% enddocs %}


{% docs data_quality_severity %}
Priority tier for Revenue Operations remediation.

| Value      | Condition                                 | Action                     |
|------------|-------------------------------------------|----------------------------|
| `Critical` | Zombie or Missing subscription            | Immediate remediation      |
| `High`     | MRR variance > $100                       | Finance review this period |
| `Medium`   | MRR variance $10–$100, or Date Slip       | Next reconciliation cycle  |
| `Low`      | MRR variance $0.05–$10                    | Batch correction           |
| `None`     | Matched or Term Mismatch within tolerance | No action required         |
{% enddocs %}
