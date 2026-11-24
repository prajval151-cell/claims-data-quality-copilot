# Unusual Payment Totals

Alert: `unusual_payment_total` (business anomaly). A batch reconciles cleanly, so source and target
agree, but its total paid amount is far above (spike) or below (drop) the baseline of recent clean
batches. This is a business or operational question, not a pipeline defect.

## Symptoms

- Batch total in paise is several times the baseline median of prior clean batches, or far below it.
- Example: a typical daily total of about 5,000,000,000 paise, then one day at 15,000,000,000 paise.
- Row counts are normal and reconciliation passes in both directions: no missing, extra, or changed rows.
- A high robust z-score and large relative deviation from the baseline median.

## Checks to run

- Confirm reconciliation passed for the batch; if not, follow the ETL runbooks first.
- Rank the batch's payments by amount and check whether a few large payments or a broad increase drive the total.
- Group the batch by claim, policy product, payee type, and payment method to find where the increase sits.
- Look for legitimate events: catastrophe or weather claims, bulk settlements, backlog clearance, or a new large customer.
- Check for business-process risks: duplicate payouts across batches, payments above approved claim amounts, or unusual payees.

## Possible causes

- Genuine surge in claims, such as a natural catastrophe or seasonal peak.
- Bulk settlement or backlog release processed on a single day.
- Upstream data error at the source system, such as amounts multiplied by a unit factor before extraction.
- Duplicate or fraudulent payments that are valid at the data level but wrong for the business.
- Baseline too short or unrepresentative, such as a holiday period.
