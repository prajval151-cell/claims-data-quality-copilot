# Amount Mismatch

Alert: `amount_mismatch` (ETL integrity, high). The total paid amount in integer paise for a batch
differs between source and target. If row counts also differ, start with the Partial Load runbook.

## Symptoms

- Batch paise total in staging or target differs from the source total.
- Example: source 50,000,000 paise, target 50,000,100 paise for `BATCH-YYYYMMDD`, with identical row counts (a value changed, no rows lost).
- Column-level reconciliation shows mismatches in `amount_paise` for specific payment IDs.
- The difference can be tiny (a few paise) or a round multiple such as 100x.

## Checks to run

- Reconcile the batch row by row and list payment IDs whose `amount_paise` differs between source and target.
- Compute the difference for each mismatched row: a factor of 100 suggests a rupee-to-paise conversion error; small differences suggest rounding.
- Confirm the amount column type is an integer (BIGINT) at every step, never float or decimal with rounding.
- Review transformation code between staging and target for currency conversion, rounding, or fee and tax adjustments.
- If row counts also differ, check for missing or duplicated payments before investigating values.

## Possible causes

- Floating-point conversion or rounding of amounts instead of exact integer paise.
- Unit confusion between rupees and paise, applied to some rows or one code path.
- A transformation or enrichment step that adjusts amounts (fees, tax, reversals) without an audit trail.
- Manual correction or update in staging that was not applied to the source of record.
- Missing or duplicated rows, which also change the total (see Partial Load).
