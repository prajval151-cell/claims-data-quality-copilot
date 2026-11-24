# Zero-Row Load

Alert: `zero_row_load` (ETL integrity, critical). A source batch contains payments but the
target fact table received none of them, so the whole batch is absent downstream.

## Symptoms

- The batch audit shows source rows greater than zero while staging rows and target rows are both 0.
- Example: `BATCH-YYYYMMDD` has 1,000 source payments, 0 staged, 0 loaded; the batch total in the target is 0 paise.
- Full-run totals are short by exactly one batch's row count and amount; every other batch reconciles.
- Downstream reports for that business day show no claim payments at all, not just fewer.

## Checks to run

- Confirm the source still holds the batch: count rows and sum paise for the batch ID in the source database.
- Inspect the staging step for the load run: was any row written to staging, and was the staging filter or WHERE clause changed?
- Read the load run record: status, start and finish time, and any staging filter text recorded for the run.
- Check that the batch ID or batch date format matches between source and loader (for example a date-format or time-zone change).
- Re-run the load for only that batch into a scratch target and reconcile it before touching production.

## Possible causes

- A staging filter or predicate that excludes the batch, such as a wrong date range or an inverted condition.
- The loader read an empty or truncated extract file, or picked up the file before it finished arriving.
- Batch key mismatch, so the loader's lookup for the batch returned nothing.
- A transaction that cleared the target batch but failed or was rolled back before inserting new rows.
- Permissions or connection failure on the source that returned an empty result instead of an error.
