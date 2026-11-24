# Missing or Stale Batches

Alerts: `missing_expected_batch` (completeness, high) and `stale_data` (freshness). A batch that
the schedule says should have arrived by the as-of date is absent, and the newest loaded data is
older than expected.

## Symptoms

- One or more scheduled batch IDs are not present in the source batch list or were never loaded.
- Example: the schedule expects a daily batch, the as-of date is day N, but the latest loaded batch is from day N-2.
- The latest batch date in the target lags the expected latest date by one or more days.
- Dashboards show flat or missing figures for the most recent business days.

## Checks to run

- List expected batch IDs for the as-of date from the batch schedule and compare with batches received and loaded.
- Check whether the upstream system produced the extract: file landing zone, transfer logs, and source batch tables.
- Check the scheduler for the load job: did it run, fail, or get skipped, and when did it last succeed?
- Confirm the as-of date, time zone, and schedule settings (frequency, arrival lag, grace period) are correct.
- After the batch arrives, load it and re-run reconciliation and the freshness check.

## Possible causes

- Upstream extract delayed or not produced (holiday calendar, source outage, end-of-month processing).
- File transfer failure or a file delivered to the wrong location or with an unexpected name.
- Load scheduler paused, failed, or misconfigured after a change.
- Schedule configuration that expects batches the business does not produce (for example weekend days).
- Clock or time-zone mismatch that makes a batch look late.
