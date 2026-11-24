# Partial Load

Alert: `row_count_mismatch` (ETL integrity, high). Some, but not all, payments of a batch reached
the target. The row count in staging or target is lower (or higher) than the source count.

## Symptoms

- Target row count for a batch differs from the source row count, but is not zero.
- Example: source 1,000 rows, staging 980, target 980 for `BATCH-YYYYMMDD`; a matching amount gap usually appears too.
- Payment IDs present in the source are missing from the target (source-to-target differences), or extra IDs appear in the target.
- Other batches in the same run reconcile normally.

## Checks to run

- List the payment IDs missing from the target for the batch and look for a pattern: last rows of the file, one payment method, one payee type.
- Compare source, staging, and target counts to find which step lost rows (extract to staging, or staging to target).
- Look for rejected or quarantined rows, constraint violations, or type-conversion errors in the load logs.
- Check whether the source extract was read while still being written (file size or row count changed afterwards).
- Check for duplicate payment IDs: duplicates can be collapsed by a primary key and reduce the target count.

## Possible causes

- Truncated extract file or interrupted network transfer that cut off the tail of the batch.
- Rows rejected by validation, NOT NULL or CHECK constraints, or failed date and number parsing.
- A filter that is too narrow, dropping a subset such as one payment method or a late time window.
- Paging or chunking bug in the extractor that skips the last chunk.
- De-duplication on a non-unique key that silently removes legitimate payments.
