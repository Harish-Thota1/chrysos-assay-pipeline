# Verification

Three checks. All the output below is real, from the dates shown.

```
1. Python, S3 vs the live database        did the pipeline lose anything?
2. Python, S3 vs the generator's log      does an independent record agree?
3. SQL, inside Snowflake                  do the warehouse counts add up?
```

## 1. Did the pipeline lose anything?

`verification/verify_current.py` compares every `(measurement_id, updated_at)`
pair in S3 against the live source. Not row counts: pairs. A count can match
while the contents differ.

Run 2 October, after 31 unattended runs over 10 hours:

```
watermark                     2026-10-02T13:53:03Z
S3 files / rows read             63 /    235,676
distinct ids in S3                 225,698
rows in the source                 223,922
------------------------------------------------------------
ROWS S3 IS RESPONSIBLE FOR         223,746   (updated_at <= watermark)
  CURRENT                          223,746
  BEHIND                                 0   <- must be 0
  MISSING                                0   <- must be 0
  correct                         100.000%
------------------------------------------------------------
NOT DUE YET                            176   (changed after the watermark)
------------------------------------------------------------
STALE in S3                          1,932   <- deleted in the source
S3 that is wrong                    0.856%
```

The split by watermark is what makes this trustworthy. A row changed after the
watermark is not due yet; counting it as missing would report a failure on
every run and the check would stop being read.

## 2. Does it agree with an independent record?

`verification/reconcile.py` compares against the generator's own change log,
written by the thing making the changes, which knows nothing about the
pipeline. Agreement between them is evidence rather than circular.

```
coverage start                2026-10-02T02:28:07Z
coverage end                  2026-10-02T03:20:47Z
generator runs in coverage               4
backdated rows excluded                  1   (stamped before the window opened)
delta files / rows                2 /        787
distinct ids in deltas                 635
duplicate rows from overlap            152   (silver collapses these)
------------------------------------------------------------
expected in coverage                   635
MISSING                                  0   <- must be 0
surplus                                  0
```

The first version of this check reported one missing row. It was not missing:
every generator run inserts one row stamped four minutes in the past, so a run
inside the coverage window can hold a row stamped outside it. The check's
boundary was wrong, not the pipeline. A check that produces false failures is
worse than no check.

## 3. Does the Snowflake side agree?

Run 4 October, after about 185 incremental runs:

```
DW_SOURCE   ROW_COUNT       IDS   OLDEST                  NEWEST
delta         107,320    52,536   2026-10-02 02:30:42     2026-10-04 10:01:13
snapshot      217,184   217,184   2026-07-03 17:56:41     2026-10-02 02:30:42
```

```
bronze_rows   silver_rows   collapsed_by_overlap   source_duplicates
    324,504       266,600                 57,904                 264
```

```
row_count   ids        grain_violations   orphan_machine_keys
  266,600   266,600                   0                     0

crm_missing_cert   non_crm_has_cert
               0                  0
```

Three of those numbers can be worked out in advance, which is how you know the
pipeline is behaving rather than just finishing:

**107,320 / 52,536 = 2.04.** A 15 minute look-back on a 15 minute schedule puts
every row in two consecutive windows, so it arrives roughly twice. Predicted
from the arithmetic before the data existed, measured at 2.04 over 185 runs.

**264 source duplicates.** The generator inserts exactly one duplicate per run
and skips one run in eight. About 300 firings, so about 262 doing work. Those
264 are deliberately kept: they are a fact about the source, not an artifact of
the pipeline.

**217,184 + 52,536 - 266,600 = 3,120.** Rows that existed in the snapshot and
were later updated, so they appear on both sides. The dedup matched them rather
than double counting.

## The gap

```
1,932 stale rows      one hour after the snapshot
0.856% of the table
172 rows per hour     and it never decreases
```

4,100 a day. Four days after a snapshot it is around 7%.

A deleted row is not in any query result, so no `WHERE updated_at > watermark`
can find it. `verification/verify_current.py` detects them by comparing key
sets in both directions. Moving that comparison into the pipeline and writing
delete markers is the work that was not done.
