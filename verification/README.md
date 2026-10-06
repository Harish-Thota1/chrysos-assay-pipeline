# verification/

Two scripts that check the pipeline did not lose anything.

```
verify_current.py     compares S3 against the live database
reconcile.py          compares S3 against the generator's own log
```

Both are read only. Both run on a laptop, not in AWS.

A pipeline that runs is not the same as a pipeline that works. "96 jobs
succeeded" says nothing about whether any rows went missing. These two are what
turns the first into the second.

---

## verify_current.py

```
source db.env
python3 verification/verify_current.py --bucket <BUCKET>
```

It reads every Parquet file in S3, reads the live database, and compares them.

### What it compares, and why that choice matters

It compares pairs of `(measurement_id, updated_at)`.

**Not row counts.** Two counts can match while the contents differ. 223,746 on
both sides proves nothing if one side has row 500 and the other has row 900.
Comparing the sets names the exact id that is wrong.

**Not just ids.** An id tells you the row arrived. It does not tell you S3 has
the *current* version of it. If a row was updated in the source and the
pipeline never picked up the change, the id is present on both sides and
everything looks fine. The `updated_at` half is what catches that.

### What it reports

```
rows S3 is responsible for    223,746      updated_at <= watermark
  CURRENT                     223,746
  BEHIND                            0      <- must be 0
  MISSING                           0      <- must be 0
  correct                      100.000%
NOT DUE YET                       176      changed after the watermark
STALE in S3                     1,932      deleted in the source
```

```
CURRENT       S3 has the row, with the same updated_at as the database
BEHIND        S3 has the row, with an older updated_at. a missed update.
MISSING       S3 does not have the row at all
NOT DUE YET   the row changed after the watermark. the pipeline has not
              been asked for it yet. not a failure.
STALE         S3 has a row the database no longer has. a delete.
```

### Why it splits by watermark

This is the part that makes the number trustworthy.

The watermark says how far the pipeline has read. Rows that changed after it
have not been asked for yet. If those counted as `MISSING`, the check would
report a failure on every single run, because the generator is always writing
something.

A check that fails every time stops being read, and then it is worse than no
check at all. So rows past the watermark are reported separately as `NOT DUE
YET`, and `MISSING` only counts rows the pipeline has actually had the chance
to collect.

### STALE is the delete gap

`STALE` is the one number here that is not zero and is not supposed to be.

A deleted row is gone from the source, so no query can find it. The copy in S3
stays. This script finds them only because it holds the full key set from both
sides and can subtract in either direction:

```
every key in the source      {1, 2, 3, 5}
every key in S3              {1, 2, 3, 4, 5}
in S3 but not the source     {4}            <- deleted
```

The main README, section 4, has the measured growth rate.

---

## reconcile.py

```
python3 verification/reconcile.py --bucket <BUCKET>
```

This one does not look at the database at all. It compares S3 against a log the
generator writes.

Every time the generator changes the source, it writes a JSON file to
`s3://<bucket>/change_log/` listing every id it touched and what it did:

```json
{"measurement_id": 218431, "machine_id": 7, "action": "update"}
```

That log is written by the thing making the changes. It knows nothing about the
pipeline.

### Why a second check at all

Because the first one is the pipeline being compared to the database it reads
from. This one is two independent records of the same events lining up.

If the pipeline and the generator's log agree on 635 rows, two separate systems
wrote down the same thing. That is evidence. The pipeline checking itself would
not be.

### What it reports

```
coverage start                2026-10-02T02:28:07Z
coverage end                  2026-10-02T03:20:47Z
generator runs in coverage               4
backdated rows excluded                  1
delta files / rows                2 /        787
distinct ids in deltas                 635
duplicate rows from overlap            152
expected in coverage                   635
MISSING                                  0   <- must be 0
surplus                                  0
```

### Why there is a --lag-minutes flag

Because of a bug in this script, not in the pipeline.

The first version reported one missing row. It was not missing. Every generator
run inserts one row stamped four minutes in the past, on purpose, to test
exactly this. So a generator run inside the coverage window can hold a row
stamped outside it.

The window was wrong, not the data. `--lag-minutes` (default 4) excludes those
rows and prints how many it excluded, so the exclusion is visible rather than
hidden.

A check that produces false failures is worse than no check, which is the same
reason `verify_current.py` splits by watermark.

---

## What neither of them checks

Neither script can find a deleted row by looking at the pipeline's output
alone, because nothing in the output refers to a row that is not there.

`verify_current.py` finds them only because it loads the complete key set from
both sides. That needs a full scan of the source table, which is why delete
detection in the pipeline would run once a day rather than every 15 minutes.

---

`docs/verification.md` has the full output of both, from real runs, with the
arithmetic that predicts the numbers.
