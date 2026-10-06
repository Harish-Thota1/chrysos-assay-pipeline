# simulator/

```
change_source.py      a Glue job, every 15 minutes
```

This keeps changing the source database so the pipeline has something to copy.

**It is not part of the pipeline.** It stands in for the lab's own system.

---

## Why it exists

An incremental load copies only what changed. To test that, something has to
change. A table nobody is writing to can be copied correctly by any pipeline,
including a broken one, so it proves nothing.

Deletes in particular cannot be tested without a source that deletes.

## Why it is in its own folder

Chrysos would not own the lab's system and could not modify it. So nothing in
`pipeline/` is allowed to assume anything about this generator beyond one
thing: there is an `updated_at` column and it is maintained.

Keeping the two apart is how that stays true. If the generator and the loader
shared a file, the loader would gradually start relying on behaviour a real
source system does not guarantee, and nobody would notice until it was pointed
at a real database.

---

## What it does each run

```
inserts     ~60 new samples, 2 element rows each, so ~120 rows
updates     15 recent rows and 5 old ones, re-measured
deletes     40 rows, removed outright
duplicates  1 deliberate repeat of a (sample_id, element) pair
backdates   1 row stamped 4 minutes in the past
```

Then it writes a JSON file to `s3://<bucket>/change_log/` listing every id it
touched and what happened to it.

### The change log

This is what `verification/reconcile.py` checks against. It is written by the
thing making the changes, so it is a second, independent record of the same
events. When the pipeline's output and this log agree, that is two systems
writing down the same thing rather than one system agreeing with itself.

### The deliberate duplicate

One run in every batch inserts the same `(sample_id, element)` pair twice.

Real labs do this: a sample gets re-run and both results are recorded. It is a
data quality problem worth reporting, not a pipeline fault worth hiding.

It is why `05_silver.sql` de-duplicates on `measurement_id`, the primary key,
and not on `(sample_id, element)`. Deduplicating on the pair would silently
delete one row of every real duplicate. Instead `silver.source_duplicate`
counts them and leaves them alone.

### The backdated row

One row per run is stamped four minutes before the run happens.

This exists to test the verification, not the pipeline. The first version of
`reconcile.py` reported this row as missing, because a generator run inside the
coverage window held a row stamped outside it. The check's boundary was wrong.
`reconcile.py` now takes `--lag-minutes` to account for it.

---

## The six faults it injects

All six pass the `CHECK` constraints on the source table. That is the point:
constraints catch what someone thought of at design time, and anything else
walks straight through into the warehouse.

```
null_value       the detector read, the calculation failed
negative_value   background subtraction overshot
absurd_value     50,000 ppm. a decimal place, or a calibration fault
customer_dirty   stray whitespace or case. free-text field, two operators
future_started   started_utc in the future. a lab PC with its clock wrong
zero_duration    0 seconds. an aborted measurement that still wrote a row
```

### The faults are clustered, not spread

```python
SICK_MACHINES = {
    7:  "null_value",       # detector on the way out
    23: "absurd_value",     # calibration drifting
    41: "zero_duration",    # robot aborting mid cycle
}
SICK_RATE       = 0.35     # how often a sick machine produces its fault
BACKGROUND_RATE = 0.004    # everything else, occasionally, from anywhere
```

Three machines carry one signature fault each, at 35%. The other 57 get a 0.4%
background rate of anything.

This matters because real data quality problems have a cause, and the cause
sticks to one machine. A failing detector produces many nulls, on one unit, for
days. If the faults were spread evenly across all 60 machines, "which machine
is producing bad data" would have no answer, and the pattern would be noise
rather than a signal.

The machine numbers are fixed in code rather than random, so the same units
misbehave run after run and a trend is visible over days.

---

## Running it

Normally it runs as a Glue job on a 15 minute trigger, which produces 96 runs a
day with nothing attached. `docs/how-to-run.md` phase 5.

For one run by hand:

```
source db.env
python3 simulator/change_source.py --seed 42
```

```
--seed 42      fix the randomness, so the run is repeatable
--clean        inject no faults at all
--purge 0      insert and update, delete nothing
```

`--clean` is useful when testing the pipeline itself: it removes the data
quality noise so any problem you see is the pipeline's.
