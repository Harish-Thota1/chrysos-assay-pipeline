#!/usr/bin/env python3
"""
Chrysos POC  ·  the source system, misbehaving.

Stands in for a customer lab going about its day: samples arrive, results
get corrected, jars get voided, old rows get purged, and a small amount of
rubbish comes out of machines that are not well.

ONE FILE, TWO HOMES.

    locally      source db.env && python3 scripts/change_source.py
    in Glue      a Python Shell job, with  --connection chrysos-source-connection

In Glue it reads the database credentials out of the Glue Connection, so
the password exists in exactly one place and never in this file.

Every run writes a ground-truth JSON naming precisely which rows it
inserted, updated, deleted and dirtied. That file is how the pipeline gets
scored later: not "the counts look about right" but "row 24646 should have
changed, did it".
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone

try:
    import psycopg2
    import psycopg2.extras
except ImportError:                                    # pragma: no cover
    sys.exit("psycopg2 is not installed.  pip install psycopg2-binary")


# ======================================================================
#  VOLUME
#
#  Real arrival rates are not a constant. Two cases matter, and neither
#  is "sometimes 55 instead of 60":
#
#    a QUIET run, where nothing changed at all. This is where watermark
#    bugs live, because a naive pipeline advances its watermark on an
#    empty read and silently skips anything that was in flight.
#
#    a BURST, where a backlog clears at once. This is what makes runs
#    overlap, and Glue jobs have a concurrency limit of 1.
# ======================================================================
def draw_volume(rng: random.Random, base: int) -> tuple[int, str]:
    roll = rng.random()
    if roll < 0.125:                                   # 1 in 8
        return 0, "quiet"
    if roll > 0.95:                                    # 1 in 20
        return rng.randint(base * 8, base * 33), "burst"
    return rng.randint(int(base * 0.65), int(base * 1.5)), "normal"


# ======================================================================
#  DEFECTS
#
#  Every one of these passes the CHECK constraints on the table. That is
#  the point: constraints catch what you thought of at design time, and
#  everything else walks through and lands in your pipeline.
#
#  And they are CLUSTERED, not sprayed. Real data quality problems have
#  a cause, and the cause sticks to one machine. A failing detector
#  produces many nulls, on one unit, for days. If defects were spread
#  evenly across all 60 machines, "which machine is producing bad data"
#  would have no answer, and that question is the entire premise of the
#  POC.
# ======================================================================
DEFECTS = {
    "null_value":     "value_ppm is NULL. the detector read, the calculation failed.",
    "negative_value": "value_ppm is negative. background subtraction overshot.",
    "absurd_value":   "value_ppm is 50,000. a decimal place wrong, or a calibration fault.",
    "customer_dirty": "customer has stray whitespace or case. free-text field, two operators.",
    "future_started": "started_utc is in the future. a lab PC with its clock wrong.",
    "zero_duration":  "measure_seconds is 0. an aborted measurement that still wrote a row.",
}

# machine_id  ->  its signature fault.  Chosen once, deterministically,
# so the same units misbehave run after run and a trend is visible.
SICK_MACHINES = {
    7:  "null_value",       # detector on the way out
    23: "absurd_value",     # calibration drifting
    41: "zero_duration",    # robot aborting mid cycle
}
SICK_RATE       = 0.35     # how often a sick machine produces its fault
BACKGROUND_RATE = 0.004    # everything else, occasionally, from anywhere


def apply_defect(row: dict, kind: str, rng: random.Random) -> dict:
    if kind == "null_value":
        row["value_ppm"] = None
        row["uncertainty_ppm"] = None
    elif kind == "negative_value":
        row["value_ppm"] = -round(rng.uniform(0.01, 0.4), 4)
    elif kind == "absurd_value":
        row["value_ppm"] = 50000.0
        row["uncertainty_ppm"] = 0.0001
    elif kind == "customer_dirty":
        row["customer"] = rng.choice([f"{row['customer']} ",
                                      f" {row['customer']}",
                                      row["customer"].lower()])
    elif kind == "future_started":
        row["started_utc"] += timedelta(days=rng.randint(1, 3))
    elif kind == "zero_duration":
        row["measure_seconds"] = 0
    return row


def choose_defect(machine_id: int, rng: random.Random) -> str | None:
    signature = SICK_MACHINES.get(machine_id)
    if signature and rng.random() < SICK_RATE:
        return signature
    if rng.random() < BACKGROUND_RATE:
        return rng.choice(list(DEFECTS))
    return None


# ======================================================================
#  the physics, same shapes as the seed
# ======================================================================
def lognormal_ppm(element: str, rng: random.Random) -> float:
    base = math.exp((sum(rng.random() for _ in range(4)) - 2) * 2.0 - 0.7)
    return round(base * (18 if element == "silver" else 1), 4)


def uncertainty(value: float) -> float:
    return round(0.05 * math.sqrt(value) + 0.005, 4)


def below_lod(element: str, value: float) -> bool:
    return value < (1.5 if element == "silver" else 0.02)


COLS = ("sample_id", "machine_id", "customer", "element", "value_ppm",
        "uncertainty_ppm", "measure_seconds", "assay_mode", "sample_type",
        "crm_code", "crm_certified_ppm", "below_detection", "started_utc")


# ======================================================================
#  the six changes
# ======================================================================
def new_samples(cur, n: int, rng: random.Random) -> tuple[list[int], list[dict]]:
    """(1) Samples arrive."""
    if n == 0:
        return [], []
    cur.execute("SELECT machine_id, customer FROM machine ORDER BY machine_id")
    fleet = cur.fetchall()
    cur.execute("SELECT coalesce(max(measurement_id), 0) FROM assay_measurement")
    hint = cur.fetchone()[0]

    now, rows, planned = datetime.now(timezone.utc), [], []
    for i in range(n):
        machine_id, customer = rng.choice(fleet)
        sample_id = f"SMP-NEW-{hint + i + 1:07d}"
        started = now - timedelta(seconds=rng.randint(60, 3000))
        for element in ("gold", "silver"):
            v = lognormal_ppm(element, rng)
            row = dict(sample_id=sample_id, machine_id=machine_id,
                       customer=customer, element=element, value_ppm=v,
                       uncertainty_ppm=uncertainty(v),
                       measure_seconds=rng.randint(88, 96),
                       assay_mode="gold_tuned", sample_type="customer",
                       crm_code=None, crm_certified_ppm=None,
                       below_detection=below_lod(element, v),
                       started_utc=started)
            defect = choose_defect(machine_id, rng)
            if defect:
                row = apply_defect(row, defect, rng)
            rows.append(tuple(row[c] for c in COLS))
            planned.append((machine_id, defect))

    # fetch=True is load bearing. execute_values sends in pages, and a
    # plain fetchall() afterwards returns only the LAST page, so any run
    # over one page would silently under-record what it inserted.
    returned = psycopg2.extras.execute_values(
        cur,
        f"INSERT INTO assay_measurement ({','.join(COLS)}) VALUES %s "
        f"RETURNING measurement_id",
        rows, page_size=500, fetch=True)
    ids = [r[0] for r in returned]
    assert len(ids) == len(rows), f"recorded {len(ids)} ids for {len(rows)} rows" 

    defects = [{"measurement_id": i, "machine_id": m, "defect": d,
                "meaning": DEFECTS[d]}
               for i, (m, d) in zip(ids, planned) if d]
    return ids, defects


def duplicate_key(cur) -> list[dict]:
    """(2) The same sample and element recorded twice.

    A jar re-keyed after a correction with both rows kept. There is no
    unique constraint on (sample_id, element), so nothing stops it, and
    every aggregate downstream double counts that sample.
    """
    cur.execute(
        f"""INSERT INTO assay_measurement ({','.join(COLS)})
            SELECT {','.join(COLS)} FROM assay_measurement
             WHERE sample_type = 'customer' AND value_ppm IS NOT NULL
             ORDER BY random() LIMIT 1
         RETURNING measurement_id, machine_id, sample_id, element""")
    r = cur.fetchone()
    if not r:
        return []
    return [{"measurement_id": r[0], "machine_id": r[1],
             "defect": "duplicate_key",
             "meaning": f"second row for sample {r[2]} / {r[3]}. no unique "
                        f"constraint exists, so aggregates double count it."}]


def recalculate_recent(cur, n: int) -> list[int]:
    """(3) A result recalculated within the last few days."""
    cur.execute(
        """UPDATE assay_measurement AS a
              SET value_ppm       = round(a.value_ppm * 1.04, 4),
                  uncertainty_ppm = round(a.uncertainty_ppm * 1.10, 4)
            WHERE a.measurement_id IN (
                    SELECT measurement_id FROM assay_measurement
                     WHERE started_utc > now() - interval '7 days'
                       AND sample_type = 'customer' AND value_ppm > 0
                     ORDER BY random() LIMIT %s)
          RETURNING a.measurement_id""", (n,))
    return [r[0] for r in cur.fetchall()]


def recalculate_old(cur, n: int) -> list[int]:
    """(4) THE LATE-ARRIVING CORRECTION.

    A measurement from two months ago, corrected today. started_utc is
    still two months old; only updated_at moves. Watermark on the wrong
    column and this correction never reaches you.
    """
    cur.execute(
        """UPDATE assay_measurement AS a
              SET value_ppm       = round(a.value_ppm * 0.88, 4),
                  uncertainty_ppm = round(a.uncertainty_ppm * 1.25, 4)
            WHERE a.measurement_id IN (
                    SELECT measurement_id FROM assay_measurement
                     WHERE started_utc < now() - interval '60 days'
                       AND sample_type = 'customer' AND value_ppm > 0
                     ORDER BY random() LIMIT %s)
          RETURNING a.measurement_id""", (n,))
    return [r[0] for r in cur.fetchall()]


def void_sample(cur) -> list[int]:
    """(5) A jar keyed against the wrong sample id. The lab voids it.

    Both element rows go. No flag, no tombstone. They stop existing, and
    a watermark query can never see that.
    """
    cur.execute(
        """DELETE FROM assay_measurement
            WHERE sample_id = (SELECT sample_id FROM assay_measurement
                                WHERE sample_type = 'customer'
                                ORDER BY random() LIMIT 1)
          RETURNING measurement_id""")
    return [r[0] for r in cur.fetchall()]


def retention_purge(cur, n: int) -> list[int]:
    """(6) A retention job removing the oldest rows in one statement."""
    if n <= 0:
        return []
    cur.execute(
        """DELETE FROM assay_measurement
            WHERE measurement_id IN (SELECT measurement_id
                                       FROM assay_measurement
                                      ORDER BY started_utc ASC LIMIT %s)
          RETURNING measurement_id""", (n,))
    return [r[0] for r in cur.fetchall()]


def backdated_insert(cur, lag_minutes: int, rng: random.Random) -> dict:
    """(7) THE COMMIT-LAG TRAP.

    A row visible now but carrying an updated_at from minutes ago, as a
    transaction that opened before your read and committed after it
    would. PostgreSQL's now() returns TRANSACTION START time, so a five
    minute batch stamps every row with the moment it began.

    A load reading strictly after its saved watermark never sees this
    row. Not late. Never. The fix is a look-back window, which only
    works if writing a row twice is harmless.
    """
    stamp = datetime.now(timezone.utc) - timedelta(minutes=lag_minutes)
    cur.execute("SELECT machine_id, customer FROM machine ORDER BY random() LIMIT 1")
    machine_id, customer = cur.fetchone()
    v = lognormal_ppm("gold", rng)
    cur.execute(
        """INSERT INTO assay_measurement
             (sample_id, machine_id, customer, element, value_ppm,
              uncertainty_ppm, measure_seconds, assay_mode, sample_type,
              below_detection, started_utc, created_at, updated_at)
           VALUES (%s,%s,%s,'gold',%s,%s,92,'gold_tuned','customer',%s,%s,%s,%s)
        RETURNING measurement_id""",
        (f"SMP-LAGGED-{int(stamp.timestamp())}", machine_id, customer, v,
         uncertainty(v), below_lod("gold", v), stamp, stamp, stamp))
    return {"measurement_id": cur.fetchone()[0],
            "updated_at": stamp.isoformat(), "lag_minutes": lag_minutes}


# ======================================================================
#  where the credentials come from
# ======================================================================
def connect(args):
    """Three roads in, tried in order.

    1. EXPLICIT PARAMETERS (--pghost etc).  What a Glue job uses.

       A Glue job attached to a VPC has NO ROUTE TO THE INTERNET, and the
       Glue API (glue.<region>.amazonaws.com) is on the internet. So a job
       inside the VPC cannot ask Glue for a connection's password: the call
       hangs until the job times out, with no error. The S3 gateway endpoint
       covers S3 and nothing else.

       Reaching the Glue API from inside a VPC needs an INTERFACE endpoint,
       about $7.30/month. For a POC that is most of the budget for one API
       call, so the details come in as job parameters instead.

       The trade, stated honestly: the password then lives in the job's
       configuration. At production scale you would buy the interface
       endpoint and read it from Secrets Manager, so no password sits in a
       job definition at all.

    2. THE GLUE CONNECTION (--connection).  Works from anywhere WITH
       internet, which means your laptop, not a VPC-attached job.

    3. THE PG* ENVIRONMENT VARIABLES.  Local use, same as psql.
    """
    if args.pghost:
        return psycopg2.connect(host=args.pghost, port=int(args.pgport),
                                dbname=args.pgdatabase, user=args.pguser,
                                password=args.pgpassword), args.pghost

    if args.connection:
        import boto3
        props = (boto3.client("glue")
                 .get_connection(Name=args.connection, HidePassword=False)
                 ["Connection"]["ConnectionProperties"])
        url = props["JDBC_CONNECTION_URL"]             # jdbc:postgresql://host:5432/db
        host_port, _, dbname = url.split("//", 1)[1].partition("/")
        host, _, port = host_port.partition(":")
        return psycopg2.connect(host=host, port=int(port or 5432), dbname=dbname,
                                user=props["USERNAME"],
                                password=props["PASSWORD"]), host

    if not os.environ.get("PGHOST"):
        sys.exit("No connection details. Pass --pghost..., or --connection, "
                 "or run 'source db.env'.")
    return psycopg2.connect(""), os.environ["PGHOST"]


def write_truth(truth: dict, bucket: str | None, prefix: str) -> str:
    stamp = truth["run_at"].replace(":", "").replace("-", "")[:15] + "Z"
    body = json.dumps(truth, indent=2)
    if bucket:
        import boto3
        key = f"{prefix.rstrip('/')}/{stamp}.json"
        boto3.client("s3").put_object(Bucket=bucket, Key=key,
                                      Body=body.encode(),
                                      ContentType="application/json")
        return f"s3://{bucket}/{key}"
    from pathlib import Path
    out = Path("out/change_log"); out.mkdir(parents=True, exist_ok=True)
    path = out / f"{stamp}.json"; path.write_text(body)
    return str(path)


# ======================================================================
def main() -> int:
    p = argparse.ArgumentParser(description="Make the source system change.")
    p.add_argument("--connection",    help="Glue connection name. needs internet, so not from a VPC job")
    p.add_argument("--pghost",        help="database host. what a VPC-attached Glue job uses")
    p.add_argument("--pgport",        default="5432")
    p.add_argument("--pgdatabase",    default="chrysos")
    p.add_argument("--pguser",        default="postgres")
    p.add_argument("--pgpassword")
    p.add_argument("--s3-bucket",     help="write ground truth here instead of ./out")
    p.add_argument("--s3-prefix",     default="change_log")
    p.add_argument("--base-samples",  type=int,   default=60)
    p.add_argument("--recalc-recent", type=int,   default=15)
    p.add_argument("--recalc-old",    type=int,   default=5)
    p.add_argument("--purge",         type=int,   default=40)
    p.add_argument("--lag-minutes",   type=int,   default=4)
    p.add_argument("--clean",  action="store_true", help="inject no defects")
    p.add_argument("--seed",   type=int, help="fix the randomness")
    args, _unknown = p.parse_known_args()             # Glue adds args of its own

    rng = random.Random(args.seed)
    if args.clean:
        SICK_MACHINES.clear()
        global BACKGROUND_RATE
        BACKGROUND_RATE = 0.0

    conn, host = connect(args)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT current_database(), count(*) FROM assay_measurement")
    db, before = cur.fetchone()

    n_samples, mode = draw_volume(rng, args.base_samples)
    inserted, defects = new_samples(cur, n_samples, rng)
    if not args.clean and inserted:
        defects += duplicate_key(cur)
    upd_recent = recalculate_recent(cur, args.recalc_recent)
    upd_old    = recalculate_old(cur, args.recalc_old)
    voided     = void_sample(cur)
    purged     = retention_purge(cur, args.purge)
    lagged     = backdated_insert(cur, args.lag_minutes, rng)

    cur.execute("SELECT count(*) FROM assay_measurement")
    after = cur.fetchone()[0]
    conn.commit()

    truth = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "database": db, "host": host, "volume_mode": mode,
        "samples_generated": n_samples,
        "rows_before": before, "rows_after": after,
        "inserted_ids": inserted,
        "updated_recent_ids": upd_recent, "updated_old_ids": upd_old,
        "deleted_voided_ids": voided, "deleted_purged_ids": purged,
        "backdated_row": lagged, "dirty_rows": defects,
    }
    where = write_truth(truth, args.s3_bucket, args.s3_prefix)

    print(f"\n{db} at {host}   {before:,} rows before   [{mode.upper()} run]")
    print(f"  inserted            {len(inserted):>5}   ({n_samples} samples x 2 elements)")
    print(f"  recalculated recent {len(upd_recent):>5}")
    print(f"  recalculated OLD    {len(upd_old):>5}   started_utc 60+ days ago")
    print(f"  voided a sample     {len(voided):>5}   gone, no trace")
    print(f"  retention purge     {len(purged):>5}   gone, no trace")
    print(f"  backdated insert        1   id {lagged['measurement_id']}, "
          f"updated_at = now minus {args.lag_minutes} min")
    if defects:
        by_machine: dict[int, dict[str, int]] = {}
        for d in defects:
            by_machine.setdefault(d["machine_id"], {})
            by_machine[d["machine_id"]][d["defect"]] = \
                by_machine[d["machine_id"]].get(d["defect"], 0) + 1
        print(f"  DIRTY ROWS          {len(defects):>5}")
        for mid in sorted(by_machine):
            flag = "  <-- sick machine" if mid in SICK_MACHINES else ""
            kinds = ", ".join(f"{k} x{v}" for k, v in sorted(by_machine[mid].items()))
            print(f"        machine {mid:<3} {kinds}{flag}")
    print(f"  {after:,} rows after   (net {after - before:+,})")
    print(f"  ground truth  {where}\n")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
