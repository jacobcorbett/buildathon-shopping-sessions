"""Normalise inputs, resolve identity links, and run the DuckDB stages."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import csv
import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from time import perf_counter

import duckdb


DATA = Path(os.environ.get("DATA_DIR", "/data"))
OUTPUT = Path(os.environ.get("OUTPUT_DIR", "/output"))
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
THREADS = int(os.environ.get("NCPU", "8"))
MEMORY = int(os.environ.get("MEM_GB", "24"))
INTEGER_FIELDS = {"ts", "ingest_ts", "price_cents"}
SCHEMAS = {
    "events": ("event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"),
    "clicks": ("click_id", "user_id", "campaign_id", "ts"),
    "links": ("user_id_a", "user_id_b"),
    "products": ("product_id", "category"),
    "campaigns": ("campaign_id", "channel"),
}


@contextmanager
def timed(stage):
    start = perf_counter()
    yield
    print(f"{stage}: {perf_counter() - start:.3f} s", flush=True)


def normalise(source, destination, width):
    subprocess.run(["normalise", str(width), source, destination], check=True)


def sql_type(field):
    return "BIGINT" if field in INTEGER_FIELDS else "VARCHAR"


def csv_view(db, name, files, fields):
    columns = "{" + ", ".join(f"'{field}': '{sql_type(field)}'" for field in fields) + "}"
    db.sql(
        rf"""
        SELECT * FROM read_csv(?, columns={columns}, auto_detect=false,
            delim=',', header=false, quote='"', escape='', new_line='\n')
        """,
        params=[[str(file) for file in files]],
    ).create_view(name)


def prepare_inputs(db, workspace):
    files = {}
    tasks = []
    for name, fields in SCHEMAS.items():
        if name in ("events", "clicks", "links"):
            sources = sorted((DATA / name).glob("*.csv"))
        else:
            sources = [DATA / f"{name}.csv"]
        files[name] = []
        for index, source in enumerate(sources):
            destination = workspace / f"{name}-{index}.csv"
            files[name].append(destination)
            tasks.append((source, destination, len(fields)))
    # Start large shards first so a single large tail shard does not hold up SQL.
    tasks.sort(key=lambda task: task[0].stat().st_size, reverse=True)
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        for job in [pool.submit(normalise, *task) for task in tasks]:
            job.result()
    for name, paths in files.items():
        if paths:
            csv_view(db, f"raw_{name}", paths, SCHEMAS[name])
        else:
            columns = ", ".join(f"NULL::{sql_type(field)} AS {field}" for field in SCHEMAS[name])
            db.execute(f"CREATE VIEW raw_{name} AS SELECT {columns} WHERE false")


def resolve_links(db, mapping):
    # Union by size and compress paths. Compute the minimum separately so the
    # representative can follow tree size without affecting the public ID.
    ids = {}
    parents = []
    sizes = []
    minimum = []

    def index(user):
        if user not in ids:
            ids[user] = len(parents)
            parents.append(len(parents))
            sizes.append(1)
            minimum.append(user)
        return ids[user]

    def root(node):
        while node != parents[node]:
            parents[node] = parents[parents[node]]
            node = parents[node]
        return node

    links = db.execute("""
        SELECT user_id_a, user_id_b FROM raw_links
        WHERE user_id_a IS NOT NULL AND user_id_b IS NOT NULL
    """)
    while batch := links.fetchmany(32768):
        for a, b in batch:
            left, right = root(index(a)), root(index(b))
            if left == right:
                continue
            if sizes[left] < sizes[right]:
                left, right = right, left
            parents[right] = left
            sizes[left] += sizes[right]
            minimum[left] = min(minimum[left], minimum[right])
    # CSV quoting protects any carriage returns in a user ID.
    with mapping.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file, quoting=csv.QUOTE_ALL, lineterminator="\n")
        for user, node in ids.items():
            person = minimum[root(node)]
            if user != person:
                writer.writerow((user, person))
    csv_view(db, "identities", [mapping], ("user_id", "person"))


def main():
    manifest = json.loads((DATA / "manifest.json").read_text())
    with TemporaryDirectory(dir=SCRATCH) as temp, duckdb.connect() as db:
        workspace = Path(temp)
        db.execute("SET temp_directory = ?", [temp])
        db.execute("SET threads = ?", [THREADS])
        db.execute("SET memory_limit = ?", [f"{max(1, MEMORY * 3 // 4)}GB"])
        db.execute("SET preserve_insertion_order = false")
        db.execute(
            "CREATE TABLE bounds AS SELECT epoch_ms(?::TIMESTAMP) AS start_ms, epoch_ms(?::TIMESTAMP) AS end_ms",
            [manifest["window_start"], manifest["window_end"]],
        )
        with timed("normalise CSV"):
            prepare_inputs(db, workspace)
        with timed("resolve linked IDs"):
            resolve_links(db, workspace / "identity.csv")
        sql = Path(__file__).with_name("pipeline.sql").read_text()
        for stage in sql.split("-- stage: ")[1:]:
            name, statements = stage.split("\n", 1)
            with timed(name):
                db.execute(statements)
        with timed("write Parquet"):
            for table in ("user_daily", "campaign_daily"):
                folder = OUTPUT / table
                folder.mkdir(parents=True, exist_ok=True)
                db.execute(f"COPY {table} TO ? (FORMAT PARQUET)", [str(folder / "part-0.parquet")])


if __name__ == "__main__":
    main()
