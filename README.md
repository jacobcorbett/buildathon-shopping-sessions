# Shopping Sessions

Buildathon entry for the Databricks clickstream pipeline challenge. The
native Rust executable validates the track's CSV dialect, resolves linked
identities, sessionises events and writes both daily aggregates as Parquet.

[Official track brief, rules and starter assets](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)

The submission cutoff is Sunday 22 November 2026 at 11 am ET (4 pm UK time).

## Build and verify

Build from the repository root. Keep organiser downloads, inputs and generated
outputs in an ignored directory such as `.buildathon/`.

```sh
export IMAGE=shopping-sessions:local
docker buildx build --platform linux/amd64 --load -t "$IMAGE" .
```

Use any organiser starter kit's checker scripts and fixtures. Set `KIT` to the
absolute path of the extracted kit, `DEV` to the generated development input
directory and `ANSWER` to the extracted development answers. The checker
requires Python 3.10+ and Polars.

```sh
python3 "$KIT/scripts/run_fixtures.py" "$KIT/fixtures" -- "$KIT/scripts/docker_run.sh"
"$KIT/scripts/docker_run.sh" "$DEV" .buildathon/dev-output
python3 "$KIT/scripts/compare.py" .buildathon/dev-output "$ANSWER"
```

Generate the public development input with the organiser's generator.

```sh
generate --seed 1 --events 1000000 --clicks 250000 --output .buildathon/dev
```

The container reads from `/data`, writes both Parquet tables under `/output`,
without intermediate files. The kit's runner defaults to the scorer's 8
vCPUs, 24 GiB, no swap and offline settings.
Stage timings go to standard output. The scorer measures total container
wall time.
