# Shopping Sessions

Buildathon entry for the Databricks clickstream pipeline challenge.

[Official track brief and submission page](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)

The submission cutoff is Sunday 22 November 2026 at 11 am ET (4 pm UK time).

## Local setup

The official rules, PySpark, DuckDB and Polars starters, public development answers and generator are in `.buildathon/` on Yoga. Extract the chosen kit there and follow its setup instructions. Docker is available on Yoga.

Validate both Parquet tables against the exact expected rows. The linux/amd64 scorer mounts input at /data and measures total container wall time. The final uses roughly 60 million events with a three-minute run cap.
