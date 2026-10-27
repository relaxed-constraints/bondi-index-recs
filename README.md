# BONDI index recommendation experiments

A standalone research workflow using BONDI for query generation and execution,
and Yanex for configuration, experiment tracking and artifacts. No mindexer,
mdbrtools, origami or index-advisor checkout is required.

## Setup

Install uv, then run from this repository root:

```sh
uv sync --locked
```

Python 3.11+ is required. BONDI 0.1.2 and Yanex 0.6.1 come from PyPI; `uv.lock`
records resolved dependencies. There are no editable or sibling-directory dependencies.
PyMongo supplies BSON support; do not install a separate `bson` package.

## 1. Generate and verify a workload

The YAML files target existing local MongoDB datasets on port 27018:
`yelp.business` and `citibike.rides2023`. They do not download or import data.

```sh
uv run yanex run experiments/workload.py -c experiments/configs/workload-yelp.yaml
# Or use experiments/configs/workload-citibike.yaml.
```

BONDI learns a schema, screens generated queries against an owned random sample,
then the script counts every filter against the full source collection. Any count
failure or out-of-range selectivity prevents workload export. Selectivity excludes
skip and limit. Keep the source static; population checks cannot detect in-place updates.
Sampling needs create/write/drop permissions in `bondi_samples`; source data is read
only. The sample is removed when its context exits. A generator seed does not fix
MongoDB's random sampling; reuse saved workloads for comparisons.

Override nested generation settings with e.g. `-p generation.sort_probability=1.0`.
A quick smoke run uses:

```sh
uv run yanex run experiments/workload.py -c experiments/configs/workload-yelp.yaml \
  -p num_patterns=2 -p instances_per_pattern=2 -p schema_limit=1000 \
  -p estimator_sample_size=1000 -p generation_timeout_secs=30 -t smoke
```

## 2. Collect Atlas Performance Advisor recommendations

Copy `experiments/.env.example` to `experiments/.env` and supply credentials.
Exported environment values take precedence. Never commit the filled-in file.
The Atlas database must contain the intended workload dataset; the script checks
its collection name and population, not document-by-document equality.

```sh
uv run yanex run experiments/recommend.py \
  -c experiments/configs/recommend-apa.yaml -D <workload-experiment-id>
```

The supplied config deliberately has `drop_existing_indexes: true` for dedicated
research databases. Secondary index definitions are saved before deletion; they
are not automatically recreated. The script preserves `_id_`, temporarily changes
slow-query logging settings, replays queries sequentially through BONDI, restores
capture settings, then polls APA. It does not build suggested indexes or evaluate them.

Database credentials need read, index-management and capture-setting permissions.
Atlas API credentials need access to the selected project's recommendation endpoints;
API access and database access are separate. Network access to both is required.
APA stability is a heuristic, not proof of complete query coverage; empty responses
until the deadline fail the run rather than count as a valid zero-index result.

## Results and provenance

Yanex stores runs under `~/.yanex/experiments/` by default. The recommendation's
explicit dependency ID identifies its workload; runs from the old repository remain
in their original project. Only native BONDI workloads are supported.

- Workload: `workload.jsonl`, `pattern_summary.json`, `workload_manifest.json`.
- Recommendation: `replay.jsonl`, `capture.json`, raw `apa_response_*.json`,
  `recommendations.json`, and index definitions when indexes were dropped.
- Yanex also records parameters, metrics and run status. Partial replay observations
  are retained on failure; downstream work must require a completed run.

Load a saved workload with `bondi.workload.Workload.load(path)`. The custom Yanex
loader/saver hooks preserve BONDI query objects and BSON operands.

## Development

```sh
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Tests mock database/API operations and require no credentials. CI runs these offline
checks only. Run database-backed smoke checks deliberately against research datasets.
