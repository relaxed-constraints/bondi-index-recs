# Index recommendation experiments using BONDI and Yanex

Yanex experiments for random workload generation and Atlas Performance Advisor index
recommendation collection using the new [BONDI](https://relaxed-constraints.github.io/bondi/)
database research toolkit and [Yanex](https://github.com/rueckstiess/yanex) for
configuration, experiment tracking and artifacts.

There are two Yanex scripts:

1.  `experiments/workload.py`: Generates and verifies query workloads against local MongoDB datasets
2.  `experiments/recommend.py`: Collects Atlas Performance Advisor recommendations for a given workload

See details below.


## Setup

### Using `uv` (preferred)

Install [uv](https://docs.astral.sh/uv/), then run from this repository root:

```sh
uv sync --locked
```

BONDI 0.1.3 and Yanex are installed from PyPI; no separate BONDI checkout is needed.

### Using `pip`

From the repository root, in an activated Python virtual environment:

```sh
python -m pip install -r requirements.txt
```

Python 3.11+ is required.


### Datasets

The two datasets, **yelp** and **citibike**, can be downloaded as mongodumps here:
- [Yelp dataset](https://www.dropbox.com/scl/fi/a1yukzvmf8ubdlw8x3iq1/yelp_mongodump.zip?rlkey=3dk6sokdta5a88ota872djmpl&st=v9di45gr&dl=0)
- [Citibike dataset](https://www.dropbox.com/scl/fi/2k5f8fqzbcfe4g352lyt4/citibike_2023_mongodump.zip?rlkey=l7pz8nv0635b3mqdorfziaqki&dl=0)

Unzip, then use `mongorestore` to import the datasets into your local MongoDB instance.

The config files assume MongoDB running on default port 27017. The namespaces are configured to be
`yelp.business` and `citibike.rides2023`. Both can be changed in the config files under `experiments/configs/`.


### Atlas Cluster (for index recommendations)

You need to provision a dedicated (paid) Atlas cluster for collecting index recommendations (Performance Advisor is not available on free or shared tiers).

#### Provisioning the cluster

Use the following cluster configurations for minimal setup and to keep costs low:

- Deploy on AWS in Sydney region (ap-southeast-2)
- Cluster tier: M10
- Storage: Increase to 30GB
- Disable Auto-scaling (cluster tier and storage scaling)
- MongoDB version 8.0 or higher
- Backup and restore options: Disabled

Some of these settings are only visible in the _Advanced Settings_ section.

Provisioning can take 5-10 minutes.

#### Configuring Database User and API Key

Under "Database & Network Access", configure a database user with `dbAdminAnyDatabase@admin` and `readWriteAnyDatabase@admin` roles and make sure to whitelist your IP address to allow connections from your local machine.

Under "Project Identity & Access Management > Applications > API Keys", create an API key with the necessary permissions to access the Atlas Performance Advisor via API. You need the `Project Observability Viewer` and `Project Read Only` project permissions. This key will be used for the `ATLAS_PUBLIC_KEY` and `ATLAS_PRIVATE_KEY` environment variables, see below.

#### Creating the `.env` Environment file

Copy the example environment file (experiments/.env.example) to `experiments/.env` and fill in the required values:

- `MONGODB_URI` The connection string for your Atlas MongoDB cluster. Click `Connect` in the Atlas UI and select `Connect your application` to get the connection string, something like `mongodb+srv://<username>:<password>@<cluster-id>.mongodb.net/`. Replace `<username>`, `<password>`, with the DB User credentials you created.
- `ATLAS_PROJECT_ID` The ID of your Atlas project. You can find the ID in the URL when you are viewing your project in the Atlas UI, e.g. `https://cloud.mongodb.com/v2/<ATLAS_PROJECT_ID>/#/clusters`.
- `ATLAS_CLUSTER_NAME` The name of the Atlas cluster (not its hostname).
- `ATLAS_PUBLIC_KEY` Your Atlas public key, as generated for the API key you created.
- `ATLAS_PRIVATE_KEY` Your Atlas private key, as generated for the API key you created.


## Usage

Change into the `experiments` directory and run the Yanex scripts as described below.

```
cd experiments
```

With uv, prefix the commands below with `uv run` (for example,
`uv run yanex run workload.py -c configs/workload-yelp.yaml`). With pip, keep
your virtual environment activated so `yanex` is on your PATH.

### 1. Generate a workload

Make sure you have the data imported into your local MongoDB instance and updated the YAML config files
to your specific local MongoDB setup and namespaces.

To generate a workload for the `yelp` dataset, run:

```sh
yanex run workload.py -c configs/workload-yelp.yaml
```

Change the config file to `configs/workload-citibike.yaml` for the Citibike dataset.


#### Most common config parameters

- `num_patterns`: Number of query patterns to generate.
- `instances_per_pattern`: Number of instances per query pattern.
- `min_selectivity`: Minimum selectivity for generated queries.
- `max_selectivity`: Maximum selectivity for generated queries.
- `seed`: Random seed for workload generation.

- `generation.min_predicates`: Minimum number of predicates per query pattern.
- `generation.max_predicates`: Maximum number of predicates per query pattern.

See the [MongoWorkloadGenerator](https://relaxed-constraints.github.io/bondi/reference/mongodb/generation/#bondi.mongodb.MongoWorkloadGenerator.generate) and [MongoGenerationConfig](https://relaxed-constraints.github.io/bondi/reference/mongodb/generation/#bondi.mongodb.MongoGenerationConfig) documentation for more details on these parameters.

These can be changed in the respective YAML config files or overridden via command-line arguments with the `-p` flag, for example:

```sh
yanex run workload.py -c configs/workload-yelp.yaml -p num_patterns=10 -p instances_per_pattern=5
```

All other `yanex` CLI options can be used alongside, like specifying an experiment name `-n <name>` or adding tags `-t <tag>` for better experiment management, see [Yanex run command](https://github.com/rueckstiess/yanex/blob/main/docs/commands/run.md) documentation.



### 2. Collect Atlas Performance Advisor recommendations

Make sure you have the same dataset imported into your Atlas cluster and filled in
`experiments/.env` as described above. `MONGODB_URI` must point to this Atlas cluster.
The script gets the database and collection names from the workload experiment,
so the namespaces should match your local dataset.

To collect recommendations for a generated workload, run from the `experiments` directory:

```sh
yanex run recommend.py -c configs/recommend-apa.yaml -D <workload-experiment-id>
```

Replace `<workload-experiment-id>` with the ID printed by Yanex when step 1 completed (get it via `yanex list`).

The `-D` flag makes that run a dependency: the script loads its saved workload, and
Yanex records which workload was used. The same recommendation config works for both
Yelp and Citibike. See the [Yanex dependencies documentation](https://github.com/rueckstiess/yanex/blob/main/docs/commands/run.md#dependencies) for more details.

The script replays the workload using the
[BONDI MongoExecutor](https://relaxed-constraints.github.io/bondi/reference/mongodb/executors/#bondi.mongodb.MongoExecutor),
then waits for Atlas Performance Advisor to return stable recommendations. It saves
the suggested indexes in `recommendations.json`; it does not create those indexes.

#### Most common config parameters

- `concurrency`: Number of concurrent replay queries (default: `1`, sequential execution).
- `workload_repetitions`: Number of times to replay the entire workload (default: `1`).
- `slow_query_ms`: Slow-query logging threshold during replay, in milliseconds (default: `0`, including even very fast queries in slow-query logging).
- `query_timeout_ms`: Server-side time limit for each replayed query, in milliseconds (default: `360000`, or 6 minutes).
- `poll_timeout_seconds`: Maximum time to wait for recommendations after replay (default: `600`).
- `poll_interval_seconds`: Time between recommendation requests (default: `30`).
- `minimum_wait_seconds`: Minimum polling time before accepting stable recommendations (default: `120`).
- `stable_polls`: Number of consecutive matching recommendation summaries required (default: `3`).
- `drop_existing_indexes`: Drop secondary indexes before replay to start from an `_id_`-only baseline (default: `true`). Their definitions are saved, but the indexes are not automatically restored.

These can be changed in `configs/recommend-apa.yaml` or overridden via command-line
arguments with the `-p` flag, for example:

```sh
yanex run recommend.py -c configs/recommend-apa.yaml -D <workload-experiment-id> \
  -p workload_repetitions=3 -p slow_query_ms=0 -p poll_timeout_seconds=1200
```

Atlas processes query logs asynchronously, so recommendations may take a few minutes
to appear. If no stable, nonempty result arrives before the timeout, the experiment
fails rather than reporting that no indexes are needed. Stable results do not guarantee
that Atlas has processed every query. Slow-query capture settings are restored after replay.

As with workload generation, you can add a name and tags to make runs easier to find:

```sh
yanex run recommend.py -c configs/recommend-apa.yaml -D <workload-experiment-id> \
  -n yelp-apa -t yelp
```

See the [Yanex run command](https://github.com/rueckstiess/yanex/blob/main/docs/commands/run.md)
for additional CLI options.


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

See the documentation for the [BONDI Workload class](https://relaxed-constraints.github.io/bondi/reference/queries/#bondi.workload.Workload).


## Accessing the workload and index recommendations

To use the workload in your own code from a Yanex `workload` experiment, you can load it like so:

```python
import yanex.results as yr
from bondi.workload import Workload

# Load the workload artifact from a completed Yanex workload experiment.
experiment = yr.get_experiment("<workload-experiment-id>")
workload = experiment.load_artifact("workload.jsonl", loader=Workload.load)

# Each entry contains a query and its optional pattern ID.
for entry in workload:
    query = entry.query

    pattern_id = entry.pattern_id
    filter_doc = query.filter  # dict; BSON values are already restored
    projection = query.projection  # dict, or None
    sort = query.sort  # ordered tuple, e.g. (("stars", -1),)
    skip = query.skip  # integer; 0 means no skip
    limit = query.limit  # integer, or None for no limit

    # Use these values in your own code.
```

To access the index recommendations from a `recommend` experiment, you can load them like so:

```python
import yanex.results as yr

# Load the recommendations from a completed Yanex recommendation experiment.
experiment = yr.get_experiment("<recommendation-experiment-id>")
recommendations = experiment.load_artifact("recommendations.json")

# Already ordered by descending APA score; preserve this order.
indexes = recommendations["indexes"]

for rank, index in enumerate(indexes, start=1):
    score = index["score"]  # APA weight
    keys = index["keys"]  # ordered pairs, e.g. [["city", 1], ["stars", -1]]

    # PyMongo expects a list of tuples for compound index keys.
    keys = [(field, direction) for field, direction in keys]

    # Use rank, keys and score in your own code.
```

## Demo notebook

`notebooks/load_yanex_results.ipynb` shows how to inspect saved patterns, queries
and recommendations. Replace the two experiment IDs with completed runs available
in your local Yanex store. Select this project's Python environment in your
notebook editor (install `ipykernel` there if prompted).

## Development

```sh
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Tests mock database/API operations and require no credentials. CI runs these offline
checks only. Run database-backed smoke checks deliberately against research datasets.

When dependencies change, regenerate the pip requirements from the lockfile:

```sh
uv export --locked --no-dev --no-emit-project --no-hashes --format requirements-txt --output-file requirements.txt
```

Replay attempts all queries with `continue_on_error=True` and saves ordered outcomes
(including status and error type/code) in `replay.jsonl`. If any query failed, the
experiment fails before polling APA. Each repetition uses a shared experiment comment.
Results are available after each whole-workload call; interrupting a call loses that
call's partial observations, while completed repetitions are retained.

Pattern summaries now embed BONDI's versioned dictionary representation. To print
patterns from a workload experiment:

```python
import yanex.results as yr
from bondi.mongodb import MongoQueryPattern

experiment = yr.get_experiment("<workload-experiment-id>")
for summary in experiment.load_artifact("pattern_summary.json"):
    pattern = MongoQueryPattern.from_dict(summary["pattern"])
    print(f"Pattern {summary['pattern_id']}: {pattern}")
```

This applies to newly generated summaries. Older `asdict()` summaries have no format
version; their patterns can instead be reconstructed from saved workload queries
with `MongoQueryPattern.from_query(entry.query)`.
