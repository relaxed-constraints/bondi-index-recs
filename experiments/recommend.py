"""Replay a saved BONDI workload and track Atlas recommendations with Yanex.

The workload experiment is supplied as a Yanex dependency (-D). This script
configures capture, replays workloads with configurable concurrency, polls Performance Advisor (APA),
and saves observations plus ranked index suggestions. It does not build the
suggested indexes or evaluate their performance.
"""

import json
import os
from contextlib import contextmanager
from datetime import timezone
from pathlib import Path
from time import monotonic, sleep, time
from urllib.parse import quote, urlsplit

import requests
import yanex
from bondi.mongodb import MongoCollectionRef, MongoExecutor, MongoFindQuery
from bondi.workload import Workload
from bson import json_util
from dotenv import load_dotenv
from pymongo import MongoClient, ReadPreference
from pymongo.errors import PyMongoError
from requests.auth import HTTPDigestAuth

# --- Atlas access and server time ---
# MongoDB credentials execute/administer queries; Atlas API credentials retrieve
# recommendations. These are separate connections with separate permissions.


def server_time_ms(client):
    """Use the database server's clock to delimit replay, rather than the laptop clock."""
    # PyMongo's default naive datetimes represent UTC, not the local timezone.
    value = client.admin.command("hello")["localTime"]
    return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)


class AtlasAPI:
    """Reuse an authenticated HTTP session for cluster checks and APA polling."""

    def __init__(self, params):
        self.timeout = params["api_timeout_seconds"]
        self.session = requests.Session()
        self.session.auth = HTTPDigestAuth(
            os.environ["ATLAS_PUBLIC_KEY"], os.environ["ATLAS_PRIVATE_KEY"]
        )
        self.session.headers["Accept"] = f"application/vnd.atlas.{params['api_version']}+json"
        self.base = (
            "https://cloud.mongodb.com/api/atlas/v2/groups/"
            + quote(os.environ["ATLAS_PROJECT_ID"], safe="")
            + "/clusters/"
            + quote(os.environ["ATLAS_CLUSTER_NAME"], safe="")
        )

    def get(self, suffix="", **params):
        try:
            response = self.session.get(self.base + suffix, params=params, timeout=self.timeout)
        except requests.RequestException as exc:
            raise RuntimeError(f"Atlas API network failure ({type(exc).__name__})") from None
        if response.status_code != 200:
            raise RuntimeError(
                f"Atlas API HTTP {response.status_code}; check API credentials, roles, access list and cluster settings"
            )
        data = response.json()
        return data.get("content", data)

    def suggestions(self, namespace, since, until):
        return self.get(
            "/performanceAdvisor/suggestedIndexes",
            namespaces=namespace,
            since=since,
            until=until,
            envelope="false",
        )


# --- Input dependency and recommendation representation ---


def load_workload():
    """Load one completed workload experiment, retaining native BONDI query objects."""
    # Yanex resolves -D to a previous run and locates its artifacts. The custom
    # loader lets BONDI restore BSON operands and query options from its JSONL.
    deps = list(yanex.get_dependencies().values())
    if len(deps) != 1:
        raise ValueError("Supply one workload dependency: -D <workload_id>")
    dep = deps[0]
    if dep.status != "completed" or dep.script_path.name != "workload.py":
        raise ValueError("Dependency must be a completed workload.py experiment")
    manifest = dep.load_artifact("workload_manifest.json")
    if not manifest:
        raise ValueError("Workload manifest is missing")
    if manifest.get("schema_version") != 3 or manifest.get("query_format") != "bondi-workload-v1":
        raise ValueError("Expected a BONDI workload; legacy formats are no longer supported")
    workload = dep.load_artifact("workload.jsonl", loader=Workload.load)
    if workload is None:
        raise ValueError("Workload artifact is missing")
    target = MongoCollectionRef(*manifest["namespace"].split(".", 1))
    if not workload or len(workload) != manifest["query_count"]:
        raise ValueError("Workload query count does not match its manifest")
    if any(
        not isinstance(entry.query, MongoFindQuery) or entry.query.target != target
        for entry in workload
    ):
        raise ValueError("Workload must contain MongoDB find queries for its manifest namespace")
    return dep, manifest, workload


def ranked_indexes(response, namespace):
    """Normalize native APA order by descending weight; preserve compound key order."""
    result = []
    for item in response["suggestedIndexes"]:
        if item["namespace"] != namespace:
            raise ValueError("APA returned an index for an unexpected namespace")
        keys = [[field, direction] for entry in item["index"] for field, direction in entry.items()]
        if not keys or len({field for field, _ in keys}) != len(keys):
            raise ValueError("APA returned an invalid index definition")
        options = {"collation": item["collation"]} if item.get("collation") else {}
        result.append(
            {
                "namespace": namespace,
                "keys": keys,
                "options": options,
                "score": item["weight"],
            }
        )
    result.sort(
        key=lambda item: (
            -item["score"],
            json.dumps([item["keys"], item["options"]], sort_keys=True),
        )
    )
    identities = [json.dumps([item["keys"], item["options"]], sort_keys=True) for item in result]
    if len(set(identities)) != len(identities):
        raise ValueError("APA returned duplicate index definitions")
    return result


# --- Capture lifecycle and baseline indexes ---
# APA uses slow-query logging. The profile command also controls its threshold;
# we retain the existing profiling level rather than enabling system.profile.


def profile_command(db, level, **options):
    """Run the administrative command with a clearer database-permission error."""
    try:
        return db.command("profile", level, **options)
    except PyMongoError as exc:
        if getattr(exc, "code", None) != 13:
            raise
        action = "read" if level == -1 else "change/restore"
        raise RuntimeError(
            f"Cannot {action} profiling settings on database {db.name!r} "
            f"(MongoDB code={getattr(exc, 'code', None)}). "
            "The MongoDB database user needs profiling permissions, e.g. dbAdmin "
            "on this database in addition to its existing roles. "
            "Atlas API Project Read Only does not grant database permissions."
        ) from None


@contextmanager
def capture_threshold(db, params):
    """Change slow logging only, then restore even on replay failure."""
    previous = profile_command(db, -1)
    if previous.get("filter"):
        raise RuntimeError(
            "Existing profiling filter overrides slowms; use a dedicated unfiltered deployment"
        )
    primary = db.client.primary
    changed = False
    try:
        profile_command(db, previous["was"], slowms=params["slow_query_ms"], sampleRate=1.0)
        changed = True
        effective = profile_command(db, -1)
        if effective.get("slowms") != params["slow_query_ms"] or effective.get("sampleRate") != 1:
            raise RuntimeError("The requested slow logging settings did not take effect")
        effective["settings_check"] = {
            "status": "pending",
            "expected": {"slowms": params["slow_query_ms"], "sampleRate": 1.0},
            "initial": {key: effective.get(key) for key in ("slowms", "sampleRate", "filter")},
        }
        # Code inside `with capture_threshold(...)` runs here. On normal return,
        # record whether settings drifted; finally attempts restoration on errors too.
        yield effective
        if db.client.primary != primary:
            raise RuntimeError("Primary changed during replay; capture settings need inspection")
        final = profile_command(db, -1)
        check = effective["settings_check"]
        check["final"] = {key: final.get(key) for key in ("slowms", "sampleRate", "filter")}
        changed_settings = (
            final.get("slowms") != params["slow_query_ms"]
            or final.get("sampleRate") != 1
            or bool(final.get("filter"))
        )
        check["status"] = "changed" if changed_settings else "endpoints_match"
        if changed_settings:
            check["warning"] = (
                "Slow logging settings changed during replay; capture completeness is uncertain. "
                "Continuing to retrieve available APA recommendations."
            )
            print(
                f"WARNING: {check['warning']} "
                f"Expected={check['expected']}, observed={check['final']}",
                flush=True,
            )
    finally:
        # Settings belong to a host. After failover, changing the new primary would
        # not restore the original host, so stop and require inspection instead.
        if db.client.primary != primary:
            raise RuntimeError(
                "Cannot restore settings on the original primary after failover; inspect that host before retrying"
            )
        if changed:
            profile_command(
                db,
                previous["was"],
                slowms=previous["slowms"],
                sampleRate=previous.get("sampleRate", 1.0),
            )


def prepare_indexes(coll, params):
    """Require an _id_-only baseline; save and drop other indexes only with opt-in."""
    # Unlike capture settings, dropped indexes are not automatically restored.
    # Their saved definitions allow deliberate restoration after the experiment.
    indexes = list(coll.list_indexes())
    secondary = [index for index in indexes if index["name"] != "_id_"]
    if secondary:
        if not params.get("drop_existing_indexes", False):
            names = ", ".join(index["name"] for index in secondary)
            raise ValueError(
                f"Found {len(secondary)} secondary indexes: {names}. "
                "For a clean baseline, rerun with -p drop_existing_indexes=true "
                "to save their definitions and drop them. None were dropped."
            )
        yanex.save_artifact(json.loads(json_util.dumps(indexes)), "indexes_before_drop.json")
        for index in secondary:
            print(f"Dropping secondary index {index['name']}", flush=True)
            coll.drop_index(index["name"])
        indexes = list(coll.list_indexes())
        if any(index["name"] != "_id_" for index in indexes):
            raise RuntimeError("Secondary indexes remain after cleanup; replay aborted")
    return indexes


# --- Workload replay and execution observations ---


def replay(client, manifest, workload, params, *, executor):
    """Replay through BONDI while PyMongo manages capture and dataset checks."""
    # Population equality catches obvious dataset mismatches, not content changes.
    database, collection = manifest["namespace"].split(".", 1)
    db = client[database]
    coll = db[collection]
    population = coll.count_documents({}, maxTimeMS=params["query_timeout_ms"])
    if population != manifest["population"]:
        raise ValueError("Atlas collection count does not match the workload dataset")
    observations = []
    try:
        with capture_threshold(db, params) as effective:
            # Verify profiling access/settings before any index deletion.
            indexes = prepare_indexes(coll, params)
            since = server_time_ms(client)
            # Results retain input order even with concurrent execution. Finish all
            # repetitions before rejecting a replay with errors; never poll APA for it.
            for repetition in range(params["workload_repetitions"]):
                results = executor.run_workload(
                    workload,
                    query_kwargs={
                        "max_time_ms": params["query_timeout_ms"],
                        "batch_size": params["batch_size"],
                        "comment": f"yanex:{yanex.get_experiment_id()}:{repetition}",
                    },
                )
                for query_id, (_, result) in enumerate(zip(workload, results, strict=True)):
                    # Store structured error identity, not exception text that could
                    # expose connection details. None means BONDI did not run an entry.
                    error = result.error if result is not None else None
                    observations.append(
                        {
                            "repetition": repetition,
                            "query_id": query_id,
                            "returned_documents": result.row_count if result is not None else None,
                            "seconds": result.elapsed_ms / 1000 if result is not None else None,
                            "status": result.status if result is not None else "not_run",
                            "error": {
                                "type": type(error).__name__,
                                "code": getattr(error, "code", None),
                            }
                            if error is not None
                            else None,
                        }
                    )
                print(
                    f"Replay {repetition + 1}: {len(results)}/{len(workload)} results", flush=True
                )
            until = server_time_ms(client) + 1
        failures = sum(row["status"] != "success" for row in observations)
        if failures:
            raise RuntimeError(f"Replay failed for {failures} queries; APA polling skipped")
        return {
            "since_ms": since,
            "until_ms": until,
            "population": population,
            "slow_query_ms": effective["slowms"],
            "slow_query_sample_rate": effective["sampleRate"],
            "settings_check": effective.get("settings_check", {}),
            "completed_queries": len(observations),
            "initial_indexes": json.loads(json_util.dumps(indexes)),
        }
    finally:
        # Save every returned outcome, including failures, before propagating errors.
        # An interrupted run_workload call cannot supply its partial results; earlier
        # completed repetitions are still saved here.
        yanex.save_artifact(observations, "replay.jsonl")


# --- Wait for asynchronous APA processing ---


def poll_indexes(api, manifest, window, params):
    """Poll a fixed replay window until nonempty suggestions stabilize or time runs out."""
    # Padding widens the time window, not the replay itself. Keep other workloads
    # out of this interval to avoid attributing their suggestions to this run.
    padding_ms = int(params.get("window_padding_seconds", 60) * 1000)
    since_ms = window["since_ms"] - padding_ms
    until_ms = window["until_ms"] + padding_ms
    started = monotonic()
    deadline = started + params["poll_timeout_seconds"]
    sleep(min(params["initial_delay_seconds"], params["poll_timeout_seconds"]))
    previous, stable, attempt = None, 0, 0
    while monotonic() < deadline:
        attempt += 1
        response = api.suggestions(manifest["namespace"], since_ms, until_ms)
        yanex.save_artifact(response, f"apa_response_{attempt:03d}.json")
        indexes = ranked_indexes(response, manifest["namespace"])
        # IDs are response-local. Compare definitions/scores and total associated observations.
        fingerprint = (indexes, sum(s.get("count", 0) for s in response.get("shapes", [])))
        stable = stable + 1 if fingerprint == previous else 1
        previous = fingerprint
        elapsed = monotonic() - started
        print(
            f"APA poll {attempt}: {len(indexes)} indexes; stable {stable}/{params['stable_polls']}",
            flush=True,
        )
        # Stability is an acceptance heuristic, not proof that every query reached
        # APA. Save raw responses and explicitly report coverage as unverified.
        if (
            indexes
            and stable >= params["stable_polls"]
            and elapsed >= params["minimum_wait_seconds"]
        ):
            return indexes, {
                "polls": attempt,
                "request_since_ms": since_ms,
                "request_until_ms": until_ms,
                "poll_seconds": elapsed,
                "response_artifact": f"apa_response_{attempt:03d}.json",
                "acceptance": "nonempty_stable_response",
                "observation_coverage_verified": False,
            }
        sleep(max(0, min(params["poll_interval_seconds"], deadline - monotonic())))
    raise TimeoutError(
        "APA did not produce stable nonempty recommendations before the deadline; empty responses are inconclusive, not a valid zero-index result"
    )


# --- Orchestrate the tracked experiment ---


def main():
    # 1. Resolve configuration and credentials. Environment values override .env;
    # credentials stay out of the experiment's YAML parameters and saved artifacts.
    if yanex.is_standalone():
        raise RuntimeError(
            "Run with yanex run experiments/recommend.py -D <workload_id> -c experiments/configs/recommend-apa.yaml"
        )
    params = yanex.get_params()
    if params["method"] != "apa":
        raise ValueError("Only method=apa is implemented")
    load_dotenv(Path(__file__).with_name(".env"), override=False)
    required = (
        "MONGODB_URI",
        "ATLAS_PROJECT_ID",
        "ATLAS_CLUSTER_NAME",
        "ATLAS_PUBLIC_KEY",
        "ATLAS_PRIVATE_KEY",
    )
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise ValueError("Missing environment variables: " + ", ".join(missing))
    dep, manifest, workload = load_workload()
    api = AtlasAPI(params)
    try:
        # 2. Check API access and target identity before capture or index changes.
        # The API cluster and MongoDB connection must refer to the same deployment.
        cluster = api.get()
        atlas_host = urlsplit(cluster["connectionStrings"]["standardSrv"]).hostname
        if urlsplit(os.environ["MONGODB_URI"]).hostname != atlas_host:
            raise ValueError("MONGODB_URI and the Atlas API cluster refer to different targets")
        now_ms = int(time() * 1000)
        api.suggestions(manifest["namespace"], now_ms - 60000, now_ms)
        # 3. Open separate administrative and execution connections to that target.
        # Both read from the primary. The executor has no additional total-operation
        # deadline here; replay passes the configured server-side max_time_ms.
        client_options = {
            "serverSelectionTimeoutMS": params["connection_timeout_ms"],
            "socketTimeoutMS": params["query_timeout_ms"] + 10000,
            "read_preference": ReadPreference.PRIMARY,
        }
        with (
            MongoClient(os.environ["MONGODB_URI"], **client_options) as client,
            MongoExecutor.from_uri(
                os.environ["MONGODB_URI"],
                client_kwargs=client_options,
                concurrency=params.get("concurrency", 1),
                continue_on_error=True,
            ) as executor,
        ):
            window = replay(client, manifest, workload, params, executor=executor)
        # 4. Capture settings are restored before polling. Save the actual replay
        # window separately so delayed APA responses can be investigated later.
        yanex.save_artifact(window, "capture.json")
        indexes, polling = poll_indexes(api, manifest, window, params)
        # 5. Save ranked recommendations and their provenance. workload_id links
        # back to the Yanex dependency; raw APA responses retain the original evidence.
        result = {
            "schema_version": 1,
            "method": "apa",
            "status": "completed",
            "workload_id": dep.id,
            "namespace": manifest["namespace"],
            "population": manifest["population"],
            "indexes": indexes,
            "ordering": "descending APA weight; ties by keys and options",
            "api_version": params["api_version"],
            "capture": window,
            "polling": polling,
        }
        yanex.save_artifact(result, "recommendations.json")
        # Small numeric metrics support run comparisons in Yanex; the artifacts
        # above retain index definitions, capture details and polling diagnostics.
        yanex.log_metrics(
            {
                "recommended_indexes": len(indexes),
                "replayed_queries": window["completed_queries"],
                "apa_wait_seconds": polling["poll_seconds"],
            }
        )
        print(f"Saved {len(indexes)} ranked indexes in recommendations.json", flush=True)
    except PyMongoError as exc:
        # Do not let connection errors echo a credential-bearing URI into experiment logs.
        raise RuntimeError(
            f"MongoDB operation failed ({type(exc).__name__}, code={getattr(exc, 'code', None)})"
        ) from None
    finally:
        api.session.close()


if __name__ == "__main__":
    main()
