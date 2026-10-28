"""Generate patterns with BONDI, verify their filters against MongoDB, and save to Yanex.

Run through ``yanex run`` with a workload YAML config. BONDI supplies schema
inspection, sample-based estimation and generation; this script adds dataset
constraints, exact verification and experiment artifacts for recommendation runs.
"""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from importlib.metadata import version
from math import ceil, floor
from time import monotonic

import yanex
from bondi.mongodb import (
    MongoCollectionRef,
    MongoExecutor,
    MongoGenerationConfig,
    MongoSampleEstimator,
    MongoWorkloadGenerator,
)
from bondi.schema import Schema
from pymongo import MongoClient

# --- Dataset-specific pattern constraints ---
# A pattern fixes fields, operators, operand types and query options; its instances
# vary the sampled values. These callbacks constrain shapes before BONDI accepts
# them. They belong to this experiment rather than the general-purpose toolkit.


def attribute_fields(pattern):
    """Distinct attributes used anywhere in the complete query shape."""
    fields = {p.field for p in pattern.predicates}
    fields.update(field for field, _ in pattern.sort or ())
    fields.update(field for field, _ in pattern.projection or ())
    return {field for field in fields if field == "attributes" or field.startswith("attributes.")}


def validate_yelp_pattern(
    candidate,
    accepted_patterns,
    *,
    num_patterns,
    max_attribute_fields=1,
    max_attribute_pattern_fraction=0.5,
):
    """At most one attribute field overall and at least half attribute-free queries."""
    count = len(attribute_fields(candidate))
    if count > max_attribute_fields:
        return False
    if count == 0:
        return True
    quota = int(num_patterns * max_attribute_pattern_fraction)
    return sum(bool(attribute_fields(p)) for p in accepted_patterns) < quota


def validate_citibike_pattern(candidate, accepted_patterns):
    """Exclude name/ID pairs for the same station endpoint."""
    fields = {p.field for p in candidate.predicates}
    return not any(
        pair <= fields
        for pair in (
            {"start_station_name", "start_station_id"},
            {"end_station_name", "end_station_id"},
        )
    )


def pattern_validator(namespace, num_patterns, diversity=None):
    """Choose a callback by collection; partial binds Yelp's experiment-wide quota."""
    diversity = diversity or {}
    if not diversity.get("enabled", True):
        return None
    max_fields = diversity.get("max_attribute_fields", 1)
    fraction = diversity.get("max_attribute_pattern_fraction", 0.5)
    if type(max_fields) is not int or max_fields < 0 or not 0 <= fraction <= 1:
        raise ValueError("Invalid attribute diversity limits")
    return {
        "yelp.business": partial(
            validate_yelp_pattern,
            num_patterns=num_patterns,
            max_attribute_fields=max_fields,
            max_attribute_pattern_fraction=fraction,
        ),
        "citibike.rides2023": validate_citibike_pattern,
    }.get(namespace)


# --- Experiment workflow: configure → sample/generate → verify → save ---


def main():
    # 1. Enter through Yanex so parameters, metrics and artifacts belong to a run.
    # get_params() returns the YAML configuration with CLI -p overrides applied.
    if yanex.is_standalone():
        raise RuntimeError("Run this script with yanex run experiments/workload.py")

    params = yanex.get_params()

    # 2. Identify the collection and open a client for schema reads and exact counts.
    # A BONDI reference is just a database/collection name, not a live connection.
    target = MongoCollectionRef(params["database"], params["collection"])
    namespace = f"{target.database}.{target.collection}"
    client = MongoClient(params["uri"])
    source = client[target.database][target.collection]
    started = monotonic()
    try:
        population = source.count_documents({}, hint="_id_", maxTimeMS=params["count_max_time_ms"])
        if not population:
            raise ValueError("The source collection is empty")
        print(f"Source {namespace}: {population:,} documents", flush=True)

        # 3. Learn available fields/types and candidate values from a bounded cursor.
        # This schema input is the first schema_limit documents, not a random sample.
        # Streaming avoids retaining all documents; distinct values still use memory.
        with source.find({}).limit(params["schema_limit"]) as documents:
            schema = Schema.from_documents(documents, values="distinct")
        validator = pattern_validator(namespace, params["num_patterns"], params.get("diversity"))
        # 4. Generate candidates using one persisted random sample for screening.
        # The estimator's context prepares its owned collection and drops it on exit,
        # including when generation raises; the executor context closes its connection.
        # Screening uses MongoDB semantics but is approximate. The tighter margin
        # reduces boundary failures; only the later full-collection counts are exact.
        with (
            MongoExecutor.from_uri(
                params["uri"], timeout_seconds=params["count_max_time_ms"] / 1000
            ) as executor,
            MongoSampleEstimator(
                executor,
                target,
                sample_size=min(params["estimator_sample_size"], population),
                persist=True,
            ) as estimator,
        ):
            # YAML's generation section maps directly to BONDI's configuration.
            # The seed controls generation choices, not MongoDB's random sample.
            generator = MongoWorkloadGenerator(
                target=target,
                schema=schema,
                estimator=estimator,
                config=MongoGenerationConfig(**params["generation"]),
            )
            workload, patterns = generator.generate(
                num_patterns=params["num_patterns"],
                instances_per_pattern=params["instances_per_pattern"],
                return_patterns=True,
                min_selectivity=params["min_selectivity"] + params["screening_margin"],
                max_selectivity=params["max_selectivity"] - params["screening_margin"],
                validate_pattern=validator,
                seed=params["seed"],
                max_candidate_attempts=params["max_candidate_attempts"],
                max_instance_attempts=params["max_instance_attempts"],
                timeout_seconds=params["generation_timeout_secs"],
            )
            generation_stats = generator.last_stats
        # 5. Check and inspect the generated groups before expensive verification.
        # Entries retain pattern IDs, which index the returned patterns list.
        pattern_ids = [entry.pattern_id for entry in workload]
        native_queries = [entry.query for entry in workload]
        expected = params["num_patterns"] * params["instances_per_pattern"]
        if (
            len(workload) != expected
            or len(patterns) != params["num_patterns"]
            or Counter(pattern_ids)
            != {pid: params["instances_per_pattern"] for pid in range(params["num_patterns"])}
            or not all(
                patterns[pid].matches(query) for pid, query in zip(pattern_ids, native_queries)
            )
        ):
            raise RuntimeError("Generator returned an incomplete or inconsistent workload")
        generation_seconds = monotonic() - started
        queries = native_queries
        print("\nGenerated patterns (before exact verification):", flush=True)
        for pattern_id, pattern in enumerate(patterns):
            print(
                f"Pattern {pattern_id}: {pattern}; "
                f"sort={pattern.sort}, skip={pattern.skip}, limit={pattern.limit}, "
                f"projection={pattern.projection}",
                flush=True,
            )
        print("\nGenerated queries:", flush=True)
        for query_id, (pattern_id, query) in enumerate(zip(pattern_ids, queries)):
            print(
                f"Query {query_id} (pattern {pattern_id}): {query.to_json()}",
                flush=True,
            )
        # 6. Count filter matches over the full collection, ignoring skip and limit.
        # Selectivity is matching documents / source population, not returned rows.
        verification_started = monotonic()

        min_count = ceil(params["min_selectivity"] * population)
        max_count = floor(params["max_selectivity"] * population)
        print(
            f"\nExact selectivity verification: {len(queries):,} queries, "
            f"{params['verify_workers']} workers; allowed matches "
            f"{min_count:,}–{max_count:,} documents (inclusive) out of {population:,} "
            f"(selectivity {params['min_selectivity']:g}–{params['max_selectivity']:g}). "
            "Counts apply to filters before skip/limit.",
            flush=True,
        )

        def count_query(query):
            return source.count_documents(query.filter, maxTimeMS=params["count_max_time_ms"])

        # map preserves query order even when counts finish out of order. A timeout
        # or database error propagates, preventing the artifact-saving phase below.
        # This concurrency applies only to verification, not workload generation.
        with ThreadPoolExecutor(max_workers=params["verify_workers"]) as pool:
            counts = list(pool.map(count_query, queries))
        selectivities = [count / population for count in counts]
        print(
            f"Exact verification finished: {len(counts)}/{len(queries)} counted "
            f"in {monotonic() - verification_started:.1f}s.",
            flush=True,
        )
        if not all(
            params["min_selectivity"] <= s <= params["max_selectivity"] for s in selectivities
        ):
            raise RuntimeError("Exact selectivity verification failed; no workload exported")
        # Detect population changes during the run. Equal counts do not detect
        # in-place updates: the experiment still assumes a static source dataset.
        after = source.count_documents({}, hint="_id_", maxTimeMS=params["count_max_time_ms"])
        if after != population:
            raise RuntimeError("Source population changed during generation; no workload exported")
        # 7. Summarize each pattern using the verified selectivities. Fingerprints
        # count distinct full queries, including options and compound sort order.
        canonical = [q.fingerprint() for q in native_queries]  # Preserves compound sort precedence.
        summary = []
        for pid in range(params["num_patterns"]):
            ids = [i for i, p in enumerate(pattern_ids) if p == pid]
            values = [selectivities[i] for i in ids]
            summary.append(
                {
                    "pattern_id": pid,
                    "pattern": patterns[pid].to_dict(),
                    "query_ids": ids,
                    "queries": len(ids),
                    "unique_queries": len({canonical[i] for i in ids}),
                    "min_selectivity": min(values),
                    "max_selectivity": max(values),
                    "mean_selectivity": sum(values) / len(values),
                }
            )
        # 8. Publish artifacts only after every query passes verification.
        # Yanex chooses the run's artifact directory; BONDI's custom saver preserves
        # native query types and pattern IDs in JSONL. The manifest describes the
        # dataset and format so recommend.py can validate its dependency.
        yanex.save_artifact(summary, "pattern_summary.json")
        yanex.save_artifact(workload, "workload.jsonl", saver=lambda wkl, path: wkl.save(path))
        yanex.save_artifact(
            {
                "schema_version": 3,
                "query_format": "bondi-workload-v1",
                "generator": "bondi.mongodb.MongoWorkloadGenerator.generate",
                "generation_stats": generation_stats,
                "selectivity_semantics": "filter matches / full collection population, before skip and limit",
                "namespace": namespace,
                "population": population,
                "workload_artifact": "workload.jsonl",
                "query_count": len(queries),
                "pattern_count": len(patterns),
                "server_version": client.server_info()["version"],
                "package_versions": {name: version(name) for name in ("yanex", "bondi", "pymongo")},
                "exact_verified": True,
                "source_must_be_static": True,
                "reproducibility_note": "Seed controls generator RNG, not MongoDB sampling; reuse this workload artifact.",
            },
            "workload_manifest.json",
        )
        # Metrics are scalar summaries for comparing Yanex runs; artifacts hold
        # the detailed workload and per-pattern results used by downstream scripts.
        yanex.log_metrics(
            {
                "query_count": len(queries),
                "pattern_count": len(patterns),
                "unique_queries": len(set(canonical)),
                "queries_with_sort": sum(bool(q.sort) for q in native_queries),
                "queries_with_limit": sum(q.limit is not None for q in native_queries),
                "queries_with_projection": sum(q.projection is not None for q in native_queries),
                "queries_with_skip": sum(bool(q.skip) for q in native_queries),
                "population": population,
                "generation_seconds": generation_seconds,
                "verification_seconds": monotonic() - verification_started,
                "min_selectivity": min(selectivities),
                "max_selectivity": max(selectivities),
            }
        )
        print(
            f"Saved verified workload.jsonl ({len(queries)} queries)",
            flush=True,
        )
    finally:
        # Also close the direct PyMongo connection if any earlier stage failed.
        client.close()


if __name__ == "__main__":
    main()
