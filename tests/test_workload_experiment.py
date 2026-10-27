"""Artifact integrity and cleanup tests for the workload experiment."""

import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import yaml
from bondi.mongodb import (
    MongoCollectionRef,
    MongoFindQuery,
)
from bondi.mongodb import (
    MongoQueryPattern as QueryPattern,
)
from bondi.workload import Workload, WorkloadEntry

from experiments import workload as experiment


def FindQuery(filter, *, collection="business", sort=None, **kwargs):
    return MongoFindQuery(
        MongoCollectionRef("yelp", collection),
        filter,
        sort=tuple((sort or {}).items()),
        **kwargs,
    )


class WorkloadExperimentTests(unittest.TestCase):
    def test_standalone_does_not_connect(self):
        with (
            patch.object(experiment.yanex, "is_standalone", return_value=True),
            patch.object(experiment, "MongoClient") as connect,
        ):
            with self.assertRaisesRegex(RuntimeError, "yanex run"):
                experiment.main()
            connect.assert_not_called()

    def run_case(self, *, exact_count=5, generation_error=None, final_population=1000):
        config = Path(__file__).parents[1] / "experiments/configs/workload-yelp.yaml"
        params = dict(
            yaml.safe_load(config.read_text()),
            num_patterns=1,
            instances_per_pattern=1,
            min_selectivity=0.00001,
            max_selectivity=0.9,
        )
        query = FindQuery(
            {"city": {"$eq": "example"}},
            sort={"stars": -1, "city": 1},
            limit=2,
            projection={"city": 1},
            collection="business",
        )
        workload = Workload([WorkloadEntry(query, pattern_id=0)])
        patterns = [QueryPattern.from_query(query)]
        generator = Mock()
        generator.last_stats = {"accepted_patterns": 1}
        generator.generate = Mock(return_value=(workload, patterns), side_effect=generation_error)
        source = MagicMock()
        source.count_documents.side_effect = [1000, exact_count, final_population]
        client = MagicMock()
        client.__getitem__.return_value.__getitem__.return_value = source
        client.server_info.return_value = {"version": "test"}
        source.find.return_value.limit.return_value.__enter__.return_value = iter(
            [{"city": "example", "stars": 4}]
        )
        executor = MagicMock()
        estimator = MagicMock()
        copied, saved = {}, {}

        def save(value, name, *, saver=None):
            if saver is None:
                saved[name] = value
            else:
                from tempfile import TemporaryDirectory

                with TemporaryDirectory() as directory:
                    path = Path(directory) / name
                    saver(value, path)
                    copied[name] = path.read_bytes()

        with ExitStack() as stack:
            stack.enter_context(patch.object(experiment.yanex, "is_standalone", return_value=False))
            stack.enter_context(
                patch.object(
                    experiment.yanex,
                    "get_params",
                    return_value=params,
                )
            )
            stack.enter_context(
                patch.object(
                    experiment.yanex,
                    "save_artifact",
                    side_effect=save,
                )
            )
            stack.enter_context(
                patch.object(
                    experiment.yanex,
                    "copy_artifact",
                    side_effect=lambda path, name: copied.update({name: Path(path).read_bytes()}),
                )
            )
            stack.enter_context(patch.object(experiment.yanex, "log_metrics"))
            stack.enter_context(patch.object(experiment, "MongoClient", return_value=client))
            stack.enter_context(
                patch.object(experiment, "MongoWorkloadGenerator", return_value=generator)
            )
            stack.enter_context(
                patch.object(experiment.MongoExecutor, "from_uri", return_value=executor)
            )
            stack.enter_context(
                patch.object(experiment, "MongoSampleEstimator", return_value=estimator)
            )
            error = None
            try:
                experiment.main()
            except RuntimeError as exc:
                error = exc
        client.drop_database.assert_not_called()
        if generation_error is None:
            self.assertEqual(
                source.count_documents.call_args_list[1].args,
                (query.filter,),
            )
        if generation_error is None:
            self.assertEqual(
                source.count_documents.call_args_list[1].kwargs,
                {"maxTimeMS": params["count_max_time_ms"]},
            )
        client.close.assert_called_once()
        estimator.__exit__.assert_called_once()
        executor.__exit__.assert_called_once()
        return copied, saved, error

    def test_exact_failure_blocks_export(self):
        copied, saved, error = self.run_case(exact_count=950)
        self.assertIn("Exact selectivity", str(error))
        self.assertNotIn("workload.jsonl", copied)
        self.assertNotIn("verification.jsonl", saved)

    def test_verification_start_and_completion_are_flushed(self):
        with patch("builtins.print") as output:
            _, _, error = self.run_case()
        self.assertIsNone(error)
        messages = [str(call.args[0]) for call in output.call_args_list]
        self.assertTrue(any("Exact selectivity verification:" in m for m in messages))
        self.assertTrue(any("Exact verification finished: 1/1 counted" in m for m in messages))
        self.assertTrue(all(call.kwargs.get("flush") for call in output.call_args_list))

    def test_generator_failure_cleans_up(self):
        copied, _saved, error = self.run_case(generation_error=RuntimeError("generation failed"))
        self.assertEqual(str(error), "generation failed")
        self.assertNotIn("workload.jsonl", copied)

    def test_population_change_blocks_export(self):
        copied, _, error = self.run_case(final_population=1001)
        self.assertIn("population changed", str(error))
        self.assertNotIn("workload.jsonl", copied)

    def test_count_failure_blocks_export(self):
        copied, _, error = self.run_case(exact_count=RuntimeError("count timed out"))
        self.assertIn("count timed out", str(error))
        self.assertNotIn("workload.jsonl", copied)

    def test_native_generation_config_for_both_datasets(self):
        for name, max_predicates, excluded in (
            ("yelp", 5, "business_id"),
            ("citibike", 8, "ride_id"),
        ):
            params = yaml.safe_load(
                (
                    Path(__file__).parents[1] / f"experiments/configs/workload-{name}.yaml"
                ).read_text()
            )
            config = experiment.MongoGenerationConfig(**params["generation"])
            self.assertEqual(config.min_predicates, 2)
            self.assertEqual(config.max_predicates, max_predicates)
            self.assertEqual(config.exclude_fields, ["_id", excluded])
            self.assertEqual(config.sort_probability, 0.5)
            self.assertEqual(config.skip_max, 20)
            self.assertEqual(config.limit_min, 5)
            if name == "yelp":
                self.assertEqual(config.field_weights["overrides"]["attributes.*"], 0.05)

    def test_success_exports_matching_manifest(self):
        copied, saved, error = self.run_case()
        self.assertIsNone(error)
        manifest = saved["workload_manifest.json"]
        self.assertEqual(manifest["query_format"], "bondi-workload-v1")
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as directory:
            path = Path(directory) / "workload.jsonl"
            path.write_bytes(copied["workload.jsonl"])
            loaded = Workload.load(path)
        self.assertEqual(loaded[0].pattern_id, 0)
        row = {
            "limit": loaded[0].query.limit,
            "sort": dict(loaded[0].query.sort),
            "projection": loaded[0].query.projection,
        }
        self.assertEqual(row["limit"], 2)
        self.assertEqual(list(row["sort"]), ["stars", "city"])
        self.assertEqual(row["projection"], {"city": 1})
        summary = saved["pattern_summary.json"]
        self.assertEqual(summary[0]["query_ids"], [0])
        self.assertEqual(summary[0]["pattern_id"], 0)
        self.assertEqual(
            set(copied) | set(saved),
            {"workload.jsonl", "workload_manifest.json", "pattern_summary.json"},
        )
        self.assertNotIn("parameters", saved["workload_manifest.json"])


class PatternCallbackTests(unittest.TestCase):
    @staticmethod
    def pattern(*fields):
        query = FindQuery({field: {"$eq": "example"} for field in fields}, collection="business")
        return QueryPattern.from_query(query)

    def test_yelp_attribute_limits_and_odd_quota(self):
        validate = experiment.pattern_validator("yelp.business", 5)
        attribute = self.pattern("attributes.WiFi")
        self.assertFalse(validate(self.pattern("attributes.WiFi", "attributes.Parking"), ()))
        self.assertFalse(validate(self.pattern("attributes", "attributes.WiFi"), ()))
        self.assertTrue(validate(attribute, ()))  # Quota is not based on prefix length.
        self.assertTrue(validate(attribute, (attribute,)))
        self.assertFalse(validate(attribute, (attribute, attribute)))
        self.assertFalse(validate(self.pattern("attributes"), (attribute, attribute)))
        self.assertTrue(validate(self.pattern("city"), (attribute, attribute)))
        self.assertTrue(validate(self.pattern("attributes_other"), (attribute, attribute)))
        self.assertFalse(experiment.pattern_validator("yelp.business", 1)(attribute, ()))

    def test_attributes_across_all_components(self):
        validate = experiment.pattern_validator("yelp.business", 4)

        def shape(**kwargs):
            return QueryPattern.from_query(
                FindQuery({"city": {"$eq": "x"}}, collection="business", **kwargs)
            )

        attribute = shape(sort={"attributes.WiFi": 1})
        self.assertFalse(
            validate(
                shape(sort={"attributes.WiFi": 1}, projection={"attributes.Parking": 1}),
                (),
            )
        )
        self.assertTrue(
            validate(
                shape(sort={"attributes.WiFi": 1}, projection={"attributes.WiFi": 1}),
                (),
            )
        )
        self.assertFalse(validate(shape(projection={"attributes.WiFi": 1}), (attribute, attribute)))

    def test_citibike_endpoint_pairs(self):
        validate = experiment.pattern_validator("citibike.rides2023", 10)
        self.assertFalse(validate(self.pattern("start_station_name", "start_station_id"), ()))
        self.assertFalse(validate(self.pattern("end_station_name", "end_station_id"), ()))
        self.assertTrue(validate(self.pattern("start_station_name", "end_station_id"), ()))
        self.assertTrue(validate(self.pattern("start_station_id", "end_station_name"), ()))
        self.assertTrue(validate(self.pattern("start_station_id"), ()))

    def test_unknown_namespace_has_no_callback(self):
        self.assertIsNone(experiment.pattern_validator("other.collection", 10))


if __name__ == "__main__":
    unittest.main()
