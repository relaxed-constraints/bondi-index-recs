"""APA ordering, capture cleanup and polling acceptance tests."""

from contextlib import ExitStack, nullcontext
from datetime import datetime
from pathlib import Path
import unittest
from unittest.mock import Mock, MagicMock, patch

from experiments import recommend
from pymongo.errors import OperationFailure


def response(weight=3):
    return {
        "suggestedIndexes": [
            {
                "namespace": "yelp.business",
                "weight": weight,
                "index": [{"z": 1}, {"a": -1}],
                "collation": {"locale": "en"},
            }
        ],
        "shapes": [{"count": 10}],
    }


class RecommendTests(unittest.TestCase):
    def test_server_time_treats_naive_bson_datetime_as_utc(self):
        client = Mock()
        client.admin.command.return_value = {"localTime": datetime(2026, 10, 8, 7, 16, 37, 930000)}
        self.assertEqual(recommend.server_time_ms(client), 1791443797930)

    def test_secondary_indexes_require_explicit_opt_in(self):
        coll = Mock()
        coll.list_indexes.return_value = [{"name": "_id_"}, {"name": "city_1"}]
        with self.assertRaisesRegex(ValueError, "drop_existing_indexes=true"):
            recommend.prepare_indexes(coll, {})
        coll.drop_index.assert_not_called()

    def test_index_definitions_saved_before_drop_and_id_preserved(self):
        indexes = [
            {"name": "_id_", "key": {"_id": 1}},
            {"name": "city_1", "key": {"city": 1}},
        ]
        coll = Mock()
        coll.list_indexes.side_effect = [indexes, indexes[:1]]
        with patch.object(recommend.yanex, "save_artifact") as save:
            coll.drop_index.side_effect = lambda name: save.assert_called_once_with(
                indexes, "indexes_before_drop.json"
            )
            self.assertEqual(
                recommend.prepare_indexes(coll, {"drop_existing_indexes": True}),
                indexes[:1],
            )
        coll.drop_index.assert_called_once_with("city_1")

    def test_profiling_denial_precedes_index_deletion(self):
        client = Mock()
        db = Mock()
        db.name = "yelp"
        coll = Mock()
        client.__getitem__ = Mock(return_value=db)
        db.__getitem__ = Mock(return_value=coll)
        coll.count_documents.return_value = 10
        db.command.side_effect = OperationFailure("not authorized", code=13)
        with (
            patch.object(recommend, "prepare_indexes") as prepare,
            patch.object(recommend.yanex, "save_artifact"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Cannot read profiling.*dbAdmin"):
                recommend.replay(
                    client,
                    {"namespace": "yelp.business", "population": 10},
                    [],
                    {"query_timeout_ms": 100},
                    executor=Mock(),
                )
        prepare.assert_not_called()

    def test_denied_setting_change_does_not_attempt_restore(self):
        db = Mock()
        db.command.side_effect = [
            {"was": 0, "slowms": 100},
            OperationFailure("denied", code=13),
        ]
        with self.assertRaisesRegex(RuntimeError, "Cannot change/restore"):
            with recommend.capture_threshold(db, {"slow_query_ms": 1}):
                self.fail("must not enter")
        self.assertEqual(db.command.call_count, 2)

    def test_order_keys_collation_and_weights(self):
        data = response()
        data["suggestedIndexes"].append(
            {"namespace": "yelp.business", "weight": 5, "index": [{"b": 1}]}
        )
        indexes = recommend.ranked_indexes(data, "yelp.business")
        self.assertEqual(indexes[0]["keys"], [["b", 1]])
        self.assertEqual(indexes[1]["keys"], [["z", 1], ["a", -1]])
        self.assertEqual(indexes[1]["options"], {"collation": {"locale": "en"}})

    def test_other_namespace_and_duplicates_rejected(self):
        with self.assertRaises(ValueError):
            recommend.ranked_indexes(response(), "other.collection")
        data = response()
        data["suggestedIndexes"] *= 2
        with self.assertRaises(ValueError):
            recommend.ranked_indexes(data, "yelp.business")

    def test_replay_failure_restores_logging(self):
        db = Mock()
        db.client.primary = ("host", 27017)
        db.command.side_effect = [
            {"was": 0, "slowms": 100, "sampleRate": 0.5},
            {},
            {"was": 0, "slowms": 1, "sampleRate": 1},
            {},
        ]
        with self.assertRaisesRegex(RuntimeError, "replay error"):
            with recommend.capture_threshold(db, {"slow_query_ms": 1}):
                raise RuntimeError("replay error")
        self.assertEqual(db.command.call_args.kwargs, {"slowms": 100, "sampleRate": 0.5})
        self.assertEqual(db.command.call_args.args, ("profile", 0))

    def test_logging_drift_warns_and_preserves_capture_metadata(self):
        for final in (
            {"slowms": 100, "sampleRate": 1},
            {"slowms": 1, "sampleRate": 0.5},
            {"slowms": 1, "sampleRate": 1, "filter": {"op": "query"}},
            {"slowms": 1, "sampleRate": 1},
        ):
            with self.subTest(final=final):
                db = Mock()
                db.client.primary = ("host", 27017)
                db.command.side_effect = [
                    {"was": 0, "slowms": 50, "sampleRate": 0.5},
                    {},
                    {"was": 0, "slowms": 1, "sampleRate": 1},
                    final,
                    {},
                ]
                with patch("builtins.print") as output:
                    with recommend.capture_threshold(db, {"slow_query_ms": 1}) as effective:
                        pass
                drift = final != {"slowms": 1, "sampleRate": 1}
                self.assertEqual(effective["slowms"], 1)
                self.assertEqual(
                    effective["settings_check"]["status"], "changed" if drift else "endpoints_match"
                )
                self.assertEqual(effective["settings_check"]["final"]["slowms"], final["slowms"])
                self.assertEqual(output.called, drift)
                self.assertEqual(db.command.call_args.kwargs, {"slowms": 50, "sampleRate": 0.5})

    def test_filter_blocks_capture_without_mutation(self):
        db = Mock()
        db.command.return_value = {"was": 0, "filter": {"op": "query"}}
        with self.assertRaisesRegex(RuntimeError, "filter"):
            with recommend.capture_threshold(db, {"slow_query_ms": 1}):
                self.fail("must not enter")
        self.assertEqual(db.command.call_count, 1)

    @staticmethod
    def artifact_loader(manifest, raw):
        def load(name, loader=None):
            if name == "workload_manifest.json":
                return manifest
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as directory:
                path = Path(directory) / name
                path.write_bytes(raw)
                return loader(path)

        return load

    def test_new_format_replayed_with_all_options(self):
        from tempfile import TemporaryDirectory
        from bondi.mongodb import MongoCollectionRef, MongoFindQuery
        from bondi.workload import Workload, WorkloadEntry

        row = {
            "filter": {"city": "x", "date": {"$gte": datetime(2023, 1, 1)}},
            "collection": "business",
            "sort": {"stars": -1, "city": 1},
            "limit": 2,
            "skip": 1,
            "projection": {"city": 1},
        }
        query = MongoFindQuery(
            MongoCollectionRef("yelp", "business"),
            row["filter"],
            sort=tuple(row["sort"].items()),
            skip=1,
            limit=2,
            projection=row["projection"],
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "workload.jsonl"
            Workload([WorkloadEntry(query, pattern_id=0)]).save(path)
            raw = path.read_bytes()
        manifest = {
            "schema_version": 3,
            "query_format": "bondi-workload-v1",
            "namespace": "yelp.business",
            "population": 10,
            "query_count": 1,
        }
        dep = Mock(status="completed", script_path=Path("workload.py"))
        dep.load_artifact.side_effect = self.artifact_loader(manifest, raw)
        with patch.object(recommend.yanex, "get_dependencies", return_value={"x": dep}):
            _, _, queries = recommend.load_workload()
        self.assertIsInstance(queries[0], MongoFindQuery)
        self.assertEqual(queries[0].filter, row["filter"])
        client = MagicMock()
        coll = client["yelp"]["business"]
        coll.count_documents.return_value = 10
        executor = Mock()
        executor.run.return_value = Mock(row_count=2, elapsed_ms=250)
        with (
            patch.object(
                recommend,
                "capture_threshold",
                return_value=nullcontext(
                    {"slowms": 1, "sampleRate": 1, "settings_check": {"status": "changed"}}
                ),
            ),
            patch.object(recommend, "prepare_indexes", return_value=[]),
            patch.object(recommend, "server_time_ms", return_value=1000),
            patch.object(recommend.yanex, "get_experiment_id", return_value="test"),
            patch.object(recommend.yanex, "save_artifact") as save,
        ):
            result = recommend.replay(
                client,
                manifest,
                queries,
                {"query_timeout_ms": 100, "batch_size": 10, "workload_repetitions": 1},
                executor=executor,
            )
        executor.run.assert_called_once_with(
            queries[0],
            query_kwargs={"max_time_ms": 100, "batch_size": 10, "comment": "yanex:test:0:0"},
        )
        coll.find.assert_not_called()
        save.assert_called_once_with(
            [{"repetition": 0, "query_id": 0, "returned_documents": 2, "seconds": 0.25}],
            "replay.jsonl",
        )
        self.assertEqual(result["completed_queries"], 1)
        self.assertEqual(result["settings_check"]["status"], "changed")

    def test_executor_failure_keeps_partial_observations_and_restores_capture(self):
        from contextlib import contextmanager
        from bondi.mongodb import MongoCollectionRef, MongoFindQuery

        restored = []

        @contextmanager
        def capture(*args):
            try:
                yield {"slowms": 1, "sampleRate": 1}
            finally:
                restored.append(True)

        client = MagicMock()
        client["yelp"]["business"].count_documents.return_value = 10
        executor = Mock()
        executor.run.side_effect = [Mock(row_count=2, elapsed_ms=100), RuntimeError("failed")]
        query = MongoFindQuery(MongoCollectionRef("yelp", "business"), {})
        with (
            patch.object(recommend, "capture_threshold", capture),
            patch.object(recommend, "prepare_indexes", return_value=[]),
            patch.object(recommend, "server_time_ms", return_value=1000),
            patch.object(recommend.yanex, "get_experiment_id", return_value="test"),
            patch.object(recommend.yanex, "save_artifact") as save,
        ):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                recommend.replay(
                    client,
                    {"namespace": "yelp.business", "population": 10},
                    [query, query, query],
                    {"query_timeout_ms": 100, "batch_size": 10, "workload_repetitions": 1},
                    executor=executor,
                )
        self.assertEqual(restored, [True])
        self.assertEqual(executor.run.call_count, 2)
        self.assertEqual(executor.run.call_args.kwargs["query_kwargs"]["comment"], "yanex:test:0:1")
        save.assert_called_once_with(
            [{"repetition": 0, "query_id": 0, "returned_documents": 2, "seconds": 0.1}],
            "replay.jsonl",
        )

    def test_legacy_workloads_rejected_before_loading(self):
        dep = Mock(status="completed", script_path=Path("workload.py"))
        dep.load_artifact.return_value = {"schema_version": 2, "query_format": "find-query-v1"}
        with patch.object(recommend.yanex, "get_dependencies", return_value={"x": dep}):
            with self.assertRaisesRegex(ValueError, "legacy formats"):
                recommend.load_workload()
        dep.load_artifact.assert_called_once_with("workload_manifest.json")

    def test_bondi_loader_rejects_wrong_target_and_non_find_queries(self):
        from tempfile import TemporaryDirectory
        from bondi.mongodb import MongoCollectionRef, MongoFindQuery, MongoAggregateQuery
        from bondi.workload import Workload, WorkloadEntry

        for query in (
            MongoFindQuery(MongoCollectionRef("other", "business"), {}),
            MongoAggregateQuery(MongoCollectionRef("yelp", "business"), []),
        ):
            with TemporaryDirectory() as directory:
                path = Path(directory) / "workload.jsonl"
                Workload([WorkloadEntry(query)]).save(path)
                raw = path.read_bytes()
            manifest = {
                "schema_version": 3,
                "query_format": "bondi-workload-v1",
                "namespace": "yelp.business",
                "query_count": 1,
            }
            dep = Mock(status="completed", script_path=Path("workload.py"))
            dep.load_artifact.side_effect = self.artifact_loader(manifest, raw)
            with patch.object(recommend.yanex, "get_dependencies", return_value={"x": dep}):
                with self.assertRaisesRegex(ValueError, "MongoDB find queries"):
                    recommend.load_workload()

    def poll(self, responses, timeout=20, padding=None):
        api = Mock()
        api.suggestions.side_effect = responses
        clock = [0]
        params = {
            "poll_timeout_seconds": timeout,
            "initial_delay_seconds": 1,
            "poll_interval_seconds": 1,
            "minimum_wait_seconds": 0,
            "stable_polls": 2,
        }
        if padding is not None:
            params["window_padding_seconds"] = padding
        with ExitStack() as stack:
            stack.enter_context(patch.object(recommend, "monotonic", side_effect=lambda: clock[0]))
            stack.enter_context(
                patch.object(
                    recommend,
                    "sleep",
                    side_effect=lambda n: clock.__setitem__(0, clock[0] + n),
                )
            )
            stack.enter_context(patch.object(recommend.yanex, "save_artifact"))
            return recommend.poll_indexes(
                api,
                {"namespace": "yelp.business"},
                {"since_ms": 1000, "until_ms": 2000},
                params,
            ), api

    def test_poll_requires_nonempty_stability_with_fixed_window(self):
        (indexes, metadata), api = self.poll(
            [
                {"suggestedIndexes": [], "shapes": []},
                response(2),
                response(3),
                response(3),
            ]
        )
        self.assertEqual(metadata["polls"], 4)
        self.assertEqual(indexes[0]["score"], 3)
        self.assertTrue(
            all(c.args == ("yelp.business", -59000, 62000) for c in api.suggestions.call_args_list)
        )
        self.assertFalse(metadata["observation_coverage_verified"])

    def test_poll_padding_override(self):
        for padding in (0, 120):
            with self.subTest(padding=padding):
                (_, metadata), api = self.poll([response(), response()], padding=padding)
                expected = (1000 - padding * 1000, 2000 + padding * 1000)
                api.suggestions.assert_called_with("yelp.business", *expected)
                self.assertEqual(metadata["request_since_ms"], expected[0])
                self.assertEqual(metadata["request_until_ms"], expected[1])

    def test_empty_polling_fails_instead_of_publishing_no_indexes(self):
        with self.assertRaises(TimeoutError):
            self.poll([{"suggestedIndexes": []}] * 5, timeout=3)


if __name__ == "__main__":
    unittest.main()
