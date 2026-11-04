from __future__ import annotations

from datetime import datetime
import importlib
from pathlib import Path
import sys
import unittest
import uuid

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "s5"
sys.path.insert(0, str(COMPONENT_ROOT))

try:
    composition = importlib.import_module("src.composition")
    bus_module = importlib.import_module("src.infrastructure.b3_bus.b3_event_consumer")
    repository_module = importlib.import_module(
        "src.infrastructure.database.duckdb_time_series_repository"
    )
except (ImportError, ModuleNotFoundError) as import_error:
    S5_IMPORT_ERROR = import_error
else:
    S5_IMPORT_ERROR = None


class S5IntegrationTests(unittest.TestCase):
    def test_zone_occupancy_produces_metrics_and_query_reply(self):
        if S5_IMPORT_ERROR is not None:
            self.skipTest(f"Dependência do S5 ausente: {S5_IMPORT_ERROR}")

        bus = bus_module.InMemoryB3EventConsumer()
        repository = repository_module.DuckDBTimeSeriesRepository(":memory:")
        ticks = iter((datetime(2026, 9, 23, 14, 29, 30), datetime(2026, 9, 23, 15, 0, 3)))
        service = composition.build_s5(
            bus,
            bus,
            repository,
            clock=lambda: next(ticks),
            granularities=("1h",),
        )
        published: dict[str, list[dict]] = {}
        for topic in ("ods.s5.zone_metrics", "ods.s5.metrics.reply"):
            bus.subscribe(topic, lambda message, topic=topic: published.setdefault(topic, []).append(message))
        bus.start()
        service.worker.run()

        session_id = str(uuid.uuid4())
        bus.publish(
            "ods.s1.zone_occupancy",
            {
                "message_type": "event",
                "message_id": str(uuid.uuid4()),
                "schema": "ods.s1.zone_occupancy",
                "schema_version": "1.0",
                "producer": "S1",
                "published_at": "2026-09-23T14:29:59.460Z",
                "captured_at": "2026-09-23T14:29:59.200Z",
                "source_id": "cam0",
                "session_id": session_id,
                "sequence": 1205,
                "artifact_version": "zones-a8-2",
                "payload": {
                    "zone_id": "vaga-12",
                    "count": 1,
                    "count_by_class": {"vehicle": 1},
                    "max_occupancy": 1,
                    "track_ids": ["track-1"],
                },
            },
        )
        bus.publish(
            "ods.s1.zone_transition",
            {
                "message_type": "event",
                "message_id": str(uuid.uuid4()),
                "schema": "ods.s1.zone_transition",
                "schema_version": "1.0",
                "producer": "S1",
                "published_at": "2026-09-23T14:29:59.450Z",
                "captured_at": "2026-09-23T14:29:59.200Z",
                "source_id": "cam0",
                "session_id": session_id,
                "sequence": 1204,
                "artifact_version": "zones-a8-2",
                "payload": {
                    "track_id": "track-1",
                    "class": "vehicle",
                    "zone_id": "vaga-12",
                    "transition": "EXIT",
                    "entered_at": "2026-09-23T14:28:59.200Z",
                    "dwell_s": 60,
                },
            },
        )
        service.worker.run()

        query_id = str(uuid.uuid4())
        bus.publish(
            "ods.s5.metrics.query",
            {
                "message_type": "query",
                "message_id": query_id,
                "schema": "ods.s5.metrics.query",
                "schema_version": "1.0",
                "producer": "A8",
                "target": "S5",
                "published_at": "2026-09-23T15:00:04.000Z",
                "payload": {
                    "metric": "ZONE",
                    "zone_id": "vaga-12",
                    "class": "vehicle",
                    "period_start_at": "2026-09-23T14:00:00.000Z",
                    "period_end_at": "2026-09-23T15:00:00.000Z",
                    "granularity": "1h",
                },
            },
        )

        self.assertIn("ods.s5.zone_metrics", published)
        self.assertIn("ods.s5.metrics.reply", published)
        metric = published["ods.s5.zone_metrics"][0]
        metric_errors = validate_message(
            metric,
            schema="ods.s5.zone_metrics",
            message_type="event",
            producer="S5",
            payload_fields=(
                "zone_id",
                "window_start_at",
                "window_end_at",
                "granularity",
                "total_count",
                "occupancy_peak",
                "occupancy_avg",
                "dwell_avg_s",
            ),
        )
        self.assertFalse(metric_errors, f"Métrica do S5 inválida: {'; '.join(metric_errors)}")
        reply = published["ods.s5.metrics.reply"][0]
        reply_errors = validate_message(
            reply,
            schema="ods.s5.metrics.reply",
            message_type="reply",
            producer="S5",
            payload_fields=("status", "series"),
            context_fields=("correlation_id",),
        )
        self.assertFalse(reply_errors, f"Reply do S5 inválida: {'; '.join(reply_errors)}")
        self.assertEqual(reply["correlation_id"], query_id)
        self.assertEqual(reply["payload"]["status"], "OK")
        bus.stop()
