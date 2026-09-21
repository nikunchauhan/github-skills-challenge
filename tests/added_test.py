import json
import runpy
import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import event_consumer  # noqa: E402
from aiops_pipeline import load_data, run_pipeline  # noqa: E402
from anomaly_detector import AnomalyDetector  # noqa: E402
from calculations import area_of_circle, get_nth_fibonacci  # noqa: E402
from event_producer import EventProducer  # noqa: E402
from event_topic import EventTopic  # noqa: E402


def test_calculations_cover_errors_and_iteration():
    with pytest.raises(ValueError):
        area_of_circle(-1)

    with pytest.raises(ValueError):
        get_nth_fibonacci(-1)

    assert get_nth_fibonacci(10) == 55


def test_detector_covers_each_anomaly_reason():
    detector = AnomalyDetector()
    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 501,
        "cpu_percent": 81,
        "memory_percent": 81,
        "log_level": "WARNING",
    }

    event = detector.detect(record)

    assert event["reasons"] == [
        "High response time",
        "High CPU utilization",
        "High memory utilization",
        "Error log detected",
    ]
    assert event["source"] == record


def test_pipeline_loads_data_and_processes_anomalies(tmp_path):
    records = [
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "payment-service",
            "response_time_ms": 100,
            "cpu_percent": 40,
            "memory_percent": 50,
            "log_level": "INFO",
        },
        {
            "timestamp": "2026-09-20T10:01:00",
            "service": "payment-service",
            "response_time_ms": 601,
            "cpu_percent": 40,
            "memory_percent": 50,
            "log_level": "INFO",
        },
    ]
    data_file = tmp_path / "records.json"
    data_file.write_text(json.dumps(records), encoding="utf-8")

    assert load_data(data_file) == records
    result = run_pipeline(data_file)

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert result["events_consumed"] == []


def test_event_producer_rejects_empty_events():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    assert topic.get_messages() == []


def test_event_topic_clear_removes_messages():
    topic = EventTopic("anomaly-events")
    event = {"type": "ANOMALY"}

    topic.publish(event)
    assert topic.get_messages() == [event]

    topic.clear()
    assert topic.get_messages() == []


def test_pipeline_script_entry_point(capsys, monkeypatch):
    pipeline_path = SRC_DIR / "aiops_pipeline.py"

    class ReportingConsumer:
        def __init__(self, topic):
            self.topic = topic

        def consume(self):
            return [{
                "service": "payment-service",
                "timestamp": "2026-09-20T10:05:00",
                "type": "ANOMALY",
                "reasons": ["High response time"],
            }]

    monkeypatch.setattr(event_consumer, "EventConsumer", ReportingConsumer)

    runpy.run_path(str(pipeline_path), run_name="__main__")

    output = capsys.readouterr().out
    assert "AIOps Pipeline Result" in output
    assert "Records processed: 10" in output
    assert "Anomalies detected: 2" in output
    assert "Service: payment-service" in output
    assert "Reasons: High response time" in output