import os
import json
from datetime import datetime, timedelta, timezone
import pytest
from unittest.mock import MagicMock, patch

import main

# Helper blob class for testing
class DummyBlob:
    def __init__(self, name, updated, content):
        self.name = name
        self.updated = updated
        self._content = content

    def download_as_text(self):
        return self._content


def test_list_old_json_blobs(monkeypatch):
    # Prepare dummy blobs: one old, one new
    now = datetime(2025, 5, 8, 12, 0, 0, tzinfo=timezone.utc)
    old_blob = DummyBlob("prefix/old.json", now - timedelta(hours=25), "{}")
    new_blob = DummyBlob("prefix/new.json", now - timedelta(hours=1), "{}")
    bucket = MagicMock()
    bucket.list_blobs.return_value = [old_blob, new_blob]

    # Monkeypatch datetime.now in main
    class FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(main, 'datetime', FakeDateTime)

    result = main.list_old_json_blobs(bucket, prefix="prefix/", min_age_hours=24)
    assert old_blob in result
    assert new_blob not in result
    assert len(result) == 1


def test_aggregate_events():
    # Create blobs with event JSON
    data1 = json.dumps({"timestamp": "2025-05-07T10:15:00", "type": "click"})
    data2 = json.dumps({"timestamp": "2025-05-07T10:45:00", "type": "view"})
    data3 = json.dumps({"timestamp": "2025-05-07T11:00:00", "type": "click"})
    blobs = [DummyBlob("a.json", None, data1), DummyBlob("b.json", None, data2), DummyBlob("c.json", None, data3)]
    summary = main.aggregate_events(blobs)
    lines = summary.split("\n")
    parsed = [json.loads(line) for line in lines]
    row10 = next(r for r in parsed if r["hour"] == "2025-05-07T10:00:00")
    assert row10.get("click_event_count", 0) == 1
    assert row10.get("view_event_count", 0) == 1
    row11 = next(r for r in parsed if r["hour"] == "2025-05-07T11:00:00")
    assert row11.get("click_event_count", 0) == 1
    assert row11.get("view_event_count", 0) == 0


def test_write_summary_to_gcs():
    uploaded = {}
    class FakeBlob:
        def __init__(self, path):
            self.path = path
        def upload_from_string(self, data, content_type):
            uploaded['data'] = data
            uploaded['content_type'] = content_type

    bucket = MagicMock()
    bucket.name = "test-bucket"
    bucket.blob.side_effect = lambda path: FakeBlob(path)

    date = datetime(2025, 5, 7, tzinfo=timezone.utc)
    summary_data = "{\"hour\": \"2025-05-07T10:00:00\", \"click_event_count\": 2}"
    uri = main.write_summary_to_gcs(bucket, summary_data, date)

    assert uri == "gs://test-bucket/summary/2025/05/07/hourly_summary_2025-05-07.json"
    assert uploaded['data'] == summary_data
    assert uploaded['content_type'] == "application/json"


def test_load_to_bigquery_staging(monkeypatch):
    # Setup fake load job
    fake_job = MagicMock()
    fake_job.job_id = "job_123"
    fake_job.result = MagicMock()

    calls = {}
    def fake_load(uri, table_ref, job_config):
        calls['uri'] = uri
        calls['table_ref'] = table_ref
        calls['job_config'] = job_config
        return fake_job

    monkeypatch.setattr(main.bigquery_client, 'load_table_from_uri', fake_load)

    # Provide a TableReference-like object with .table_id to satisfy logger
    table_ref = MagicMock()
    table_ref.table_id = "dataset.table"

    uri = "gs://bucket/path.json"
    main.load_to_bigquery_staging(uri, table_ref)

    assert calls['uri'] == uri
    assert calls['table_ref'] == table_ref
    fake_job.result.assert_called_once()


def test_merge_staging_to_main(monkeypatch):
    fake_query_job = MagicMock()
    fake_query_job.result = MagicMock()

    captured = {}
    def fake_query(q):
        captured['query'] = q
        return fake_query_job

    monkeypatch.setattr(main.bigquery_client, 'query', fake_query)

    main.merge_staging_to_main('ds', 'stg', 'prd')
    q = captured['query']
    assert 'MERGE' in q
    assert '`ds.prd`' in q
    assert '`ds.stg`' in q
    fake_query_job.result.assert_called_once()


def test_main_no_blobs(monkeypatch):
    monkeypatch.setattr(main, 'list_old_json_blobs', lambda bucket, prefix, min_age_hours: [])
    result = main.main()
    assert result == ("No data to process", 200)


def test_main_with_blobs(monkeypatch):
    monkeypatch.setattr(main, 'list_old_json_blobs', lambda bucket, prefix, min_age_hours: [DummyBlob("x.json", None, json.dumps({"timestamp": "2025-05-07T00:00:00", "type": "click"}))])
    monkeypatch.setattr(main, 'aggregate_events', lambda blobs: "summary_data")
    monkeypatch.setattr(main, 'write_summary_to_gcs', lambda bucket, summary, date: "gs://uri")
    monkeypatch.setattr(main, 'load_to_bigquery_staging', lambda uri, table_ref: None)
    monkeypatch.setattr(main, 'merge_staging_to_main', lambda d, s, p: None)

    result = main.main()
    assert result is None
