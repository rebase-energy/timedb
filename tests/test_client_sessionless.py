"""The CH client must be sessionless — no ClickHouse required.

A ClickHouse session serializes queries (clickhouse-connect raises
"Attempt to execute concurrent queries within the same session"), so the
client ``TimeDBClient`` builds must disable the auto-generated session id.
That is what lets its queries overlap on one client — concurrent reads
(``asyncio.gather`` in energydb, threads in sync callers) and the write path's
concurrent ``series_values`` / ``run_series`` insert lanes alike.
"""

import timedb.client as client_mod
from timedb.client import TimeDBClient


def test_client_is_sessionless(monkeypatch):
    calls: list[dict] = []

    def fake_get_client(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(client_mod.clickhouse_connect, "get_client", fake_get_client)

    td = TimeDBClient(ch_url="http://user:pass@localhost:8123/db")
    # Any further client the class builds must carry the same setting.
    td._new_client()

    assert len(calls) == 2  # constructor + the explicit _new_client()
    assert all(c["autogenerate_session_id"] is False for c in calls)
