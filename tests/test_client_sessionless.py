"""The CH clients must be sessionless — no ClickHouse required.

A ClickHouse session serializes queries (clickhouse-connect raises
"Attempt to execute concurrent queries within the same session"), so every
client ``TimeDBClient`` constructs — main and sidecars — must disable the
auto-generated session id. That is what allows concurrent reads on a single
client (``asyncio.gather`` in energydb, threads in sync callers).
"""

import timedb.client as client_mod
from timedb.client import TimeDBClient


def test_all_clients_are_sessionless(monkeypatch):
    calls: list[dict] = []

    def fake_get_client(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(client_mod.clickhouse_connect, "get_client", fake_get_client)

    td = TimeDBClient(ch_url="http://user:pass@localhost:8123/db")
    td._ensure_aux_clients(2)

    assert len(calls) == 3  # main client + two sidecar insert lanes
    assert all(c["autogenerate_session_id"] is False for c in calls)
