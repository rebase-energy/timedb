# Testing Guide for TimeDB

## Overview

The test suite lives in `tests/` and uses [pytest](https://pytest.org/).
TimeDB stores everything in ClickHouse, so ClickHouse is the only service the
tests need.

## Setup

### 1. Install test dependencies

```bash
pip install -e ".[test]"
```

### 2. Point the tests at ClickHouse

```bash
# Bash/Zsh
export TIMEDB_CH_URL='http://default:devpassword@localhost:8123/default'
```

```fish
# Fish
set -x TIMEDB_CH_URL http://default:devpassword@localhost:8123/default
```

A local instance is one command away — see [DEVELOPMENT.md](DEVELOPMENT.md)
for the `local-db/` Docker stack.

**Use a database you don't mind losing.** The live tests create and drop
`series_values` / `run_series`.

> **Live tests skip silently.** Without `TIMEDB_CH_URL`, every test that
> needs ClickHouse is *skipped*, not failed — a green run does not mean the
> suite passed. Always read the summary line (`N passed, M skipped`) and
> treat a non-zero skip count as "the databases weren't reachable".

## Running tests

```bash
pytest                       # everything
pytest -v                    # verbose
pytest tests/test_integration.py            # one file
pytest tests/test_integration.py::test_write_read_roundtrip   # one test
pytest --cov=timedb --cov-report=html       # coverage → htmlcov/index.html
```

## Test structure

| File | Covers |
| :--- | :--- |
| `test_integration.py` | End-to-end write/read against live ClickHouse: bitemporal reads, retention tiers, `skip_unchanged`, relative reads |
| `test_write_validation.py` | Input validation and the `skip_unchanged` comparison keys — mostly offline |
| `test_write_concurrency.py` | Concurrent inserts on one client, and the parallel-insert split for large batches |
| `test_client_sessionless.py` | The clients are sessionless, so independent queries can overlap on one client |
| `test_imports.py` | The public API surface imports cleanly |

There is no `conftest.py`: modules that need ClickHouse skip themselves at
import time with `pytest.skip(..., allow_module_level=True)`, and the few
shared helpers (e.g. a `td` client fixture, per-test `series_id` generation)
are defined locally in the file that uses them. Tests are independent and can
run in any order.

## Writing new tests

1. **Gate on the service** if the test needs ClickHouse — follow the
   module-level skip in `test_integration.py`. Prefer offline tests where the
   logic allows it.
2. **Allocate fresh `series_id`s** per test. TimeDB owns no catalog: the
   caller picks `series_id`, so collisions between tests are your
   responsibility, not the database's.
3. **Use timezone-aware UTC datetimes.** Naive timestamps raise.
4. **Anchor timestamps relative to `now()`, not to a literal date.** The
   `short` tier has a 180-day TTL, so a fixed literal silently rots once it
   ages past the TTL — see the `BASE_VT` comment in `test_integration.py`.
5. **Assert through the public API** (`write` / `read` / `read_relative`)
   rather than raw ClickHouse queries, unless the point of the test is
   storage layout.

## Troubleshooting

**Everything is skipped** — `TIMEDB_CH_URL` is unset, or ClickHouse is not
reachable at that URL. Check with `curl http://localhost:8123/ping`.

**Permission errors** — the ClickHouse user needs CREATE/DROP on tables in
the target database.

**Import errors for pytest** — `pip install -e ".[test]"`.
