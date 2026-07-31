# timedb Tests

The full guide is [TESTING.md](../TESTING.md); this is the short version.

## Prerequisites

ClickHouse is the only service the tests need:

```bash
# Bash/Zsh
export TIMEDB_CH_URL='http://default:devpassword@localhost:8123/default'
```

```fish
# Fish
set -x TIMEDB_CH_URL http://default:devpassword@localhost:8123/default
```

```bash
pip install -e ".[test]"
```

> Without `TIMEDB_CH_URL` the live tests **skip** rather than fail. Check the
> `N passed, M skipped` summary — a non-zero skip count means ClickHouse
> wasn't reachable.

## Running

```bash
pytest tests/                                 # all
pytest tests/ -v                              # verbose
pytest tests/test_integration.py              # one file
pytest tests/ --cov=timedb --cov-report=html  # coverage
```

## Files

- `test_integration.py` — end-to-end write/read against live ClickHouse
- `test_write_validation.py` — input validation, `skip_unchanged` scopes
- `test_write_concurrency.py` — concurrent and split inserts
- `test_client_sessionless.py` — overlapping queries on one client
- `test_imports.py` — public API imports

No `conftest.py`: modules needing ClickHouse skip themselves at import time,
and shared helpers live in the file that uses them.

## Conventions

- Allocate fresh `series_id`s per test — timedb has no catalog, so the caller
  owns identity.
- Timezone-aware UTC datetimes only; naive timestamps raise.
- Anchor timestamps relative to `now()`, never a fixed literal (the `short`
  tier's 180-day TTL would eventually drop the rows).
