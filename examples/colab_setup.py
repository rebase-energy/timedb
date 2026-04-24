"""
Colab setup script — installs PostgreSQL + ClickHouse + timedb.

Downloaded and executed by the first cell of each example notebook.
"""

import os
import subprocess
import sys


def _run(cmd):
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        print(result.stderr[-2000:] or result.stdout[-2000:])
        raise RuntimeError(f"Command failed (exit {result.returncode}): {cmd}")


print("1/4  Installing timedb …")
_run(f'"{sys.executable}" -m pip install -q timedb[pint]')

print("2/4  Configuring and starting PostgreSQL …")
subprocess.run("service postgresql start", shell=True, check=True)
with open("/tmp/init_timedb.sql", "w") as f:
    f.write("ALTER USER postgres PASSWORD 'timedb';\nCREATE DATABASE timedb;\n")
_run('su - postgres -c "psql -f /tmp/init_timedb.sql"')

print("3/4  Installing and starting ClickHouse …")
_run("apt-get -qq install -y apt-transport-https ca-certificates")
_run(
    "curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key"
    " | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg"
)
_run(
    'echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg]'
    ' https://packages.clickhouse.com/deb stable main"'
    " | tee /etc/apt/sources.list.d/clickhouse.list"
)
_run("apt-get -qq update")
_run("apt-get -qq install -y clickhouse-server clickhouse-client")
subprocess.run("service clickhouse-server start", shell=True, check=True)

print("4/4  Setting environment variables …")
os.environ["TIMEDB_PG_DSN"] = "postgresql://postgres:timedb@localhost/timedb"
os.environ["TIMEDB_CH_URL"] = "http://default:@localhost:8123/default"
print("✓  Ready — PostgreSQL + ClickHouse running in this Colab session.")
