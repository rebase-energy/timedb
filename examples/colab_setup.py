"""
Colab setup script — installs PostgreSQL + TimescaleDB + timedb.

Downloaded and executed by the first cell of each example notebook.
"""
import sys, subprocess, os


def _run(cmd):
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        print(result.stderr[-2000:] or result.stdout[-2000:])
        raise RuntimeError(f"Command failed (exit {result.returncode}): {cmd}")


print("1/4  Installing timedb …")
_run(f'"{sys.executable}" -m pip install -q timedb[pint]')

print("2/4  Installing PostgreSQL + TimescaleDB …")
_run(
    'echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ '
    '$(lsb_release -cs) main" | tee /etc/apt/sources.list.d/timescaledb.list'
)
_run('wget -qO - https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add -')
_run('apt-get -qq update')
_run('apt-get -qq install -y timescaledb-2-postgresql-14')

print("3/4  Configuring and starting PostgreSQL …")
_run('timescaledb-tune --quiet --yes')
subprocess.run('service postgresql start', shell=True, check=True)

print("4/4  Creating database …")
with open('/tmp/init_timedb.sql', 'w') as f:
    f.write("ALTER USER postgres PASSWORD 'timedb';\nCREATE DATABASE timedb;\n")
_run('su - postgres -c "psql -f /tmp/init_timedb.sql"')
_run('su - postgres -c "psql -d timedb -c \'CREATE EXTENSION timescaledb;\'"')

os.environ['DATABASE_URL'] = 'postgresql://postgres:timedb@localhost/timedb'
print("✓  Ready — PostgreSQL + TimescaleDB running in this Colab session.")
