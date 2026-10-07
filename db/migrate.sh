#!/bin/sh
# Connection details come from PGHOST, PGUSER, PGPASSWORD, PGDATABASE and PGSSLMODE.
# init.sql isn't safe to run twice, so skip it if the schema is already there. More than one task
# runs this at startup, so it takes a lock first: the second one waits, then sees the schema.
# Everything runs in one transaction, so a failure part way leaves nothing behind.
set -eu

for i in $(seq 1 30); do
  pg_isready -q && break
  echo "waiting for postgres ($i)"
  sleep 2
done

psql -v ON_ERROR_STOP=1 --single-transaction <<'SQL'
SELECT pg_advisory_xact_lock(7700100);
SELECT to_regclass('public.orders') IS NOT NULL AS has_schema \gset
\if :has_schema
  \echo schema already present
\else
  \i /db/init.sql
  \echo schema created
\endif
SQL
