#!/bin/bash
# ---------------------------------------------------------------------------
# create-sales-db.sh
# ---------------------------------------------------------------------------
# PostgreSQL init script — runs ONCE when the postgres container is first
# created (any *.sh file placed in /docker-entrypoint-initdb.d/ is executed
# automatically by the official postgres image on first start).
#
# Creates:
#   • Database : sales_db
#   • User     : sales_user  (password: sales_password)
#   • Grants all privileges on sales_db to sales_user
# ---------------------------------------------------------------------------

set -e

echo "=== [init-db] Creating sales_db database and sales_user ==="

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- Create the dedicated sales user (skip if it already exists)
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'sales_user') THEN
            CREATE USER sales_user WITH PASSWORD 'sales_password';
            RAISE NOTICE 'Created user: sales_user';
        ELSE
            RAISE NOTICE 'User sales_user already exists — skipping creation.';
        END IF;
    END
    \$\$;

    -- Create the sales database owned by the airflow superuser
    -- (we grant privileges below instead of using OWNER to avoid permission issues)
    SELECT 'CREATE DATABASE sales_db'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'sales_db') \gexec

    -- Grant all privileges on the sales_db database to sales_user
    GRANT ALL PRIVILEGES ON DATABASE sales_db TO sales_user;

EOSQL

# Connect to sales_db to set default schema privileges
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "sales_db" <<-EOSQL

    -- Allow sales_user to create objects in the public schema
    GRANT ALL ON SCHEMA public TO sales_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO sales_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO sales_user;

EOSQL

echo "=== [init-db] sales_db and sales_user created successfully ==="
