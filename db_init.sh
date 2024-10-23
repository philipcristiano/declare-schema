#initdb --username $PGUSER --pgdata $PGDATA

psql -U declare-schema declare-schema -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
