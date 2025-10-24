#initdb --username $PGUSER --pgdata $PGDATA
createdb -U $PGUSER declare-schema

psql -U declare-schema declare-schema -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
