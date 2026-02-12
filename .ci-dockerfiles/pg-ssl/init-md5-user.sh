#!/bin/bash
# Create an MD5-authenticated user for backward-compatibility tests.
# Runs as part of docker-entrypoint-initdb.d during container initialization.

set -e

# Temporarily switch to md5 password encoding so the md5user's stored
# password uses MD5 hashing.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SET password_encryption = 'md5';
    CREATE USER md5user WITH PASSWORD 'md5pass';
    GRANT ALL PRIVILEGES ON DATABASE postgres TO md5user;
    SET password_encryption = 'scram-sha-256';
EOSQL

# Write a custom pg_hba.conf that routes md5user to MD5 auth and everyone
# else to scram-sha-256.
cat > "$PGDATA/pg_hba.conf" <<-'EOF'
# MD5 backward-compatibility user
host all md5user 0.0.0.0/0 md5
host all md5user ::/0 md5
# All other users use SCRAM-SHA-256
host all all 0.0.0.0/0 scram-sha-256
host all all ::/0 scram-sha-256
local all all scram-sha-256
EOF

# Reload configuration
pg_ctl reload -D "$PGDATA"
