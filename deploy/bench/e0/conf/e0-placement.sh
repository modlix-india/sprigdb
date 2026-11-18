# Sourced by sprig-up.sh for the E0 arm, after the tenant and its user exist
# and before the benchmark's databases are created: puts those databases on
# the storage engine. Functions and variables of sprig-up.sh are in scope.
for db in sbtest e0diff e0crash redis; do
    python3 -I /opt/e0/lib/mysqlwire.py --host 127.0.0.1 --port "$PSQL" --user bench.app --password "$E0_APP_PW" --retry 90 \
        -e "CREATE DATABASE IF NOT EXISTS $db; ALTER DATABASE $db SPRIG SET (storage.placement = 'engine')" ||
        die "placing $db on the engine failed"
done
say "sbtest, e0diff, e0crash and redis (the tenant's Redis databases) placed on the engine"
