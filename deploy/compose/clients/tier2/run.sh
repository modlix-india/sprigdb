#!/usr/bin/env bash
# Tier 2 clients: frameworks run their migrations, drift checks and, where
# the package ships them, their own database tests against one endpoint, a
# MySQL 8.4 server or a gateway's 3306, so the two runs can be compared.
#
#   run.sh <framework> <host> <port> <user> <password> <label>
#
# <framework>  on a laptop: django (Django 5.2's own tests: introspection,
#              inspectdb, backends, schema; the sdist ships them), prisma
#              (Prisma 6: schema from the data model, drift check,
#              introspection, a second migration), migrations (Flyway 11
#              and Liquibase 4: migrate, validate, clean, update, status,
#              rollback), gorm, sequelize and rails (official golang,
#              node and ruby images). typeorm (its compile needs more than
#              768 MB: CI's 4 GB, or compile on the host and run the tests
#              in node:22), laravel, and django in full
#              (SPRIG_TIER2_DJANGO_MODULES set empty) run in CI, and ran on
#              a laptop in parts on 17 Nov 2026.
#              expected/<framework>.txt lists the tests allowed to pass on
#              MySQL and not through the gateway (ci/tier2.sh reads it).
# <label>      a name for the results: reference, gateway, ...
#
# Results go to $SPRIG_TIER2_RESULTS/<framework>/<label>/ (default
# clients/results/tier2): the framework's own output and steps.tsv, one
# line per step or test (name, pass/fail/skip, detail). Each run uses a
# container of SPRIG_TIER2_MEMORY (768m) and the clients' package caches.
# Downloads come from the language registries (PyPI, npm, Maven Central),
# pinned below. Django's suite needs a login that can create databases.
# <host> 127.0.0.1 is the host's loopback, reached from the containers.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
FW=$1 HOST=$2 PORT=$3 USER=$4 PASS=$5 LABEL=$6
CACHE=${SPRIG_TIER2_CACHE:-$HERE/../.src/.cache}
OUT=${SPRIG_TIER2_RESULTS:-$HERE/../results/tier2}/$FW/$LABEL
MEM=${SPRIG_TIER2_MEMORY:-768m}
NAME=${SPRIG_TIER2_PREFIX:-sprig-tier2}-$FW-$$
DJANGO=5.2.8 PYMYSQL=1.1.2
PY_IMAGE=python:3.12.12-slim-bookworm
MAVEN_IMAGE=maven:3.9.11-eclipse-temurin-21
PRISMA_IMAGE=sprigdb-conn-prisma:dev
mkdir -p "$OUT" "$CACHE/pip" "$CACHE/m2"
rm -f "$OUT"/*
# A server on the host's loopback (CI's reference and gateway): the
# containers share the host's network on Linux; Docker Desktop reaches the
# host as host.docker.internal.
NET=()
if [[ $HOST == 127.0.0.1 || $HOST == localhost ]]; then
  if [[ $(uname -s) == Linux ]]; then NET=(--network host); else HOST=host.docker.internal; fi
fi
case $FW in
  django)
    MODULES=${SPRIG_TIER2_DJANGO_MODULES-introspection inspectdb backends schema}
    docker run --rm --name "$NAME" --memory "$MEM" ${NET[@]+"${NET[@]}"} -v "$CACHE/pip:/root/.cache/pip" -v "$HERE/django:/t:ro" \
      -e T2_HOST="$HOST" -e T2_PORT="$PORT" -e T2_USER="$USER" -e T2_PASSWORD="$PASS" "$PY_IMAGE" sh -c "
        pip install -q --disable-pip-version-check Django==$DJANGO PyMySQL==$PYMYSQL 'asgiref>=3.8.1' 'sqlparse>=0.5' &&
        pip download -q --disable-pip-version-check --no-deps --no-binary :all: Django==$DJANGO -d /tmp/dl &&
        tar xzf /tmp/dl/[dD]jango-$DJANGO.tar.gz -C /tmp && cd /tmp/[dD]jango-$DJANGO/tests &&
        cp /t/test_sprig.py . && python -W ignore runtests.py --settings=test_sprig --parallel 1 --noinput -v 2 $MODULES 2>&1" \
      > "$OUT/django.log"
    # `test_x (module.Class.test_x) ... ok | FAIL | ERROR | skipped '...'`, the
    # outcome on the same line or, after a docstring, on the next one.
    python3 - "$OUT/django.log" > "$OUT/steps.tsv" <<'PY'
import re, sys
seen, name = {}, None
st_map = {"ok": "pass", "FAIL": "fail", "ERROR": "fail", "skipped": "skip", "expected failure": "pass", "unexpected success": "fail"}
for line in open(sys.argv[1], errors="replace"):
    line = line.rstrip()
    m = re.match(r"^(test\w*) \(([\w.]+)\)(.*)$", line)
    if m:
        name = m.group(2) if m.group(2).endswith("." + m.group(1)) else f"{m.group(2)}.{m.group(1)}"
        rest = m.group(3)
    else:
        rest = line
    st = re.search(r"\.\.\. (ok|FAIL|ERROR|skipped|expected failure|unexpected success)\b", rest)
    if st and name:
        seen[name] = st_map[st.group(1)]
        name = None
    elif not m:
        name = None
for k, v in sorted(seen.items()):
    print(f"{k}\t{v}")
PY
    ;;
  prisma)
    docker run --rm --name "$NAME" --memory "$MEM" ${NET[@]+"${NET[@]}"} --entrypoint sh -v "$HERE/prisma:/t:ro" \
      -e DATABASE_URL="mysql://$USER:$PASS@$HOST:$PORT/tiertwo_prisma" "$PRISMA_IMAGE" /t/run.sh > "$OUT/prisma.log" 2>&1
    grep '^STEP' "$OUT/prisma.log" | cut -f2- > "$OUT/steps.tsv"
    ;;
  migrations)
    docker run --rm --name "$NAME" --memory "$MEM" ${NET[@]+"${NET[@]}"} -v "$CACHE/m2:/root/.m2" -v "$HERE/migrations:/src:ro" \
      -e MAVEN_OPTS="-Xmx384m -XX:+UseSerialGC" -e DB_USER="$USER" -e DB_PASSWORD="$PASS" \
      -e DB_URL="jdbc:mysql://$HOST:$PORT/tiertwo_mig?sslMode=DISABLED&allowPublicKeyRetrieval=true" "$MAVEN_IMAGE" \
      sh -c 'cp -r /src /work && sh /work/run.sh' > "$OUT/migrations.log" 2>&1
    grep '^STEP' "$OUT/migrations.log" | cut -f2- > "$OUT/steps.tsv"
    ;;
  # The frameworks' own suites below need more than the laptop holds or
  # images it doesn't keep: CI runs them (.github/workflows/heavy-sql.yml,
  # job tier2). Defined, not run yet: expect fixes on the first run. Each
  # prints `STEP <test> pass|fail|skip` lines through a small reporter, or
  # its runner's own summary is parsed.
  gorm)
    # GORM v1.31.0's tests module (gorm.io/gorm/tests) with its MySQL driver.
    docker run --rm --name "$NAME" --memory "$MEM" ${NET[@]+"${NET[@]}"} golang:1.25-bookworm sh -c "
        git clone -q --depth 1 --branch v1.31.0 https://github.com/go-gorm/gorm.git /g && cd /g/tests &&
        go mod tidy >/dev/null 2>&1 &&
        GORM_DIALECT=mysql GORM_DSN='$USER:$PASS@tcp($HOST:$PORT)/tiertwo_gorm?charset=utf8mb4&parseTime=True&loc=Local' \
        go test -p 1 -count=1 -v ./... 2>&1" > "$OUT/gorm.log"
    sed -n -E 's/^--- (PASS|FAIL|SKIP): ([^ ]+).*/\2\t\1/p' "$OUT/gorm.log" |
      sed -e 's/\tPASS$/\tpass/' -e 's/\tFAIL$/\tfail/' -e 's/\tSKIP$/\tskip/' > "$OUT/steps.tsv"
    ;;
  sequelize)
    # Sequelize v6.37.7's integration tests for the mysql dialect, all of
    # them. Its "supports schemas" tests were left out for a while (a DDL
    # job left pending on `work.Users`, 39071 for 15 minutes); now an
    # abandoned DDL job is resolved within seconds, and they pass (12 Nov
    # 2026: 1,803 of 1,808, the rest listed or flaky).
    docker run --rm --name "$NAME" --memory "$MEM" ${NET[@]+"${NET[@]}"} -e DIALECT=mysql -e SEQ_MYSQL_DB=tiertwo_sequelize \
      -e SEQ_MYSQL_USER="$USER" -e SEQ_MYSQL_PW="$PASS" -e SEQ_MYSQL_HOST="$HOST" -e SEQ_MYSQL_PORT="$PORT" \
      node:22-bookworm-slim sh -c "
        apt-get -qq update && apt-get -qq install -y git >/dev/null &&
        git clone -q --depth 1 --branch v6.37.7 https://github.com/sequelize/sequelize.git /s && cd /s &&
        npm install --no-audit --no-fund --ignore-scripts >/dev/null 2>&1 && node ./build.js >/dev/null 2>&1 &&
        npx mocha -r ./test/registerEsbuild --globals setImmediate,clearImmediate --exit --check-leaks --colors=false -t 30000 \
          --reporter tap 'test/integration/**/*.test.js' 2>&1" > "$OUT/sequelize.log"
    sed -n -E 's/^(ok|not ok) [0-9]+ (.*)$/\2\t\1/p' "$OUT/sequelize.log" |
      sed -e 's/\tok$/\tpass/' -e 's/\tnot ok$/\tfail/' > "$OUT/steps.tsv"
    ;;
  typeorm)
    # TypeORM 0.3.27's own suite with only the mysql connection enabled,
    # through mysql2 (the default `mysql` package can't log in with
    # caching_sha2_password). --no-config: the repository's .mocharc also
    # loads every test file, and some need sqlite3's native module, which
    # --ignore-scripts doesn't build.
    docker run --rm --name "$NAME" --memory "$MEM" ${NET[@]+"${NET[@]}"} node:22-bookworm-slim sh -c "
        apt-get -qq update && apt-get -qq install -y git >/dev/null &&
        git clone -q --depth 1 --branch 0.3.27 https://github.com/typeorm/typeorm.git /t && cd /t &&
        printf '[{\"skip\":false,\"name\":\"mysql\",\"type\":\"mysql\",\"connectorPackage\":\"mysql2\",\"host\":\"$HOST\",\"port\":$PORT,\"username\":\"$USER\",\"password\":\"$PASS\",\"database\":\"tiertwo_typeorm\",\"logging\":false}]' > ormconfig.json &&
        npm ci --no-audit --no-fund --ignore-scripts >/dev/null 2>&1 && npm run compile >/dev/null 2>&1 &&
        npx mocha --no-config --file build/compiled/test/utils/test-setup.js --reporter tap --timeout 60000 --exit 'build/compiled/test/**/*.js' 2>&1" > "$OUT/typeorm.log"
    sed -n -E 's/^(ok|not ok) [0-9]+ (.*)$/\2\t\1/p' "$OUT/typeorm.log" |
      sed -e 's/\tok$/\tpass/' -e 's/\tnot ok$/\tfail/' > "$OUT/steps.tsv"
    ;;
  rails)
    # Rails 8.0.3's Active Record tests on the mysql2 adapter (rails/run.sh).
    docker run --rm --name "$NAME" --memory "$MEM" ${NET[@]+"${NET[@]}"} -v "$HERE/rails:/t:ro" -e MYSQL_HOST="$HOST" -e MYSQL_PORT="$PORT" \
      -e T2_USER="$USER" -e T2_PASSWORD="$PASS" ruby:3.4-bookworm sh /t/run.sh > "$OUT/rails.log" 2>&1
    sed -n -E 's/^([A-Za-z0-9_:]+#test_[^ ]+) = [0-9.]+ s = ([.FES])$/\1\t\2/p' "$OUT/rails.log" |
      sed -e 's/\t[.]$/\tpass/' -e 's/\t[FE]$/\tfail/' -e 's/\tS$/\tskip/' > "$OUT/steps.tsv"
    ;;
  laravel)
    # Laravel framework 12.36.1's database integration tests. The composer
    # image fetches the framework and its packages into the cache; it has no
    # pdo_mysql, so the tests run in PHP's own image with pdo_mysql built
    # (the Debian image ships what docker-php-ext-install needs). The
    # framework's repository has no composer.lock; Symfony Console 7.4
    # registers commands in a way 12.36.1 predates (every test fails with
    # "make() on null"), so it stays on 7.3.
    src=$CACHE/laravel-v12.36.1
    if [[ ! -f $src/vendor/autoload.php ]]; then
      rm -rf "$src" && mkdir -p "$src"
      docker run --rm --name "$NAME-get" --memory "$MEM" -v "$src:/l" composer:2.8.12 sh -c "
          git clone -q --depth 1 --branch v12.36.1 https://github.com/laravel/framework.git /l &&
          cd /l && composer update -q --no-interaction --ignore-platform-reqs -W --with 'symfony/console:7.3.*'"
    fi
    docker run --rm --name "$NAME" --memory "$MEM" ${NET[@]+"${NET[@]}"} -v "$src:/l" -w /l -e DB_CONNECTION=mysql -e DB_HOST="$HOST" -e DB_PORT="$PORT" \
      -e DB_DATABASE=tiertwo_laravel -e DB_USERNAME="$USER" -e DB_PASSWORD="$PASS" php:8.4.14-cli sh -c "
        docker-php-ext-install pdo_mysql >/dev/null 2>&1 &&
        php -d memory_limit=512M vendor/bin/phpunit --log-junit /tmp/j.xml tests/Integration/Database >/dev/null 2>&1; cat /tmp/j.xml" > "$OUT/laravel.xml"
    python3 - "$OUT/laravel.xml" > "$OUT/steps.tsv" <<'PY'
import sys, xml.etree.ElementTree as ET
for tc in ET.parse(sys.argv[1]).iter("testcase"):
    st = "fail" if tc.find("failure") is not None or tc.find("error") is not None else "skip" if tc.find("skipped") is not None else "pass"
    print(f"{tc.get('class')}.{tc.get('name')}\t{st}")
PY
    ;;
  *) echo "tier2: unknown framework $FW" >&2; exit 2 ;;
esac
p=$(grep -c $'\tpass' "$OUT/steps.tsv"); f=$(grep -c $'\tfail' "$OUT/steps.tsv"); s=$(grep -c $'\tskip' "$OUT/steps.tsv")
echo "$FW $LABEL: $p pass, $f fail, $s skip" | tee "$OUT/summary.txt"
