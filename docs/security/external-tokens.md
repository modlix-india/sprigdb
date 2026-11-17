---
title: Logins with identity provider tokens
audience: [user, dba, operator]
sidebar:
  order: 7
---

# Logins with identity provider tokens

Applications and people that already sign in to an OpenID Connect (OIDC) identity provider, or to any service that issues signed JWTs, can log in to SprigDB with that token instead of a SprigDB password. A tenant registers the issuers it trusts. SprigDB checks each token's signature against the issuer's published keys (its JWKS), maps the token's claims to an account, and hands the claims to row-level security policies through `SPRIG_CLAIM`.

| Protocol | How the token is sent | Which account | Account created on first login |
|---|---|---|---|
| MySQL and MariaDB | As the password, over TLS | The login name (`acme.billing`) | No |
| Redis | As the password of `AUTH` | The login name | No |
| HTTP: REST and gRPC | `Authorization: Bearer <token>` | The token's user mapping | When the issuer has `jit_users` |
| HTTP, as a Basic password | `Authorization: Basic base64(<login name>:<token>)` | The login name | No |
| MongoDB | `MONGODB-OIDC` | The token's user mapping | When the issuer has `jit_users` |

Bolt, Kafka and AMQP don't take tokens.

Setting it up takes three steps:

1. An operator registers the issuer for the tenant, with claim mappings that turn a token into an account name.
2. The accounts that may log in with tokens get the `token` login method, or the issuer creates them on first use.
3. Clients send the token.

## Register an issuer

Issuers belong to a tenant. Operators register them through the Admin API, with `sprigctl` or directly. An issuer is one JSON document:

```json
{
  "issuer": "https://login.example.com/realms/acme",
  "audiences": ["sprigdb"],
  "algorithms": ["RS256", "ES256"],
  "protocols": ["mysql", "http", "mongodb"],
  "client_id": "sprigdb",
  "request_scopes": ["openid", "profile"],
  "jit_users": false,
  "max_lifetime_s": 3600,
  "mappings": [
    {"priority": 10, "kind": "user", "claim": "preferred_username", "target": "{preferred_username}"}
  ]
}
```

| Field | Default | Meaning |
|---|---|---|
| `issuer` | required | The exact `iss` value of the issuer's tokens, 1 to 512 characters. It identifies the issuer: writing a document with the same `issuer` replaces the stored one |
| `kind` | `oidc` | `oidc`, or `end_user_layer` for an application's own token service, whose tokens belong to its end users. Both are verified the same way; see [End-user layers](#end-user-layers) for what differs |
| `jwks_url` | none | The URL of the issuer's key set, up to 512 characters. Without it, SprigDB finds the key set by OIDC discovery from `issuer` (see [Key sets](#key-sets)) |
| `audiences` | required | The `aud` values accepted; at least one, none empty |
| `algorithms` | `["RS256"]` | The signature algorithms accepted: any of `RS256`, `RS384`, `RS512`, `PS256`, `PS384`, `PS512`, `ES256`, `ES384` and `EdDSA` |
| `protocols` | all four | Where the issuer's tokens log in: `mysql` (MySQL and MariaDB), `redis`, `http` (REST and gRPC) and `mongodb`; at least one |
| `client_id` | none | For `MONGODB-OIDC`'s human flow: the client id drivers ask the identity provider for; up to 255 characters |
| `request_scopes` | none | For `MONGODB-OIDC`'s human flow: the scopes drivers request |
| `jit_users` | `false` | Create the account a user mapping names when it doesn't exist, over HTTP and MongoDB |
| `max_lifetime_s` | 86400 | The longest token lifetime accepted, in seconds (`exp` minus `iat`); above 0 |
| `active` | `true` | `false` disables the issuer: its tokens are refused |
| `mappings` | none | The [claim mappings](#claim-mappings). They replace the issuer's previous mappings |

A document is refused with `ER_SPRIG_CONFIG_INVALID` (39998), naming the first problem, when a required field is missing or out of range, an algorithm isn't in the list above (`none` and the HMAC algorithms such as `HS256` are never accepted), a URL isn't `https://` (plain `http://` is accepted only for a loopback address, such as a local identity provider), or a mapping is malformed.

### With sprigctl

```sh
sprigctl tenant issuer put acme --spec issuer.json     # create or replace; --spec - reads standard input
sprigctl tenant issuer list acme
sprigctl tenant issuer disable acme https://login.example.com/realms/acme
```

```text
issuer 1 (https://login.example.com/realms/acme) of tenant acme: active, 1 mappings, auth version 812
```

`list` shows each issuer with its state, key-set URL (or `discovery`), audiences, protocols and number of mappings; `-o json` prints the full documents, mappings included. `disable` writes the stored issuer back with `"active": false`; to enable it again, `put` it with `"active": true`. To change one field, take the issuer from `list -o json`, edit it and `put` it back.

### With the Admin API

| Call | Does |
|---|---|
| `GET /admin/v1/tenants/{name}/issuers` | The tenant's issuers, with their mappings |
| `POST /admin/v1/tenants/{name}/issuers` | Creates or replaces an issuer (the body is the document above) |
| `POST /admin/v1/tenants/{name}/users:setTokenLogin` | Adds or removes an account's `token` method (see below) |

```sh
curl -s https://ctl.fra1.internal:7480/admin/v1/tenants/acme/issuers \
  --cacert /etc/sprigdb/tls/ca.pem -H "Authorization: Bearer $(cat ~/.sprig/admin-token)" \
  -H 'content-type: application/json' -d @issuer.json
```

```json
{"tenant": "acme", "issuer_id": 1, "issuer": "https://login.example.com/realms/acme", "active": true, "mappings": 1, "auth_version": 812}
```

They need an operator credential that covers tenants, as the other tenant routes do: a scoped token needs `tenants:read` to list and `tenants:write` to change. An unknown or inactive tenant fails with 39072. Each write is one change to the tenant's accounts, so every gateway follows it within moments, and it is audited as `admin.issuer_changed` or `admin.token_login_changed`.

## Claim mappings

A mapping turns a verified token's claims into the name of the account it logs in as, or into a role for its session.

| Field | Default | Meaning |
|---|---|---|
| `priority` | required | Mappings are tried in ascending priority |
| `kind` | required | `user` names the account; `role` gives the session a role (see [Roles from tokens](#roles-from-tokens)) |
| `claim` | required | The claim tested: a top-level claim name, or a dotted path into objects (`realm_access.roles`) when no top-level claim has that name; 1 to 255 characters |
| `match_kind` | `any` | `any`: the claim is present and not null (an array must not be empty). `equals`: the claim, or one element of an array claim, equals `match_value`. `regex`: the claim, or one element, matches `match_value` as a whole |
| `match_value` | none | The value or regular expression, up to 512 characters; required for `equals` and `regex`, refused for `any` |
| `target` | required | For `user`: the user name, without the tenant prefix. For `role`: the role's name (`analyst`, host `%`), or `name@host`. A template: each `{claim}` is replaced by that claim's value (`{email}`, `app-{azp}`); 1 to 255 characters |

The rules:

- The first `user` mapping, by priority, whose claim matches and whose template can be filled names the account. A template that names a missing claim, or one that isn't a string, number or boolean, doesn't match, and the next mapping is tried.
- Strings are compared as they are; numbers and booleans as JSON writes them (`42`, `true`).
- Regular expressions are anchored: `.*@example\.com` must match the whole value. Look-around and back-references aren't supported.
- A token that no `user` mapping matches is refused.
- The name is matched against the tenant's accounts like a login name: the account whose host pattern matches the client's address.

Examples:

```json
[
  {"priority": 10, "kind": "user", "claim": "azp", "match_kind": "equals",
   "match_value": "billing-service", "target": "billing"},
  {"priority": 20, "kind": "user", "claim": "client_id", "match_kind": "regex",
   "match_value": "svc-[a-z]+", "target": "app-{client_id}"},
  {"priority": 30, "kind": "user", "claim": "preferred_username", "target": "{preferred_username}"}
]
```

- Tokens issued to the client `billing-service` log in as the single service account `billing`.
- Tokens of clients named `svc-...` log in as `app-svc-...`, one account per client.
- Any other token with a `preferred_username` logs in as the account of that name: one account per person.

### Roles from tokens

`role` mappings give a token login roles for its session. Every `role` mapping whose claim matches adds its target, so a token can bring several roles:

```json
[
  {"priority": 100, "kind": "role", "claim": "realm_access.roles", "match_kind": "equals",
   "match_value": "db-readers", "target": "reader"},
  {"priority": 110, "kind": "role", "claim": "department", "target": "{department}_analyst"}
]
```

A token whose `realm_access.roles` array contains `db-readers` gets the role `reader`; one whose `department` claim is `sales` gets `sales_analyst`.

- The roles must exist in the tenant (`CREATE ROLE`). A token role the tenant doesn't define is ignored, and the login is audited as `auth.token_roles_ignored` naming it; SprigDB never creates roles from tokens.
- They don't need to be granted to the account. The session holds them as if granted, for its life, and they are active from login together with the account's default roles.
- `SET ROLE` can name them, `SET ROLE NONE` drops them, and `SET ROLE DEFAULT` and `SET ROLE ALL` include them. `CURRENT_ROLE()` lists them.
- They are evaluated again from the token's claims after every change to the tenant's accounts, issuers or roles: a changed mapping, a disabled issuer or a dropped role holds from the session's next statement, as a `REVOKE` does.
- A session logged in with a password has no token roles.

## Let an account log in with tokens

Every token login, on every protocol, needs the account's `token` login method. Turn it on for an existing account:

```sh
sprigctl tenant token-login acme billing                 # allow token logins
sprigctl tenant token-login acme alice --host '10.%'     # the account 'alice'@'10.%'
sprigctl tenant token-login acme billing --off           # stop them
```

```text
'billing'@'%' (id 17) in tenant acme: token logins allowed
```

Through the Admin API, `POST /admin/v1/tenants/acme/users:setTokenLogin` with `{"user": "billing", "host": "%", "enabled": true}` (`host` defaults to `%`). An unknown account fails with 1396.

The account keeps its other login methods: if it has a password or API keys, they keep working. Create the account and grant its privileges as usual, with `CREATE USER` and `GRANT`.

### Accounts created on first login

Over HTTP and MongoDB, when the issuer has `"jit_users": true` and the account its user mapping names doesn't exist, SprigDB creates `'<name>'@'%'` on the first login: with the `token` method only, no password and no privileges of its own. The creation is audited as `auth.user_created_jit`. Give it what it needs through [role mappings](#roles-from-tokens), or grant to it from a script that watches for the event. MySQL, MariaDB and Redis logins never create accounts: the login name must name an existing one.

### End-user layers

An issuer of kind `end_user_layer` is an application's own token service, whose tokens belong to the application's end users. End users aren't database accounts: their tokens log in as the application's service account, which a user mapping names (for example from the `azp` claim).

- `jit_users` is refused on such an issuer with 39998, so no end user ever becomes an account.
- The token's claims reach `SPRIG_CLAIM` and row-level security unchanged, so policies can filter by end user.
- Many end users share one account, so a login's `auth.login_succeeded` event names the end user too: `detail` reads `issuer <iss>; end user <sub>`.

## Log in

### MySQL and MariaDB

Send the token as the password of the account, over TLS. The login name names the account as usual (`acme.billing`, or `billing` on a [tenant endpoint](tls.md#tenant-endpoints-by-server-name)), and the token's user mapping must name that same account.

```sh
TOKEN=$(get-token)    # however your application gets its token from the identity provider
mysql -h db.example.com -P 3306 -u acme.billing --password="$TOKEN" \
  --ssl-mode=VERIFY_IDENTITY --ssl-ca=ca.pem shop
```

With the default `caching_sha2_password`, the token goes to the gateway over TLS in the full authentication step. `mysql_clear_password` works too, over TLS only (`mysql --enable-cleartext-plugin ...`). Without TLS, a token can't be sent: the login fails with 1045 (or 3159 with `mysql_clear_password`). Drivers need no special setting: pass the token as the password and turn TLS on. Tokens are long, so pass them with `--password=` or an option file rather than at a prompt.

A password that looks like a JWT (three dot-separated base64url parts, starting `eyJ`) is checked as a token when the account has the `token` method, and as a password otherwise, where it fails. Any other password is checked as a password.

### Redis

```sh
redis-cli -h db.example.com -p 6379 --tls --cacert ca.pem --user acme.billing --pass "$TOKEN"
```

`AUTH acme.billing <token>` and `HELLO 3 AUTH acme.billing <token>` work the same way: the login name names the account, and the token's user mapping must name it. A refused token answers `WRONGPASS`, like a wrong password. Use TLS: the token is sent as it is.

### REST and gRPC

Send the token as a bearer token. The token names the account through its user mapping; the tenant comes from, in order:

1. the host name, on a [tenant endpoint](tls.md#tenant-endpoints-by-server-name) (`acme.db.example.com`);
2. the `x-sprig-tenant` header;
3. otherwise the request fails with 401 and `ER_SPRIG_TENANT_AMBIGUOUS` (39786).

```sh
curl -s https://db.example.com:8443/v1/databases/shop/sql:query \
  -H "Authorization: Bearer $TOKEN" -H 'x-sprig-tenant: acme' \
  -H 'content-type: application/json' \
  -d '{"sql": "SELECT id, status FROM orders WHERE customer_id = 42"}'
```

gRPC takes the same values as `authorization` and `x-sprig-tenant` metadata.

#### As an HTTP Basic password

A client that can only send HTTP Basic, such as a BI tool, `curl -u` or a driver's REST mode, can send the token as the password instead. This works only for an account with the `token` login method, as over MySQL: the login name names the account, and the token's user mapping must name that same account. The tenant comes from the host name on a tenant endpoint, else from the `x-sprig-tenant` header, else from the login name's prefix (`acme.billing`).

```sh
curl -s https://db.example.com:8443/v1/databases/shop/sql:query \
  -u "acme.billing:$TOKEN" -H 'content-type: application/json' \
  -d '{"sql": "SELECT id, status FROM orders WHERE customer_id = 42"}'
```

- The request gets the token's claims (`SPRIG_CLAIM`) and token roles, exactly as with `Authorization: Bearer`.
- The issuer must take `http`. A login that the gateway hands to another data centre, for the tenant's [residency](residency.md), is checked there as a MySQL login, so the issuer must also take `mysql`.
- A password shaped like a token, sent for an account without the `token` method, is checked as a password and carries no claims.
- No account is created on first login, even with `jit_users`.
- The audit event is `auth.login_succeeded` with `protocol` `http` and `mechanism` `jwt`.
- Use HTTPS: the token is sent as it is, on every request.

The gateway tells the bearer credentials apart by their form: `sprig_...` is an API key, `sprigat_...` a SprigDB access token, and any other JWT an issuer's token. Every request is checked; a gateway reuses a token it verified in the last 60 seconds (never past its `exp`) without checking the signature again, and forgets it at any change to the tenant's accounts or issuers. A refused token gets 401 with 1045 in the body.

### MongoDB

MongoDB drivers log in with tokens through the `MONGODB-OIDC` mechanism, in one of two flows:

- **The human flow.** The driver sends a user name, such as `acme.alice`. Its prefix names the tenant (on a tenant endpoint, the host name does). SprigDB answers with the tenant's first active issuer that takes `mongodb`, with its `client_id` and `request_scopes`. The driver's callback, or a tool's built-in browser login, gets a token from that provider, and the driver sends it. The token's user mapping names the account; the user part of the name isn't checked against it.
- **The machine flow.** The driver sends the token at once, from a callback or from a built-in environment such as `ENVIRONMENT:k8s`. Drivers send no user name in this flow, so the tenant must come from the host name: connect to a tenant endpoint. On a shared host name the login fails.

```text
mongodb://acme.alice@db.example.com:27017/shop?tls=true&authMechanism=MONGODB-OIDC
mongodb://acme.db.example.com:27017/shop?tls=true&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s
```

With the Rust driver (3.x), a human-flow callback that hands over a token the application already has:

```rust
use futures::FutureExt as _;
use mongodb::options::{AuthMechanism, ClientOptions, Credential, oidc};

let token: String = get_token(); // from your identity provider
let mut opts = ClientOptions::parse("mongodb://db.example.com:27017/?tls=true").await?;
let mut cred = Credential::builder()
    .mechanism(AuthMechanism::MongoDbOidc)
    .username("acme.alice".to_owned())
    .build();
cred.oidc_callback = oidc::Callback::human(move |_ctx| {
    let token = token.clone();
    async move { Ok(oidc::IdpServerResponse::builder().access_token(token).build()) }.boxed()
});
opts.credential = Some(cred);
let client = mongodb::Client::with_options(opts)?;
```

A refused login answers 18 `AuthenticationFailed`.

## What SprigDB checks

A token is checked in this order; the first failure refuses it:

| Check | Refused when |
|---|---|
| Form | It isn't a compact JWS (three base64url parts), is longer than 16 KiB, has a `crit` header, or its claims aren't a JSON object |
| Issuer | `iss` isn't exactly the `issuer` of an active issuer of the tenant that takes this protocol |
| Algorithm | The header's `alg` isn't in the issuer's `algorithms` |
| Key | No key of the issuer's key set has the token's `kid` and fits the algorithm (a token without `kid` is tried against every key that fits) |
| Signature | The signature doesn't verify |
| Times | There is no `exp`; `exp` has passed; `nbf` or `iat` is in the future. Each allows `auth.token_clock_skew_seconds` (60) of clock difference |
| Audience | No `aud` value (a string or an array) is one of the issuer's `audiences` |
| Lifetime | `exp` minus `iat` (or minus now, without `iat`) is above `max_lifetime_s` |
| Account | No user mapping names an account; over MySQL, MariaDB and Redis, it names another account than the login's; the account doesn't exist (and isn't created on first login) or lacks the `token` method |

Then the account's own state applies, as for a password: a temporary lock after failed logins (3955), a suspended tenant (39303), `ACCOUNT LOCK` (3118), and `REQUIRE SSL`, `REQUIRE X509` and `auth.require_tls` (1045). Password expiry doesn't apply to token logins, since no password is used.

Keys are accepted as RSA (2,048 to 8,192 bits, for the `RS` and `PS` algorithms), EC on P-256 (`ES256`) or P-384 (`ES384`), and Ed25519 (`EdDSA`). A key that names its own `alg` verifies only that algorithm. Keys marked for encryption (`"use": "enc"`) and keys of other types are ignored.

### Key sets

- **Where from.** The issuer's `jwks_url`, or, without one, OIDC discovery: `<issuer>/.well-known/openid-configuration`, whose `issuer` must equal the configured one and whose `jwks_uri` names the key set. Discovery runs at every fetch, so a moved key set is followed.
- **How.** Over HTTPS, with the server certificate checked against the public web's root certificates (the Mozilla list), so the identity provider needs a certificate from a public certificate authority. Plain HTTP is accepted only for loopback addresses. A fetch must answer `200` within 5 seconds, with at most 1 MiB.
- **Cached per gateway.** A key set is refetched when it is older than `auth.jwks_refresh_seconds` (3600). A token whose `kid` isn't in the cached set makes the gateway refetch at once, at most once a minute per key set.
- **Rotation.** Publish a new key in the key set before signing with it: the first token signed with it brings it in. A key removed from the set stops verifying at the next refetch.
- **Outages.** While the issuer can't be reached, the cached keys keep working until they are twice `auth.jwks_refresh_seconds` old (2 hours by default); then the issuer's tokens are refused until a fetch succeeds. Failed fetches are retried at most once a minute, logged as warnings ("JWKS fetch failed") and counted in `sprig_auth_jwks_fetches_total{outcome="error"}`.

### Sessions

Over MySQL, MariaDB, Redis and MongoDB the token is checked when the connection logs in, and the session continues after the token expires. Over REST and gRPC every request carries its token. Account changes end sessions as for any login: `DROP USER` and `ACCOUNT LOCK` close them (see [Sessions after account changes](accounts-and-logins.md#sessions-after-account-changes)). Disabling an issuer, or removing an account's `token` method, refuses new logins; connections already logged in stay open, and lose their token roles at their next statement.

A login that a gateway hands to another data centre because of the tenant's [residency](residency.md) is verified there, and the session keeps the token's claims and roles.

## Claims in row-level security

`SPRIG_CLAIM('key')` returns the top-level claim `key` of the token the session logged in with, on all four protocols. Every verified claim is available, always as text (or `NULL`):

| Claim | `SPRIG_CLAIM` returns |
|---|---|
| A string | The string |
| A number | Its JSON text, as a string: `7` gives `'7'`, `2.5` gives `'2.5'` |
| `true`, `false` | `'1'`, `'0'` |
| An array or an object | Its JSON text, as a string |
| Missing | `NULL` |

Because a number arrives as text, `org_id = SPRIG_CLAIM('org')` compares as MariaDB compares a column with a string, and a boolean claim is tested with `SPRIG_CLAIM('admin') = '1'`. Policies and the session's own queries bind claims the same way, so they always agree.

`SPRIG_CLAIM` works in any query (`SELECT SPRIG_CLAIM('org')`) and in row-level security policies; see [Row-level security](row-level-security.md). This policy shows each token only its own organisation's rows:

```sql test
CREATE TABLE projects (
  id INT PRIMARY KEY,
  org VARCHAR(20) NOT NULL,
  name VARCHAR(50) NOT NULL
);
INSERT INTO projects VALUES (1, 'o-1', 'apollo'), (2, 'o-1', 'gemini'), (3, 'o-2', 'mercury');

CREATE POLICY by_org ON projects FOR ALL TO PUBLIC USING (org = SPRIG_CLAIM('org'));
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects FORCE ROW LEVEL SECURITY;

SELECT COUNT(*) FROM projects;
```

```text output
COUNT(*)
0
```

This example runs with a password, which carries no claims, so `SPRIG_CLAIM('org')` is `NULL` and no row matches. A session that logged in with a token whose `org` claim is `o-1` sees rows 1 and 2, and one whose `org` is `o-2` sees row 3.

An array claim, such as a list of groups, arrives as JSON text, so test it with the JSON functions:

```sql test
CREATE TABLE reports (id INT PRIMARY KEY, team VARCHAR(20) NOT NULL);
CREATE POLICY by_group ON reports FOR SELECT TO PUBLIC
  USING (JSON_CONTAINS(SPRIG_CLAIM('groups'), JSON_QUOTE(team)));
```

Studio's row-level security tester can simulate claims, so you can try policies before any token logs in; see [Test as a user](row-level-security.md#test-as-a-user).

## Audit and troubleshooting

Clients get the same error for every refused token, so that a token can't be probed: 1045 over MySQL and MariaDB, `WRONGPASS` over Redis, 401 with 1045 over REST and gRPC, and 18 `AuthenticationFailed` over MongoDB. The reason is in the tenant's audit stream: an `auth.login_failed` event with `mechanism` `jwt`, `reason` `bad_credentials` and the reason in `detail`.

```json
{"event":"auth.login_failed","at_ms":1763193412345,"tenant":42,"tenant_name":"acme","user":"billing","host":"%","client_ip":"10.20.1.7","gateway":"gw-a-1","protocol":"http","mechanism":"jwt","outcome":"refused","reason":"bad_credentials","detail":"token: expired"}
```

| `detail` | What to do |
|---|---|
| `token: malformed (...)` | Send the raw compact token, under 16 KiB, with an `exp` claim |
| `token: unknown issuer` | The token's `iss` must equal a registered `issuer` exactly (watch for a trailing `/`), the issuer must be active, and its `protocols` must include this one |
| `token: algorithm not allowed` | Add the token's algorithm to the issuer's `algorithms`, if it is one SprigDB verifies |
| `token: issuer keys unavailable (...)` | The key set can't be fetched and no recent copy is cached; the message names the URL and the error. Check the URL, the gateway's network access to it, and its certificate |
| `token: unknown key id` | The key set has no key with the token's `kid` that fits its algorithm, even after a refetch; a key published moments ago is fetched at most once a minute |
| `token: bad signature` | The token wasn't signed by the issuer's key, or was changed |
| `token: expired`, `token: not yet valid` | Get a fresh token; check the clocks, or raise `auth.token_clock_skew_seconds` |
| `token: wrong audience` | Add the token's `aud` to the issuer's `audiences`, or have the provider issue tokens for SprigDB's audience |
| `token: lifetime above the issuer's maximum` | Raise `max_lifetime_s`, or issue shorter-lived tokens |
| `token: no claim mapping names this account` | No user mapping matched; over MySQL, MariaDB and Redis, the token names another account than the login name |
| `token: the mapped account doesn't take tokens` | Create the account, or set `jit_users`; give it the `token` method with `sprigctl tenant token-login` |

Over MySQL, MariaDB and Redis, a token sent for an account without the `token` method is checked as a password, so its event has no `detail` and its `mechanism` is the password exchange's. A login refused for the account's state (locked, suspended, TLS required) is reported as for a password.

Other events:

| Event | When |
|---|---|
| `auth.login_succeeded` | A token login succeeded; `mechanism` is `jwt` and `detail` names the issuer, the end user for an `end_user_layer` issuer, and the token roles it took (`issuer https://...; token roles reader`) |
| `auth.token_roles_ignored` | A token login succeeded, but its role mappings named roles the tenant doesn't define; `detail` names them, `reason` is `undefined_role` |
| `auth.user_created_jit` | An account was created on first login, with the issuer in `detail` |
| `admin.issuer_changed` | An issuer was created, replaced or disabled, with its id, `active`, `jit_users`, protocols and number of mappings |
| `admin.token_login_changed` | An account's `token` method was added or removed |

Find a tenant's refused token logins of one day:

```sh
cat /mnt/audit/audit/tenant-42/2026-11-15/*.jsonl \
  | jq -c 'select(.event == "auth.login_failed" and .mechanism == "jwt") | {at_ms, user, protocol, detail}'
```

Refused tokens count toward the client address's failure limit (`auth.ip_failures_per_minute`, then 39300), but never toward the account's temporary lock: anyone can send a bad token, so counting them would let anyone lock an account. See [Audit](audit.md).

## Settings

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `auth.token_clock_skew_seconds` | int | 60 | tenant, dc, global | Clock difference allowed on `exp`, `nbf` and `iat`, 0 to 600 |
| `auth.jwks_refresh_seconds` | int | 3600 | tenant, dc, global | How often a key set is refetched, 60 to 604800; cached keys serve twice as long while the issuer can't be reached |

Both apply at once, without a restart. See [Settings](../reference/settings.md).

## Metrics

| Metric | Meaning |
|---|---|
| `sprig_auth_jwks_fetches_total{outcome}` | Key-set fetches, `ok` or `error` |
| `sprig_auth_logins_total{protocol, mechanism="jwt", outcome}` | Token logins over REST, gRPC (`protocol="http"`) and MongoDB (`protocol="mongo"`). Token logins over MySQL, MariaDB and Redis are counted under their password exchange's `mechanism` |
| `sprig_mongo_auth_total{mechanism="MONGODB-OIDC", result}` | MongoDB token logins |

See [Metrics](../reference/metrics.md).

## Not supported yet

- **Accounts created on first login** over MySQL, MariaDB and Redis, or with a token sent as an HTTP Basic password.
- **Tokens on Bolt, Kafka and AMQP.**
- **Key sets behind a private certificate authority.** Key sets are fetched only from servers with a publicly trusted certificate, or over plain HTTP on a loopback address.
- **Managing issuers in SQL or Studio.** Use `sprigctl` or the Admin API.

## Related

- [API keys and tokens](api-keys-and-tokens.md)
- [Accounts and logins](accounts-and-logins.md)
- [Row-level security](row-level-security.md)
- [TLS](tls.md)
- [Audit](audit.md)
- [Error codes](../reference/errors.md)
