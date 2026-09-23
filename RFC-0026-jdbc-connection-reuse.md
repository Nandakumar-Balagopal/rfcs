# RFC-0026 Connection reuse and pooling for JDBC connectors

## Proposers

* Nandakumar Balagopal (@Nandakumar-Balagopal)

## Related Issues

* prestodb/presto#27638 — `feat(plugin-oracle): Upgrade presto oracle version and enable inbuilt connection pool` (open). Adds pooling to a single connector using the Oracle driver's own pool library.
* prestodb/presto#27536 — `Implement jdbc-fetch-size Across JDBC Connectors`. Precedent for a configuration property added once in `presto-base-jdbc` and inherited by every JDBC connector.
* trinodb/trino#15888 — `Enable connection pooling for Postgresql` (open since January 2023, no maintainer reply). The same request in the sibling project, still unanswered.
* trinodb/trino#6719 and trinodb/trino#7069 — connections opened during planning that are never used, fixed with a `LazyConnectionFactory`. Prior art for decorating `ConnectionFactory`, and for the position that opening fewer connections is preferable to reusing more.
* trinodb/trino#30713 — `Fix Oracle pooled connection factory ignoring session extraCredentials` (open). A pool that is not keyed by credentials.
* trinodb/trino#30177 — `Oracle Failed to get a connection since v480` (open). Connection acquisition failures in the field.

A separate issue will be filed for a defect found while measuring: setting `metadata-cache-ttl` without also setting `metadata-cache-refresh-interval` makes a JDBC catalog fail to load (see [Background](#background)).

## Summary

Today, JDBC connectors generally open a new connection for each metadata operation and split, then close it afterwards. This becomes expensive when the database is remote because connection setup can cost much more than the query itself. Reuse needs to happen before pooling because some metadata paths acquire connections recursively.

This RFC proposes two changes in `presto-base-jdbc`, both inherited by every JDBC connector without connector-level code changes:

1. **Reuse** — a caller asking for a connection is given the one it already holds, and a connection is kept for a short while after release so that the next call on the same thread can adopt it. This is the cheaper change by a wide margin: no pool, no new dependency, two configuration properties.
2. **Pooling** — an optional pooled `ConnectionFactory`, **off by default**, with one pool per set of credentials, session state reset before hand-out, a bounded connection lifetime, and metrics. It earns its keep where connections are wanted at the same time rather than one after another.

Measured on Presto's own PostgreSQL connector against a PostgreSQL instance at 25 ms round-trip latency, reading column metadata for 20 tables goes from **17.5 s to 3.2 s with reuse alone**, and to **3.0 s with a pool**. A three-table join goes from 2.0 s to 1.0 s with reuse. Reuse therefore delivers most of what is available here; see [What each part is worth](#what-each-part-is-worth).

## Background

### Where the time goes

A JDBC connector calls `ConnectionFactory.openConnection()` in every metadata operation and every split, and `DriverConnectionFactory` dials the database each time. The cost of that call relative to a query, measured directly over JDBC:

| Database | Open a connection | Query on an open connection |
| --- | --- | --- |
| PostgreSQL 16, same host | 134 ms | 0 ms |
| PostgreSQL 16, 25 ms round-trip latency | 170 ms | 26 ms |
| Teradata, TLS over a wide-area link | 3560 ms | 500 ms |

The important part is the ratio. For the remote cases I tested, opening a connection was several times more expensive than running the query. On the same host the absolute cost is much smaller, so the problem is less noticeable.

### What that costs a user

Resolving column metadata opens one connection per table. Through the PostgreSQL connector, against 20 tables and 20 views at 25 ms latency:

| Configuration | `SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public'` |
| --- | --- |
| Current behaviour | **17.5 s** |
| With reuse | **3.2 s** |
| With pooling | **3.0 s** |

Data queries pay it too, because splits open connections: a three-table join goes 2.0 s → 1.0 s, and `count(*)` on a single table 1.1 s → 0.9 s.

### What each part is worth

Reuse and pooling address two different costs. Measured with each enabled on its own, and with reuse split into its two halves — serving a caller that already holds a connection, and keeping a connection for a moment after it is released:

| Workload | Untouched | Reuse, no retention | Reuse, 2 s retention | Reuse, 10 s retention | Pool of 16 |
| --- | --- | --- | --- | --- | --- |
| Column metadata, 20 tables (PostgreSQL, 25 ms) | 17.5 s | 17.6 s | **3.3 s** | **3.2 s** | 3.0 s |
| Three-table join | 2.0 s | 2.0 s | — | 1.0 s | 1.1 s |
| `count(*)` on one table | 1.1 s | 1.1 s | — | 0.9 s | 0.7 s |
| 8 concurrent metadata queries | 17.8 s | — | — | 5.0 s | 3.4 s |
| `SHOW CREATE VIEW`, nested call (Teradata, wide-area) | 10.9–14.1 s | 6.9–7.1 s | — | 7.3–7.7 s | 3.6 s |
| 16-view listing (Teradata, wide-area) | 151 s | — | — | 66 s | 50 s |

Three things follow.

**Serving a caller that already holds a connection covers only overlap.** Where a caller holds a connection while opening a second — `getViews` resolving each view's columns, `listTableColumns` per table — this removes the second connection: about 3.7 s on the nested view lookup, which is one connection on a link where opening one costs 3.6 s. It does nothing for metadata read a table at a time, because each call closes its connection before the next one opens another; 17.6 s against 17.5 s untouched.

**Keeping the connection briefly after release covers repetition, and that is where the time is.** With retention the same scan drops to 3.2 s, within seven percent of a pool, using neither a pool nor a pooling library. The retention length barely matters, 2 s and 10 s being indistinguishable, which agrees with the two seconds Trino settled on.

**A pool still wins where connections are wanted at the same time.** Retention only lets a thread reuse its own last connection; a pool shares across threads and queries. That shows up on the two Teradata paths — 50 s against 66 s on the view listing, 3.6 s against 7.4 s on the nested lookup — and under concurrency, 3.4 s against 5.0 s for eight simultaneous queries.

So the two are complementary rather than alternatives, and reuse is the one to do first: it is a fraction of the surface for the majority of the benefit.

### Why metadata caching does not answer this

`metadata-cache-ttl` caches table handles and column handles. It does not cover the path above, and it cannot help the split path at all:

| Configuration | cold | warm |
| --- | --- | --- |
| Current behaviour | 17.5 s | 17.6 s |
| `metadata-cache-ttl=30s` | 17.4 s | 17.5 s |
| Reuse | 3.2 s | 3.3 s |
| Pooling | 3.0 s | 2.8 s |
| Pooling + `metadata-cache-ttl=30s` | 2.9 s | 2.8 s |

The cache is working — a repeated single-table query does improve, 1.1 s → 0.6 s → 0.6 s with the cache on and 1.1 s on every run with it off — it simply does not reach the bulk metadata path.

Two further observations on the cache. It is off by default (`metadata-cache-ttl` is `0`). And enabling it alone breaks the catalog: `JdbcMetadataCache` passes `OptionalLong.of(0)` into Guava's `refreshAfterWrite` whenever `metadata-cache-refresh-interval` (default `0`) is below the TTL, so the catalog fails to load with `IllegalArgumentException: duration must be positive: 0 MILLISECONDS`. That is outside the scope of this RFC and will be filed separately; it is mentioned here because metadata caching is an obvious alternative to consider.

### What already exists, in Presto and in Trino

| | prestodb/presto | trinodb/trino |
| --- | --- | --- |
| Pooling | Oracle only, in review (#27638), using the Oracle driver's own pool | Oracle only, merged, on by default, using the Oracle driver's own pool |
| Lazy connections | none | `LazyConnectionFactory` (#7069) |
| Retry on connect | none | `RetryingConnectionFactory` |
| Reuse within a query | none | `ReusableConnectionFactory`, behind `query.reuse-connection` |
| `ConnectionFactory` bound in the module | no — each connector constructs its own | yes, as a decorator chain |

In both projects, pooling is currently implemented for Oracle, where the driver provides its own pool. There is no shared pooling implementation for the other JDBC connectors. The generic request has been made (trinodb/trino#15888) and never answered. Meanwhile Trino's Oracle pool demonstrates what goes wrong without a shared design: it keys nothing by credentials (trinodb/trino#30713), has no `close()`, and has to set auto-commit explicitly on every hand-out because the pool does not restore it.

## Goals

* Make the cost of reaching a remote database proportional to the work requested rather than to the number of metadata calls.
* Apply to every JDBC connector without per-connector code changes.
* Change nothing for a user who does not opt in.
* Make the failure modes of connection reuse explicit, tested, and configurable.

## Non-goals

* Fixing the `1+N` metadata access pattern itself. Reducing the number of dictionary queries is complementary, and better addressed by passing an open connection down into `getColumns` — see [Other Approaches Considered](#other-approaches-considered).
* Migrating the Oracle connector off its own pool.
* Pooling in the Presto JDBC *driver* (client to coordinator), which is a different concern.
* Any change to connectors that do not use `presto-base-jdbc`.

## Proposed Implementation

### Modules involved

* `presto-base-jdbc` — all production code.
* `presto-docs` — connector documentation for the new properties.
* No changes required in the 14 connectors that build on `presto-base-jdbc`; they construct `DriverConnectionFactory` from `BaseJdbcConfig`, so they inherit both changes.

### Concepts

* **Reuse** — a caller that already holds a connection for a set of credentials is handed the same connection when it asks again, and the connection returns to its origin only when the outermost caller closes it.
* **Credential set** — the effective JDBC connection properties, including any user and password supplied per session through `user-credential-name` and `password-credential-name`. Pools and reuse are keyed by this, never by the Presto user alone.
* **Session reset** — a database-specific action performed before a connection is handed out again, discarding state the previous user left on the session.

### Why reuse has to come first

`JdbcMetadata.getViews` holds a connection open while the client resolves the view's columns, which opens a second one; `listTableColumns` does the same per table. A pool sized below that nesting depth deadlocks: every caller holds its first connection and waits for a second that only another caller can release. Measured with the prototype before reuse was added:

```
pool size 1, SHOW CREATE VIEW                  -> Cannot get a connection, pool error
                                                  Timeout waiting for idle object
pool size 2, two concurrent SHOW CREATE VIEW   -> both queries fail
```

With reuse enabled, both cases pass. When the pool is smaller than the offered load, requests wait for an available connection instead of failing because of nested acquisition.

### How reuse works

* A caller that already holds a connection for a set of credentials is handed the same connection, reference counted, and it is released only when the outermost caller closes it.
* On release the connection is kept for `connection-reuse.retention`, recorded against the thread and credentials that held it, rather than closed.
* The next call on that thread for the same credentials adopts it. Read-only and auto-commit are restored and an open transaction rolled back first, since `BaseJdbcClient` marks a connection read-only while reading and a caller may leave auto-commit off.
* A connection nobody returns for is closed by a background sweep. Measured on PostgreSQL, sessions rise with concurrency and return to their prior count once the sweep runs.

**Open question for review — what reuse should be keyed by.** The prototype keys by thread, which needs no interface change but means a connection parked by one query could in principle be adopted by another query that lands on the same pooled thread. Trino keys by query id, which is exact, and can do so because its `openConnection` takes `ConnectorSession`. Presto's takes `JdbcIdentity`, which carries only the user and extra credentials; adding a query id to it is not viable because `JdbcIdentity` is itself a cache key in `BaseJdbcClient`, and several methods there have no `ConnectorSession` in scope at all. Keying by query id therefore means changing `ConnectionFactory.openConnection` and threading a session through `presto-base-jdbc` and every connector — a larger change that is worth making if reviewers prefer the exact semantics. This RFC recommends deciding that before implementation rather than after.

### Interfaces and contracts

`ConnectionFactory` itself does not change:

```java
@FunctionalInterface
public interface ConnectionFactory
        extends AutoCloseable
{
    Connection openConnection(JdbcIdentity identity) throws SQLException;

    @Override
    default void close() throws SQLException {}
}
```

The implementation needs three changes:

**1. Bind `ConnectionFactory` in `JdbcModule`.** Today each connector constructs its own, so there is no seam for a decorator and nothing for JMX to export. Binding it allows a chain, and follows Trino's arrangement:

```
DriverConnectionFactory  ->  ReusingConnectionFactory  ->  PoolingConnectionFactory (optional)
```

Connectors that construct a factory directly keep working; the binding is what new behaviour attaches to.

**2. A pooled factory.**

```java
public class PoolingConnectionFactory
        implements ConnectionFactory
{
    // one pool per credential set, in a bounded cache that closes what it evicts
    Connection openConnection(JdbcIdentity identity) throws SQLException;
    void close();     // closes every pool
}
```

I tested the following cases in the prototype:

* a pooled connection killed by a database restart or an idle firewall must not be handed out;
* `BaseJdbcClient` marks a connection read-only while reading and a caller may leave auto-commit off, so both must be restored before hand-out;
* a transaction left open must be rolled back;
* a connection must be retired by age, so an authenticated session cannot outlive a credential change indefinitely;
* pools must not accumulate without bound when credentials are passed through per session.

**3. A session reset hook on `JdbcClient`.**

```java
default void resetConnection(Connection connection) throws SQLException {}
```

A single reset SQL statement isn't enough for all of the databases I tested. Measured, with a temporary table created on one borrow and looked for on the next:

| Database | State survives return to the pool | Statement that clears it |
| --- | --- | --- |
| PostgreSQL | yes | `DISCARD ALL` |
| MySQL | yes | none — MySQL resets a connection through the protocol, so `RESET CONNECTION` reaches the server as SQL and is rejected |
| Teradata | yes, including the query band | `SET QUERY_BAND = NONE FOR SESSION` clears the band; nothing drops a volatile table |

For that reason, the connector needs a code-level reset hook rather than relying only on a SQL property. A `connection-pool.reset-sql` property is still useful for the databases where a statement suffices, and is validated when the pool is created so that a statement the database will not accept fails immediately, naming the property, rather than surfacing later as `Unable to activate object`.

### Code flow

Opening a connection, with pooling enabled:

* the client asks the factory for a connection, passing `JdbcIdentity`;
* a connection older than its maximum lifetime is retired rather than returned;
* if the caller already holds a connection for that credential set, it is handed the same one, reference counted;
* otherwise the pool for that credential set is located, or created and its reset statement verified;
* a connection is borrowed; if validation on borrow is enabled it is checked first, and a connection that has died is discarded and replaced;
* read-only, auto-commit and, where a connector implements it, session state are reset;
* on close, the outermost holder returns the connection; a transaction still open is rolled back;
* a connection older than its maximum lifetime is retired rather than returned;
* a connection idle longer than the maximum idle time is evicted, releasing the session at the database;
* on catalog shutdown, `@PreDestroy` on `BaseJdbcClient` closes the factory, which closes every pool.

### Configuration

| Property | Default | Meaning |
| --- | --- | --- |
| `connection-reuse.enabled` | `false` | Hand a caller the connection it already holds instead of opening a second one |
| `connection-reuse.retention` | `0s` | How long a connection is kept after release so the next call on the same thread can adopt it; `0` closes it immediately |
| `connection-pool.enabled` | `false` | Reuse connections through a pool instead of opening one per operation |
| `connection-pool.max-size` | `10` | Maximum connections per credential set |
| `connection-pool.max-wait` | `30s` | How long to wait for a connection from an exhausted pool before failing |
| `connection-pool.max-idle-time` | `5m` | How long an unused connection is kept before it is evicted |
| `connection-pool.max-lifetime` | `30m` | How long a connection may live before it is replaced; bounds how long an authenticated session outlives a credential change |
| `connection-pool.max-credential-sets` | `100` | Bounds the pools created by credential pass-through |
| `connection-pool.validate-on-borrow` | `true` | Check a connection before handing it out, at the cost of one round trip |
| `connection-pool.reset-sql` | unset | Statement run before hand-out to clear session state |

### User-facing metrics

Exported per catalog over JMX, in the manner of `JdbcMetadataCacheStats`:

* pools created;
* connections borrowed;
* time spent waiting for a borrow;
* connections active and idle per pool.

These metrics are included in the proposal because otherwise pool exhaustion and credential-set growth would be difficult to diagnose.

## Metrics

The impact is measurable as end-to-end latency of metadata and data queries against a database with realistic latency, and as the count of connections opened per query. The benchmark in [Test Plan](#test-plan) is reproducible with Docker and a latency proxy, so reviewers can confirm the numbers independently.

## Other Approaches Considered

**Per-connector pooling with the driver's own pool.** What both projects do today for Oracle. This works for Oracle because the driver provides a pool, but it doesn't give us a solution for the other JDBC connectors: PostgreSQL, MySQL, SQL Server, Redshift and Teradata drivers do not provide one. The existing implementation also shows some problems that a shared implementation should avoid, such as credential isolation and session reset. (trinodb/trino#30713).

**Metadata caching.** Measured above: it does not cover the bulk metadata path, cannot help splits, is off by default, and cannot currently be enabled without a second undocumented property.

**Reducing the number of connections instead of reusing them.** The direction Trino took in #6719/#7069, and the better fix for the `1+N` pattern: pass the connection a caller already holds into `getColumns` rather than opening another. This is complementary to the proposal. Even with reuse, the 20-table test still takes about 2.7 s because most of the remaining time is spent on dictionary queries rather than connection setup, so removing those queries is the next lever after this RFC. This RFC proposes reuse and pooling; the algorithmic change is proposed separately, needs no configuration, and should land independently.

**Pooling alone, without reuse.** Measured, and it works — 3.0 s against 17.5 s on the bulk metadata scan — but it leaves the nesting hazard in place: a pool smaller than the nesting depth deadlocks, which is how the prototype behaved before reuse existed. Reuse is cheap enough that shipping the pool without it is not worth the failure mode.

**Reuse alone, without a pool.** Also measured, and it is the recommendation for the first phase: 3.2 s against 17.5 s, with none of the pool's configuration or dependency surface. It is weaker where connections are wanted concurrently, which is what the second phase addresses.

**HikariCP instead of Apache Commons DBCP 2.** Hikari is smaller and faster on the borrow path. DBCP 2 is already present in Presto's dependency management (`org.apache.commons:commons-dbcp2`, currently unused by any module), so it adds no new dependency to approve. Either is acceptable; the prototype uses DBCP 2 for that reason.

## Adoption Plan

* **Impact on existing users:** none unless they opt in. `connection-pool.enabled` defaults to `false`, and with it unset `DriverConnectionFactory` behaves exactly as today. Eight new catalog properties, listed above. No SQL grammar, client API or session property changes.
* **SPI:** one new default method on `JdbcClient` (`resetConnection`), which is a no-op unless a connector overrides it. `ConnectionFactory` is unchanged. Binding `ConnectionFactory` in `JdbcModule` is internal to `presto-base-jdbc`.
* **Phasing:**
  * Phase 1 — connection reuse with retention, and the `ConnectionFactory` binding. Two properties, no new dependency, and most of the available benefit: 17.5 s to 3.2 s on the bulk metadata scan, 2.0 s to 1.0 s on a join. Off by default.
  * Phase 2 — the optional pool, its properties, metrics and documentation.
  * Phase 3 — consider whether the Oracle connector should move onto the shared properties, and whether a default other than `false` is appropriate for any connector.
* **Migration tools:** none needed.
* **Removing existing behaviour:** nothing is removed. Unpooled operation remains the default and the only behaviour unless configured.
* **Teaching:** a section in each JDBC connector's documentation page covering the properties, a sizing formula (total connections at the database are `nodes × credential sets × max-size`), the per-database reset statement, and the security notes below.
* **Out of scope, addressable later:** the `1+N` metadata pattern; consolidating Oracle's pool; a driver-level reset for MySQL; pooling for connectors that do not use `presto-base-jdbc`.

### Security considerations

* **Credential isolation.** A pool is keyed by credential set, so a session authenticated as one user is never handed to another. Verified with two distinct credential sets on one catalog, each of which produced its own pool. This is the defect currently open against Trino's Oracle pool.
* **Credential revocation.** A pooled connection is an already-authenticated session, so a password change, a locked account or a revoked grant is not observed while the connection is reused; validation does not re-authenticate. `connection-pool.max-lifetime` bounds that window, and is why it defaults to 30 minutes rather than to unlimited. I verified this with PostgreSQL, MySQL, and Teradata: each hands out a new session once the lifetime passes.
* **Session state between Presto users.** Where several Presto users share one set of database credentials, state left on a session — a temporary table, a session variable, a query band — is visible to the next user of that connection. I reproduced this on PostgreSQL, MySQL, and Teradata. It is mitigated by `reset-sql` or `resetConnection` where the database allows it, and cannot be fully mitigated on every database. Documentation must say so, and recommend enabling pooling with credential pass-through or on single-tenant catalogs.
* **Reuse across queries on a shared thread.** Reuse keyed by thread can hand a connection parked by one query to another query that lands on the same pooled thread. Read-only, auto-commit and any open transaction are reset on adoption, but database-held session state is not, so this carries the same exposure as pooling. Keying by query id removes it; see the open question in [How reuse works](#how-reuse-works).
* **Audit attribution.** Database-side auditing sees sessions, not Presto users. Reusing sessions weakens that mapping either way — a stale query band misattributes, and clearing it removes the attribution.
* **Availability.** A single user's concurrent metadata queries can exhaust a pool and delay others by up to `max-wait`. Unpooled, each would have opened its own connection. `max-size` guidance and the metrics above are the mitigation.

## Test Plan

### Unit tests

Against H2, in `presto-base-jdbc`:

* reuse: a nested acquisition returns the connection already held, and the connection returns to its origin only when the outermost holder closes it;
* reuse is keyed by credential set: a nested acquisition with different credentials opens a separate connection;
* a pool is created per credential set and is bounded by `max-credential-sets`, and an evicted pool is closed;
* a connection reported invalid is discarded on borrow rather than handed out;
* read-only, auto-commit and an open transaction are reset before hand-out;
* a connection older than `max-lifetime` is retired;
* a `reset-sql` the database rejects fails when the pool is created, and the error names the property;
* a released connection is kept for the retention period, adopted by the next call on that thread, and closed by the sweep when nobody returns for it;
* read-only, auto-commit and an open transaction are reset when a parked connection is adopted;
* `close()` closes every pool and every parked connection.

Plus a `TestBaseJdbcConfig` case for the eight properties and their defaults.

### Integration tests

Using the existing testcontainers setup in `presto-postgresql` and `presto-mysql`: the same query pooled and unpooled returns identical results; a read followed by a write succeeds; a write that fails does not poison the connection for the next query; a database restart under load is survived.

### Proof of concept results

I have a prototype implementing both changes. I tested it with PostgreSQL 16 and MySQL 8 in Docker, PostgreSQL behind a 25 ms latency proxy, and Teradata over a wide-area TLS connection.

Latency, PostgreSQL connector at 25 ms round trip:

| Query | Current | Reuse | Pooled |
| --- | --- | --- | --- |
| Column metadata for 20 tables | 17.5 s | **3.2 s** | **3.0 s** |
| `count(*)` on one table | 1.1 s | 0.9 s | **0.7 s** |
| Three-table join | 2.0 s | **1.0 s** | 1.1 s |

The pooled figures above have borrow validation disabled; with it enabled the scan is 5.2 s, the difference being one round trip per borrow.

Contribution of each part, measured separately, is in [What each part is worth](#what-each-part-is-worth).

Concurrency, same query and link, no failures at any point:

| Concurrent queries | Current | Pool of 4 | Pool of 8 | Pool of 16 |
| --- | --- | --- | --- | --- |
| 8 | 17.8 s | 4.1 s | 4.0 s | 3.4 s |
| 16 | 19.4 s | 5.2 s | — | 5.4 s |
| 32 | — | 9.5 s (p95 11.2 s) | — | 8.6 s (p95 10.0 s) |

One thing that surprised me in these tests was how little the pool size affected the 32-query case. A pool of four was within about 10% of a pool of sixteen. Each operation holds the connection for a relatively short time, so increasing the pool size did not provide much additional benefit.

Failure modes exercised:

| Case | Result |
| --- | --- |
| Nested acquisition, pool smaller than the nesting depth | fails before the reuse change; passes after |
| Pool exhausted, generous `max-wait` | queues and completes |
| Pool exhausted, short `max-wait` | fails with a clear pool timeout |
| Connection killed at the socket, then borrowed | discarded and replaced |
| Database restarted under load | one in-flight query fails; the pool recovers |
| Database restarted, `validate-on-borrow=false` | the next borrow is handed a dead connection and the query fails |
| Database restarted, `validate-on-borrow=true` | the next borrow succeeds |
| Client killed mid-query, twice, against a pool of one | no leak; the pool serves the next query immediately |
| Two credential sets on one catalog | one pool each, both usable |
| Read followed by a write | fails until read-only is restored on hand-out; passes after |
| Three minutes of continuous load | database sessions flat, none leaked, released after idle |

Cost of `validate-on-borrow`, which is one round trip per borrow: 0 ms on a same-host database, about 56 ms per borrow at 25 ms round trip, 258 ms over the wide-area link. It defaults to on because with it off a database restart is followed by failed queries, as above.

### Not yet tested

I have not tested the following yet:

* multiple worker nodes
* SQL Server, ClickHouse, and Oracle
* soak tests longer than a few minutes
* credential pass-through with realistic user counts
