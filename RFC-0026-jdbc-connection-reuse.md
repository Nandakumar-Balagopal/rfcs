# RFC-0026 Connection reuse and pooling for JDBC connectors

## Proposers

* Nandakumar Balagopal (@Nandakumar-Balagopal)

## Related Issues

* [prestodb/presto#27638](https://github.com/prestodb/presto/pull/27638) - Oracle connection pool using the driver's own pool library.
* [prestodb/presto#27536](https://github.com/prestodb/presto/issues/27536) - `jdbc-fetch-size`, a property added once in `presto-base-jdbc` for every JDBC connector.
* [trinodb/trino#14653](https://github.com/trinodb/trino/pull/14653) - Trino's `ReusableConnectionFactory`. Phase 1 below follows its design.
* [trinodb/trino#6719](https://github.com/trinodb/trino/issues/6719), [trinodb/trino#7069](https://github.com/trinodb/trino/pull/7069) - Trino's `LazyConnectionFactory`, for connections opened during planning and never used.
* [trinodb/trino#15888](https://github.com/trinodb/trino/issues/15888) - Request for generic JDBC connection pooling in Trino, open since January 2023 with no reply.
* [trinodb/trino#19205](https://github.com/trinodb/trino/issues/19205), [trinodb/trino#30713](https://github.com/trinodb/trino/pull/30713) - Trino's Oracle pool ignores per-user credentials.
* [trinodb/trino#6801](https://github.com/trinodb/trino/issues/6801), [trinodb/trino#30177](https://github.com/trinodb/trino/issues/30177) - Trino's Oracle pool runs out of connections under concurrent queries.
* [trinodb/trino#8559](https://github.com/trinodb/trino/pull/8559) - Trino's Oracle pool does not reset auto-commit, so the connector does it.

## Summary

Two opt-in changes in `presto-base-jdbc`, proposed as two phases:

1. **Reuse within a query.** The metadata calls of one query share a connection instead of opening one each. Nothing is reused across queries, and splits and page sinks are not affected.
2. **An optional shared pool.** One pool per set of credentials, which a connector can replace with its driver's own pool or refuse.

## Background

`BaseJdbcClient` opens a new connection for every metadata call and closes it afterwards. Reading `information_schema.columns` for a schema of 20 tables and 20 views makes the PostgreSQL connector open 84 connections, and at 25 ms round trip that query takes 18.7 s.

Pooling exists today only for Oracle ([#27638](https://github.com/prestodb/presto/pull/27638)), whose driver ships one.

### Goals

* Fewer connections per query for metadata, with no change for connectors or catalogs that do not opt in.
* A pool that is safe with credential pass-through and under concurrent reads and writes.
* Let a connector use its driver's pool instead, or refuse pooling.

### Non-goals

* Reusing connections across queries or users.
* Changing how splits and page sinks get their connections, beyond the optional pool.
* Reducing the number of metadata queries sent to the database. That is a separate change, described under Other Approaches Considered.

## Proposed Implementation

### Modules

`presto-base-jdbc` only. No connector needs to change. A connector that overrides metadata methods and wants reuse there switches those call sites to the new method below.

### Terms used below

* **Metadata connection:** opened by a metadata call, which closes it before returning. These are the only connections that are reused.
* **Data connection:** held by a split or a page sink across many calls, possibly on different threads. These are never reused.
* **Kept connection:** a metadata connection held for a short time after it is closed, so that the next metadata call of the same query can take it.
* **Dirty connection:** a connection whose session a caller changed (read-only, auto-commit, catalog, schema, isolation, savepoints, and similar). It is closed instead of kept.
* **Credential set:** the connection properties a pool is keyed by, including user and password, which differ per user when credentials are passed through.

### Interfaces

`ConnectionFactory` gets two default methods, so existing implementations still compile:

```java
public interface ConnectionFactory extends AutoCloseable
{
    // Splits, page sinks and any existing caller. Unchanged, never reused.
    Connection openConnection(JdbcIdentity identity) throws SQLException;

    // Metadata calls. May return a connection another metadata call of the same query used.
    default Connection openConnection(ConnectorSession session, JdbcIdentity identity) throws SQLException
    {
        return openConnection(identity);
    }

    // The query in the session has ended; release anything kept for it.
    default void cleanupQuery(ConnectorSession session) {}
}
```

`JdbcClient` gets a default `cleanupQuery(ConnectorSession)`. `JdbcMetadata.cleanupQuery` calls it, and `BaseJdbcClient` passes it to the connection factory. The engine already calls `ConnectorMetadata.cleanupQuery` when a query finishes (`QueryStateMachine`), so no SPI change is needed.

The metadata methods of `BaseJdbcClient` that have a session call the new method: `getSchemaNames`, `getTableNames`, `getTableHandle`, `getColumns`, `createTable`, `addColumn`, `renameColumn`, `dropColumn`, `dropTable`, `truncateTable`.

Pooling is internal to `DriverConnectionFactory`. A connector that wants its driver's pool supplies its own `ConnectionFactory`, which is where a driver pool such as Oracle's UCP ([#27638](https://github.com/prestodb/presto/pull/27638)) fits. Reuse works on top of either.

**Opting out.** A connector that supplies its own `ConnectionFactory` is never pooled, because pooling lives only inside `DriverConnectionFactory`. That is where a driver's own pool, such as Oracle's UCP, fits. For a connector that does use `DriverConnectionFactory` but whose backend should not be pooled, it passes a flag when it creates the factory; a catalog of that connector setting `connection-pool.enabled=true` then fails to load with a message naming the connector. That flag is not in the prototype yet.

Trino has no equivalent. Its pool lives inside the Oracle connector, so there is nothing for another connector to refuse, and its reuse (`query.reuse-connection`) is a catalog property that defaults to on with no way for a connector to disable it. Here both features default to off, and a connector can refuse either.

### How it works

Reuse (`connection-reuse.enabled=true`):

* Connections are keyed by query id, identity and connection properties.
* A metadata call nested inside another on the same thread gets the connection its caller holds. This keeps a pool smaller than the nesting depth from deadlocking.
* When a metadata call closes its connection, the connection is kept for `connection-reuse.retention`. The next metadata call of the same query takes it, on any thread. Only one caller holds a kept connection at a time.
* At `cleanupQuery` the query's kept connections are closed. A connection released after that is closed, not kept. The engine does not call `cleanupQuery` for every query (for example, metadata-only queries), so the retention time is the backstop.
* A dirty connection is closed on release. Nothing is reset, so no reset can fail on any driver.
* Each caller gets its own handle, so closing twice or using a handle after closing cannot affect another caller.
* At most `connection-reuse.max-retained` connections are kept across all queries.

Pool (`connection-pool.enabled=true`), Apache Commons DBCP 2:

* One pool per credential set, at most `max-credential-sets` pools. A pool whose credentials go unused for `max-idle-time` is closed.
* Metadata calls wait up to `max-wait` for a connection.
* Splits and page sinks never wait. They take an idle connection or open their own. A query that joins two tables of the catalog, or reads from it while writing to it, holds one connection and needs another. With enough such queries every connection is held by a caller waiting for a second, and the waiting callers occupy the engine's threads. `max-size` therefore bounds the connections the pool keeps, not the total at the database.
* On hand-out, read-only and auto-commit are restored, an open transaction is rolled back, and `reset-sql` runs if set.
* Connections are retired after `max-lifetime`, so a password change or revoked grant is picked up.
* Validation on borrow is on by default, with a 5 s timeout.
* A caller whose pool is evicted while it waits is served by the replacement pool.

Configuration, all per catalog:

| Property | Default |
| --- | --- |
| `connection-reuse.enabled` | `false` |
| `connection-reuse.retention` | `2s` |
| `connection-reuse.max-retained` | `10` |
| `connection-pool.enabled` | `false` |
| `connection-pool.max-size` | `10` |
| `connection-pool.max-wait` | `30s` |
| `connection-pool.max-idle-time` | `5m` (minimum `1s`) |
| `connection-pool.max-lifetime` | `30m` |
| `connection-pool.max-credential-sets` | `100` |
| `connection-pool.validate-on-borrow` | `true` |
| `connection-pool.reset-sql` | unset |

## Metrics

Exposed per pool: pools created, connections borrowed, total borrow wait, and connections opened outside the pool because it was exhausted. The prototype has these as getters; exporting them through JMX is still to do.

To judge whether the change is worth having, measure:

* Connections opened per query, counted at the database (PostgreSQL `log_connections`).
* Latency of metadata queries and scans against a database with realistic round-trip time.
* How often splits open a connection outside the pool, which shows whether `max-size` is too small.

## Other Approaches Considered

**Each connector uses its driver's pool.** This works for Oracle, and this design allows it: the connector supplies its own `ConnectionFactory`. It does not work for most other drivers, which do not ship one and say so:

* PostgreSQL JDBC: "In general it is not recommended to use the PostgreSQL provided connection pool. Check your application server or check out the excellent jakarta commons DBCP project." ([docs](https://jdbc.postgresql.org/documentation/datasource/))
* Microsoft JDBC Driver for SQL Server: "it does not provide its own pooling implementation." ([docs](https://learn.microsoft.com/en-us/sql/connect/jdbc/using-connection-pooling))
* MySQL Connector/J relies on the pool of an application server or a pooling library. ([docs](https://dev.mysql.com/doc/connector-j/en/connector-j-usagenotes-j2ee-concepts-connection-pooling.html))
 Writing a pool per connector also repeats the parts that are hard to get right, and each of those parts has gone wrong in the one per-connector pool that exists. In Trino's Oracle pool: per-user credentials are ignored ([#19205](https://github.com/trinodb/trino/issues/19205) and [#30713](https://github.com/trinodb/trino/pull/30713), both open), auto-commit is not reset so the connector resets it itself ([#8559](https://github.com/trinodb/trino/pull/8559), fixed), and concurrent inserts exhaust the pool ([#6801](https://github.com/trinodb/trino/issues/6801), fixed). In `presto-base-jdbc` each of those is fixed once for every connector; per connector, each is fixed again. The reuse in Phase 1 is not pooling at all. It changes when `BaseJdbcClient` opens connections, which only `presto-base-jdbc` can do. Trino did it in the same place.

**Reuse keyed by thread, applied to all connections.** This was the first prototype. It needed no interface change, but it reused connections across queries of the same user, and it shared data connections that split and page sinks hold across calls. Stress tests found two page sinks sharing one transaction, so an aborted insert's rows were committed, and connections handed to two threads at once. It was also slower: with 8 concurrent metadata queries it took 4.8 s against 2.6 s for query-scoped reuse, because it cannot reuse across the threads of one query.

**A pool that makes every caller wait.** Measured with a pool of 4: 12 concurrent `INSERT ... SELECT` queries all failed with a pool timeout, and joins hung for minutes, longer than `max-wait`, because the waiting splits held the engine's threads.

**Pool without reuse.** Faster than no pool, but validation on borrow costs one round trip per metadata call (4.9 s against 2.6 s with reuse on top), and nested calls can deadlock a small pool.

**Metadata caching.** `metadata-cache-ttl` does not cover `information_schema` listing or splits (17.4 s cold and warm on the query above). It also cannot be set on its own; that will be filed as a separate issue.

**HikariCP instead of DBCP 2.** Either works. DBCP 2 is already in Presto's dependency management, so the prototype uses it.

**Opening fewer connections.** Trino's `LazyConnectionFactory` avoids opening connections that are never used, and some metadata paths could pass the connection they hold instead of opening another. Both are complementary and can be done separately.

## Adoption Plan

* **Impact on existing users:** none unless a catalog sets the new properties. The SPI additions are default methods, so out-of-tree connectors compile unchanged and keep the current behaviour.
* **Phasing:** Phase 1 (reuse) is a small change and delivers most of the benefit. Phase 2 (pool, and the connector opt-out) follows as a separate PR once Phase 1 is in. A later release could turn reuse on by default, as Trino does, after it has run in production.
* **Migration:** none. No behaviour is removed.
* **Documentation:** a section in the JDBC connector docs covering the properties and how to size the pool. Total connections at the database are roughly nodes × credential sets × `max-size`, plus splits that open their own when the pool is exhausted. It also needs a `reset-sql` value per database, and a note that driver timeouts (for PostgreSQL, `loginTimeout` and `socketTimeout`) are still needed for a database that stops responding.
* **Security:** reuse never crosses queries or identities. With the pool, Presto users who share one database account can be served the same database session one after another, so session state can pass between them unless `reset-sql` clears it. `DISCARD ALL` does on PostgreSQL. MySQL has no SQL statement for this. With credential pass-through there is one pool per user, bounded by `max-credential-sets`.
* **Out of scope:** `JdbcMetadataCache` merges concurrent lookups of the same table by different users, so one user can receive another's result or error. This happens with reuse and pooling off, and will be filed separately. Lazy connections and retrying failed connection attempts, which Trino has, are also left for later.

## Test Plan

**Unit tests (H2, in the prototype, 35 tests):**

* Nested calls share a connection. Reuse never crosses queries or identities.
* A kept connection moves to another thread of the same query, but a held one is not shared across threads.
* `cleanupQuery` closes kept connections, including one released after the query ended.
* Dirty connections are closed, not kept.
* Double close and use after close.
* The retention cap and expiry.
* Splits and page sinks never share a connection, including two writers interleaved on one thread and closed on different threads.
* Splits do not wait on an exhausted pool, while metadata calls do and time out.
* A waiter on an evicted pool is served by the replacement.
* `ForwardingConnection` forwards every `Connection` method.
* Configuration defaults and mappings.

**Integration tests:** existing `presto-base-jdbc` suites pass, except 4 join push-down tests that fail the same way on master. A product test against PostgreSQL with each mode is proposed.

**Proof of concept.** PostgreSQL 16 in Docker behind a 25 ms round-trip proxy, through the `postgresql` connector. Median of 5 runs. Connections were counted at the database.

| Query | No change | Reuse | Pool | Reuse + pool |
| --- | --- | --- | --- | --- |
| `information_schema.columns`, 20 tables + 20 views | 18.7 s, 84 connections | 2.8 s, 1 | 4.9 s | 2.6 s |
| `count(*)` on one table | 0.70 s, 3 | 0.45 s, 2 | 0.27 s | 0.20 s |
| Three-table join | 1.51 s, 9 | 0.65 s, 4 | 0.52 s | 0.39 s |
| 8 of the first query at once | 17.3 s, 112 | 2.6 s, 8 | 4.8 s | 2.6 s |

The pool columns open few or no new connections because the pool already holds them from earlier runs. Its 4.9 s on the first row is validation on borrow, one round trip per metadata call.

Stress tests against the same database, direct: 6 database accounts with credential pass-through, 48 concurrent mixed queries, concurrent and cancelled inserts, `LIMIT` scans, more credential sets than `max-credential-sets`, wrong passwords, backends killed between queries, and a network blackhole.

* In a 3 minute soak of 3,129 queries across three catalogs and six accounts, every result was correct. Every row reported the expected database account, and all 783 inserts had the expected row count.
* No backends were left open afterwards beyond the pools' idle connections.

**Not yet tested:** a multi-node cluster (all tests ran on one node that is both coordinator and worker), MySQL and Teradata with the current prototype, and the connector opt-out, which is not implemented yet.
