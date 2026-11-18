# simply-cpp-cassandra

`sc-cassandra` is a small Cassandra client: CQL queries with bound values over
the native protocol (version 4), spoken directly over a socket, with no driver
or other library underneath. Rows come back with their columns' types, and a
result over several pages comes back whole.

Give the constructor contact points as an `sc::ip_endpoints`: a `;`-separated
string, a `std::vector<sc::ip_endpoint>` or a braced list. Entries without a
port use 9042. It connects to the first that answers, and replaces a connection
that broke once, transparently. One `sc::cassandra::client` can be shared by threads;
their queries take turns on its one connection.

**Documentation:** the [simply-cpp wiki](https://github.com/roelofrossouw/simply-cpp/wiki) has a [Cassandra guide](https://github.com/roelofrossouw/simply-cpp/wiki/Cassandra) and the [sc-cassandra reference](https://github.com/roelofrossouw/simply-cpp/wiki/Reference-sc-cassandra), plus [getting started](https://github.com/roelofrossouw/simply-cpp/wiki/Getting-Started) for the whole suite.

```cpp
#include <sc/cassandra/cassandra.h>

sc::cassandra::client db{"cassandra.example.com:9042"};
const auto rows = db.query(
    "select ordered_at, status, total from shop.orders "
    "where customer_id = ? and ordered_at <= ? limit ?",
    {std::int64_t{389}, std::chrono::system_clock::now(), 10});
for (const auto &row: rows) {
    const auto when = row["ordered_at"].as<sc::cassandra::timestamp>();
    const auto total = row["total"].as<double>();
}
```

## Values

A bound value's C++ type decides how it is sent, so give the type the column
has:

| C++ | CQL |
|---|---|
| `std::int32_t` (a plain `10`) | `int`, and `LIMIT ?` |
| `std::int64_t` | `bigint`, `counter` |
| `double`, `float` | `double`, `float` |
| `bool` | `boolean` |
| `std::string`, `"text"` | `text`, `ascii`; `uuid`, `inet` and `date` as text |
| a `system_clock` time point | `timestamp` (to the millisecond) |
| `sc::cassandra::blob` | `blob` |
| `nullptr` | null |

Columns arrive the same way: `smallint` and `tinyint` as `std::int32_t`, `time`
(nanoseconds since midnight) as `std::int64_t`, `uuid` and `timeuuid` as
`8-4-4-4-12` text, `inet` as `10.0.0.1` or `::1`, `date` as `2026-10-20`.
`decimal`, `varint`, `duration`, collections, user types, tuples and custom
types arrive as their bytes, an `sc::cassandra::blob`. `value.as<T>()` gives the value;
an `int` also reads as `std::int64_t` or `double`, a `float` as `double`.
`value.text()` shows any value, a timestamp as ISO 8601 in UTC.

## Errors

- `sc::cassandra::unavailable`: no contact point answered, or the connection
  broke again after its one retry.
- `sc::cassandra::error`: Cassandra answered with an error; `code` is the protocol's
  (`0x2000` syntax error, `0x2200` invalid, `0x1200` read timeout, `0x0100` a
  refused login ...). The connection stays usable.

`sc::cassandra::options` sets a login (PasswordAuthenticator), a keyspace to
`USE`, the consistency (`sc::cassandra::consistency_level::one` by default; a query can give
its own), and the connect and reply timeouts (2 s and 10 s).

## Demo

`sc-cassandra-demo --hosts HOSTS [QUERY]` runs a query and prints the rows as a
table with the columns' types; without a query, the server's version and its
keyspaces.

<!-- sc-example: examples/sc-cassandra-demo.cpp -->
```cpp
sc::cassandra::client db{args.as<std::string>("hosts"), {
    .user = args.as<std::string>("user"),
    .password = args.as<std::string>("password"),
    .keyspace = args.as<std::string>("keyspace"),
}};
std::cout << "Cassandra " << db.release_version() << " at " << db.connected_to().to_string() << "\n\n";
const auto cql = args.as<std::string>("query");
print(cql.empty() ? db.query("select keyspace_name, durable_writes from system_schema.keyspaces") : db.query(cql));
```
<!-- /sc-example -->

## Building locally

Run `bash scripts/install.sh` to configure, build, test, package and install.

## Tests

`protocol` (label `unit`) runs the client against a small fake Cassandra on
loopback: every decoded type, bound values, paging, errors, EVENT frames,
dropped connections, logins. `server` runs against a real Cassandra when
`SC_CASSANDRA_TEST_HOSTS` names one (`host[:port]`, `;`-separated; on the build
servers in `/etc/simply-cpp/test.env`), reading system tables only; without it,
it says so and passes.

## Limits

- Protocol version 4 only (Cassandra 2.2 and later, ScyllaDB); no compression,
  no TLS, no prepared statements (each query is sent as text with its values).
- One connection, to one node: no token-aware routing or load balancing, and no
  speculative retries. Queries from several threads wait their turn.
- Collections, user types, tuples, `decimal`, `varint` and `duration` aren't
  decoded (they arrive as bytes), and can't be bound except as a blob.
