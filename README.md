# Postgres

Pure Pony Postgres driver

## Status

postgres is beta quality software that will change frequently. Expect breaking changes. That said, you should feel comfortable using it in your projects.

## Installation

* Install [corral](https://github.com/ponylang/corral)
* `corral add github.com/ponylang/postgres.git --version 0.3.0`
* `corral fetch` to fetch your dependencies
* `use "postgres"` to include this package
* `corral run -- ponyc` to compile your application

This library has a transitive dependency on [ponylang/ssl](https://github.com/ponylang/ssl). It requires a C SSL library to be installed. Please see the [ssl installation instructions](https://github.com/ponylang/ssl?tab=readme-ov-file#installation) for more information.

## API Documentation

[https://ponylang.github.io/postgres](https://ponylang.github.io/postgres)

## Examples

The [examples](examples/) directory contains self-contained programs demonstrating different parts of the library. See [examples/README.md](examples/README.md) for descriptions.

## Postgres API Support

This library aims to support the Postgres API to the level required to use Postgres from Pony in ways that the Pony community needs. We do not aim to support the entire API surface. If there is functionality missing, we will happily accept high-quality pull requests to add additional support so long as  they don't come with additional external dependencies or overly burdensome maintenance.

### Authentication

Cleartext password, MD5 password, and SCRAM-SHA-256 authentication are supported. SCRAM-SHA-256 is the default authentication method in PostgreSQL 10 and later. The server chooses which method to use based on its `pg_hba.conf` configuration — the driver detects the server's request and responds automatically using the credentials from `DatabaseConnectInfo`.

KerberosV5, SCM, GSS, SSPI, SCRAM-SHA-256-PLUS (channel binding), and certificate authentication methods are not supported.

### SSL/TLS

SSL is disabled by default (`SSLDisabled`). Two SSL modes are available: `SSLRequired` mandates encryption — if the server refuses SSL negotiation, the connection fails. `SSLPreferred` attempts encryption but falls back to a plaintext connection if the server refuses. Pass either mode with an `SSLContext` to `ServerConnectInfo` to enable.

### Commands

Simple queries, parameterized queries (extended query protocol), named prepared statements, query cancellation, LISTEN/NOTIFY, COPY FROM STDIN, COPY TO STDOUT, streaming queries (portal-based cursors with windowed batch delivery), query pipelining, statement timeout (per-query automatic cancellation), and connection timeout are supported.

Some functionality that isn't yet supported is:

* Supplying connection configuration to the server
* Function calls

Note the appearance of an item on the above list isn't a guarantee that it will be supported in the future.

### Data Types

The following PostgreSQL types are decoded to typed Pony values:

* `bool` => `Bool`
* `bytea` => `Bytea`
* `int2` => `I16`
* `int4` => `I32`
* `int8` => `I64`
* `float4` => `F32`
* `float8` => `F64`
* `date` => `PgDate`
* `time` => `PgTime`
* `timestamp` / `timestamptz` => `PgTimestamp`
* `interval` => `PgInterval`
* 1-dimensional arrays of any supported element type => `PgArray`

Text-like types (`text`, `varchar`, `char`, `name`, `bpchar`, `xml`) and types with text representations (`oid`, `numeric`, `uuid`, `json`, `jsonb`) are returned as `String`. Unknown types in text format are also returned as `String`; unknown types in binary format are returned as `RawBytes`.

User-defined enum types can be registered with `CodecRegistry.with_enum_type()`, which decodes them as `String`. User-defined composite types can be registered with `CodecRegistry.with_composite_type()`, which decodes them as `PgComposite`. Custom array type mappings can be added with `CodecRegistry.with_array_type()`.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for build instructions and test setup.
