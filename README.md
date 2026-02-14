# Postgres

Pure Pony Postgres driver

## Status

Postgres is an alpha-level package.

We welcome users who are willing to experience errors and possible application shutdowns. Your feedback on API usage and in reporting bugs is greatly appreciated.

Please note that if this library encounters a state that the programmers thought was impossible to hit, it will exit the program immediately with informational messages. Normal errors are handled in standard Pony fashion.

## Installation

* Install [corral](https://github.com/ponylang/corral)
* `corral add github.com/ponylang/postgres.git --version 0.2.2`
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

MD5 password and SCRAM-SHA-256 authentication are supported. SCRAM-SHA-256 is the default authentication method in PostgreSQL 10 and later.

KerberosV5, cleartext, SCM, GSS, SSPI, SCRAM-SHA-256-PLUS (channel binding), and certificate authentication methods are not supported.

### SSL/TLS

Optional SSL/TLS encryption is supported. Pass `SSLRequired` with an `SSLContext` to `ServerConnectInfo` to enable encrypted connections. If the server refuses SSL negotiation, the connection fails. Plaintext connections are the default.

### Commands

Simple queries, parameterized queries (extended query protocol), named prepared statements, query cancellation, and LISTEN/NOTIFY are supported.

Some functionality that isn't yet supported is:

* Supplying connection configuration to the server
* Pipelining queries
* Function calls

Note the appearance of an item on the above list isn't a guarantee that it will be supported in the future.

### Data Types

The following data types are fully supported and will be converted from their postgres type to the corresponding Pony type. All other data types will be presented as `String`.

* `bool` => `Bool`
* `bytea` => `Array[U8]`
* `int2` => `I16`
* `int4` => `I32`
* `int8` => `I64`
* `float4` => `F32`
* `float8` => `F64`

As `String` is our default type, all character types such as `text` are returned to the user as `String` and as such, aren't listed in our supported types.
