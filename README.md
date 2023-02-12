# Postgres

Pure Pony Postgres driver

## Status

Postgres is an alpha-level package.

You shouldn't be using this as it is in active development and not ready to be used, at all. Don't. Just don't use it yet.

## Installation

* Install [corral](https://github.com/ponylang/corral)
* `corral add github.com/ponylang/postgres.git --version 0.0.0`
* `corral fetch` to fetch your dependencies
* `use "postgres"` to include this package
* `corral run -- ponyc` to compile your application

## API Documentation

[https://ponylang.github.io/postgres](https://ponylang.github.io/postgres)

## Postgres API Support

This library aims to support the Postgres API to the level required to use Postgres from Pony in ways that the Pony community needs. We do not aim to support the entire API surface. If there is functionality missing, we will happily accept high-quality pull requests to add additional support so long as  they don't come with additional external dependencies or overly burdensome maintenance.

### Authentication

Only MD5 password authentication is supported. KerberosV5, cleartext, SCM, GSS, SSPI, and SASL authentication methods are not supported.

### Commands

Basic API commands related to querying are supported at this time. Some functionality that isn't yet supported is:

- Supplying connection configuration to the server
- Prepared statements (aka Extended Queries)
- Pipelining queries
- Function calls
- COPY operations
- Cancelling in progress requests
- Session encryption

Note the appearance of an item on the above list isn't a guarantee that it will be supported in the future.

### Data Types

The following data types are fully supported and will be converted from their postgres type to the corresponding Pony type. All other data types will be presented as `String`.

- `bool` => `Bool`
- `int2` => `I16`
- `int4` => `I32`
- `int8` => `I64`
- `float4` => `F32`
- `float8` => `F64`
