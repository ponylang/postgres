# Development

## Prerequisites

- [ponyc](https://github.com/ponylang/ponyc)
- [corral](https://github.com/ponylang/corral)
- OpenSSL or LibreSSL (C library, for the [ssl](https://github.com/ponylang/ssl) dependency)
- [Docker](https://www.docker.com/) (for integration tests only)

## Building and Testing

Build and test targets require an SSL version flag matching your system: `3.0.x` (OpenSSL 3.x), `1.1.x` (OpenSSL 1.1.x), or `libressl`. Run `openssl version` to check.

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make ssl=<ver>` | Build and run all tests (unit + integration) and compile examples. Integration tests require running PostgreSQL containers (see below) |
| `make unit-tests ssl=<ver>` | Run unit tests only (no PostgreSQL needed) |
| `make integration-tests ssl=<ver>` | Run integration tests only |
| `make test-one t=TestName ssl=<ver>` | Run a single test by name |
| `make examples ssl=<ver>` | Compile all example programs |
| `make clean` | Remove build artifacts and corral dependencies |

`unit-tests` and `integration-tests` run with `--sequential`. Pass `config=debug` to compile with debug symbols, e.g., `make unit-tests ssl=3.0.x config=debug`.

## Integration Test Setup

Integration tests require two PostgreSQL 14.5 containers — one plain and one with SSL enabled.

### Starting and Stopping Containers

`make start-pg-containers` starts both containers. `make stop-pg-containers` stops and removes them.

The containers start in the background. PostgreSQL takes a few seconds to initialize — wait for it before running tests:

```bash
docker logs -f pg 2>&1 | grep -m1 "database system is ready to accept connections"
```

This starts:

- **Plain** on port 5432 — SCRAM-SHA-256 default auth, with an additional MD5-only user
- **SSL** on port 5433 — same as plain, plus SSL enabled using test certificates from `assets/`

Both containers use the same credentials:

| | Username | Password | Database |
|-|----------|----------|----------|
| Default | `postgres` | `postgres` | `postgres` |
| MD5 user | `md5user` | `md5pass` | `postgres` |

### Environment Variables

Tests read connection details from environment variables. The defaults match the container setup above, so you typically don't need to set these unless your containers are on a different host or port.

| Variable | Default |
|----------|---------|
| `POSTGRES_HOST` | `127.0.0.1` |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_SSL_HOST` | value of `POSTGRES_HOST` |
| `POSTGRES_SSL_PORT` | `5433` |
| `POSTGRES_USERNAME` | `postgres` |
| `POSTGRES_PASSWORD` | `postgres` |
| `POSTGRES_DATABASE` | `postgres` |
| `POSTGRES_MD5_USERNAME` | `md5user` |
| `POSTGRES_MD5_PASSWORD` | `md5pass` |
