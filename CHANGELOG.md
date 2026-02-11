# Change Log

All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org/) and [Keep a CHANGELOG](http://keepachangelog.com/).

## [unreleased] - unreleased

### Fixed

- Fix ErrorResponseMessage routine field never being populated ([PR #60](https://github.com/ponylang/postgres/pull/60))
- Fix zero-row SELECT producing RowModifying instead of ResultSet ([PR #65](https://github.com/ponylang/postgres/pull/65))
- Fix double-delivery of pg_query_failed on failed transactions ([PR #67](https://github.com/ponylang/postgres/pull/67))
- Process query cycle messages synchronously to prevent double-delivery ([PR #69](https://github.com/ponylang/postgres/pull/69))

### Added

- Add parameterized queries via extended query protocol ([PR #70](https://github.com/ponylang/postgres/pull/70))
- Add named prepared statement support ([PR #78](https://github.com/ponylang/postgres/pull/78))
- Add SSL/TLS negotiation support ([PR #79](https://github.com/ponylang/postgres/pull/79))

### Changed

- Change ResultReceiver and Result to use Query union type instead of SimpleQuery ([PR #70](https://github.com/ponylang/postgres/pull/70))
- Fix typo in SesssionNeverOpened ([PR #59](https://github.com/ponylang/postgres/pull/59))

## [0.2.2] - 2025-07-16

### Changed

- Changed SSL dependency ([PR #54](https://github.com/ponylang/postgres/pull/54))

## [0.2.1] - 2025-03-04

### Changed

- Update ponylang/lori dependency to 0.6.1 ([PR #52](https://github.com/ponylang/postgres/pull/52)

## [0.2.0] - 2025-03-02

### Changed

- Update ponylang/lori dependency to 0.6.0 ([PR #51](https://github.com/ponylang/postgres/pull/51))

## [0.1.1] - 2025-02-13

### Changed

- Update ponylang/lori dependency to 0.5.1 ([PR #50](https://github.com/ponylang/postgres/pull/50))

## [0.1.0] - 2023-02-12

### Added

- Initial version

