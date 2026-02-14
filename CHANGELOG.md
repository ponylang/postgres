# Change Log

All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org/) and [Keep a CHANGELOG](http://keepachangelog.com/).

## [unreleased] - unreleased

### Fixed

- Fix ErrorResponseMessage routine field never being populated ([PR #60](https://github.com/ponylang/postgres/pull/60))
- Fix zero-row SELECT producing RowModifying instead of ResultSet ([PR #65](https://github.com/ponylang/postgres/pull/65))
- Fix double-delivery of pg_query_failed on failed transactions ([PR #67](https://github.com/ponylang/postgres/pull/67))
- Process query cycle messages synchronously to prevent double-delivery ([PR #69](https://github.com/ponylang/postgres/pull/69))
- Send Terminate message before closing TCP connection ([PR #86](https://github.com/ponylang/postgres/pull/86))
- Fix unsupported authentication type causing silent hang ([PR #97](https://github.com/ponylang/postgres/pull/97))
- Fix ReadyForQuery queue stall with explicit transactions ([PR #105](https://github.com/ponylang/postgres/pull/105))

### Added

- Add parameterized queries via extended query protocol ([PR #70](https://github.com/ponylang/postgres/pull/70))
- Add named prepared statement support ([PR #78](https://github.com/ponylang/postgres/pull/78))
- Add SSL/TLS negotiation support ([PR #79](https://github.com/ponylang/postgres/pull/79))
- Enable follow-up queries from ResultReceiver and PrepareReceiver callbacks ([PR #84](https://github.com/ponylang/postgres/pull/84))
- Add equality comparison for Field ([PR #85](https://github.com/ponylang/postgres/pull/85))
- Add equality comparison for Row ([PR #85](https://github.com/ponylang/postgres/pull/85))
- Add equality comparison for Rows ([PR #85](https://github.com/ponylang/postgres/pull/85))
- Add query cancellation support ([PR #89](https://github.com/ponylang/postgres/pull/89))
- Add SCRAM-SHA-256 authentication support ([PR #94](https://github.com/ponylang/postgres/pull/94))
- Add transaction status tracking ([PR #106](https://github.com/ponylang/postgres/pull/106))
- Add LISTEN/NOTIFY support ([PR #108](https://github.com/ponylang/postgres/pull/108))
- Add COPY IN support ([PR #112](https://github.com/ponylang/postgres/pull/112))
- Add notice response message support ([PR #117](https://github.com/ponylang/postgres/pull/117))
- Add bytea type conversion ([PR #119](https://github.com/ponylang/postgres/pull/119))
- Add ParameterStatus tracking ([PR #120](https://github.com/ponylang/postgres/pull/120))
- Add COPY TO STDOUT support ([PR #122](https://github.com/ponylang/postgres/pull/122))

### Changed

- Change ResultReceiver and Result to use Query union type instead of SimpleQuery ([PR #70](https://github.com/ponylang/postgres/pull/70))
- Change ResultReceiver and PrepareReceiver callbacks to take Session as first parameter ([PR #84](https://github.com/ponylang/postgres/pull/84))
- Change Session constructor to accept ServerConnectInfo ([PR #89](https://github.com/ponylang/postgres/pull/89))
- Fix typo in SesssionNeverOpened ([PR #59](https://github.com/ponylang/postgres/pull/59))
- Change Session constructor to accept DatabaseConnectInfo ([PR #91](https://github.com/ponylang/postgres/pull/91))

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

