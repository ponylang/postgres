# Examples

Each subdirectory is a self-contained Pony program demonstrating a different part of the postgres library.

## query

Minimal example using `SimpleQuery`. Connects, authenticates, executes `SELECT 525600::text`, and prints the result by iterating rows and matching on `FieldDataTypes`. Start here if you're new to the library.

## prepared-query

Parameterized queries using `PreparedQuery`. Sends a query with typed parameters (`text`, `int4`) and a NULL parameter, then inspects the `ResultSet`. Shows how to construct the `Array[(String | None)] val` parameter array.

## crud

Multi-query workflow mixing `SimpleQuery` and `PreparedQuery`. Creates a table, inserts rows with parameterized INSERTs, selects them back, deletes, and drops the table. Demonstrates all three `Result` types (`ResultSet`, `RowModifying`, `SimpleResult`) and `ErrorResponseMessage` error handling.
