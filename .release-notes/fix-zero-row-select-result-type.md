## Fix zero-row SELECT producing RowModifying instead of ResultSet

A `SELECT` query returning zero rows (e.g., `SELECT 1 WHERE false`) incorrectly produced a `RowModifying` result instead of a `ResultSet` with zero rows. This made it impossible to distinguish a zero-row SELECT from an INSERT/UPDATE/DELETE at the result level. Zero-row SELECTs now correctly produce a `ResultSet`.
