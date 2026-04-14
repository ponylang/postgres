## Guard against integer overflow on server-supplied message lengths

On 32-bit platforms, a PostgreSQL server that declared a message length near `U32.max` could wrap the driver's internal size calculation to a small value (including 0). The buffer-size check then passed incorrectly and the parser could return a phantom acknowledgement message — a bogus success. The driver now validates the size arithmetic and rejects such messages as a protocol violation immediately. 64-bit platforms were not affected.
