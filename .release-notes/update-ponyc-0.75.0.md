## Update to work with ponyc 0.75.0

Ponyc 0.75.0 made the one-shot hash functions in the `crypto` package partial so callers can detect OpenSSL failure. This updates the MD5 and SCRAM-SHA-256 authentication paths to handle the new partiality.
