use lori = "lori"

primitive SSLDisabled
  """
  Do not use SSL. The connection is plaintext. This is the default.
  """

class val SSLPreferred
  """
  Prefer SSL but fall back to plaintext if the server refuses. The driver
  sends an SSLRequest during connection setup. If the server responds 'S',
  the TLS handshake proceeds normally. If the server responds 'N' (refusing
  SSL), the connection continues as plaintext — equivalent to PostgreSQL's
  `sslmode=prefer`.

  A TLS handshake failure (server accepts but the handshake itself fails)
  is NOT retried as plaintext — `pg_session_connection_failed` fires,
  matching PostgreSQL's `sslmode=prefer` behavior.

  The `SSLContext` controls certificate and cipher configuration. Users must
  `use lori = "lori"` in their own code to create an `SSLContext val`:

      let sslctx = recover val
        lori.SSLContext
          .> set_client_verify(false)
          .> set_server_verify(false)
      end
      SSLPreferred(sslctx)
  """
  let ctx: lori.SSLContext val

  new val create(ctx': lori.SSLContext val) =>
    ctx = ctx'

class val SSLRequired
  """
  Require SSL. The driver sends an SSLRequest during connection setup and
  aborts if the server refuses. The connection is encrypted before
  authentication begins.

  The `SSLContext` controls certificate and cipher configuration. Users must
  `use lori = "lori"` in their own code to create an `SSLContext val`:

      let sslctx = recover val
        lori.SSLContext
          .> set_client_verify(false)
          .> set_server_verify(false)
      end
      SSLRequired(sslctx)
  """
  let ctx: lori.SSLContext val

  new val create(ctx': lori.SSLContext val) =>
    ctx = ctx'

type SSLMode is (SSLDisabled | SSLPreferred | SSLRequired)
