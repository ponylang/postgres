use "ssl/net"

primitive SSLDisabled
  """
  Do not use SSL. The connection is plaintext. This is the default.
  """

class val SSLRequired
  """
  Require SSL. The driver sends an SSLRequest during connection setup and
  aborts if the server refuses. The connection is encrypted before
  authentication begins.

  The `SSLContext` controls certificate and cipher configuration. Users must
  `use "ssl/net"` in their own code to create an `SSLContext val`:

      let sslctx = recover val
        SSLContext
          .> set_client_verify(false)
          .> set_server_verify(false)
      end
      SSLRequired(sslctx)
  """
  let ctx: SSLContext val

  new val create(ctx': SSLContext val) =>
    ctx = ctx'

type SSLMode is (SSLDisabled | SSLRequired)
