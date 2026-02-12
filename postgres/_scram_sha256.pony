use "ssl/crypto"

primitive _ScramSha256
  """
  Pure computation functions for the SCRAM-SHA-256 authentication exchange,
  as defined in RFC 5802 (framework) and RFC 7677 (SHA-256 mechanism).

  All functions are deterministic — nonce generation is left to the caller.
  """
  fun client_first_message_bare(nonce: String): String =>
    """
    Returns the client-first-message-bare: `n=,r=<nonce>`.
    The username is empty per libpq convention (PostgreSQL ignores it,
    using the startup message username instead).
    """
    "n=,r=" + nonce

  fun client_first_message(nonce: String): String =>
    """
    Returns the full client-first-message: `n,,` + client-first-message-bare.
    The GS2 header `n,,` indicates no channel binding.
    """
    "n,," + client_first_message_bare(nonce)

  fun client_final_message_without_proof(combined_nonce: String): String =>
    """
    Returns the client-final-message without the proof:
    `c=biws,r=<combined_nonce>`. `biws` is base64("n,,").
    """
    "c=biws,r=" + combined_nonce

  fun client_final_message(combined_nonce: String, client_proof_b64: String)
    : String
  =>
    """
    Returns the complete client-final-message with proof appended.
    """
    client_final_message_without_proof(combined_nonce) + ",p=" + client_proof_b64

  fun compute_proof(password: String, salt: Array[U8] val, iterations: U32,
    client_first_bare: String, server_first: String, combined_nonce: String)
    : (Array[U8] val, Array[U8] val) ?
  =>
    """
    Compute the SCRAM client proof and server signature.

    Returns `(client_proof, server_signature)`. Partial because PBKDF2 can
    fail (e.g., zero iterations).

    The computation follows RFC 5802 Section 3:
    1. SaltedPassword = PBKDF2(password, salt, iterations, 32)
    2. ClientKey = HMAC(SaltedPassword, "Client Key")
    3. StoredKey = SHA256(ClientKey)
    4. AuthMessage = client_first_bare + "," + server_first + ","
                     + client_final_without_proof
    5. ClientSignature = HMAC(StoredKey, AuthMessage)
    6. ClientProof = ClientKey XOR ClientSignature
    7. ServerKey = HMAC(SaltedPassword, "Server Key")
    8. ServerSignature = HMAC(ServerKey, AuthMessage)
    """
    let salted_password = Pbkdf2Sha256(password, salt, iterations, 32)?
    let client_key = HmacSha256(salted_password, "Client Key")
    let stored_key = SHA256(client_key)
    let auth_message: String val = recover val
      client_first_bare + "," + server_first + ","
        + client_final_message_without_proof(combined_nonce)
    end
    let client_signature = HmacSha256(stored_key, auth_message)

    // ClientProof = ClientKey XOR ClientSignature
    let client_proof = recover iso
      let proof = Array[U8](32)
      var i: USize = 0
      while i < 32 do
        try
          proof.push(client_key(i)? xor client_signature(i)?)
        else
          error
        end
        i = i + 1
      end
      proof
    end

    let server_key = HmacSha256(salted_password, "Server Key")
    let server_signature: Array[U8] val = HmacSha256(server_key, auth_message)

    (consume client_proof, server_signature)
