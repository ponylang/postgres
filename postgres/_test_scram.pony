use "encode/base64"
use "pony_test"
use "ssl/crypto"

class \nodoc\ iso _TestScramSha256MessageBuilders is UnitTest
  fun name(): String =>
    "SCRAM/MessageBuilders"

  fun apply(h: TestHelper) =>
    let nonce = "rOprNGfwEbeRWgbNEkqO"
    let combined = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"

    h.assert_eq[String]("n=,r=rOprNGfwEbeRWgbNEkqO",
      _ScramSha256.client_first_message_bare(nonce))

    h.assert_eq[String]("n,,n=,r=rOprNGfwEbeRWgbNEkqO",
      _ScramSha256.client_first_message(nonce))

    h.assert_eq[String](
      "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0",
      _ScramSha256.client_final_message_without_proof(combined))

    h.assert_eq[String](
      "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=PROOF",
      _ScramSha256.client_final_message(combined, "PROOF"))

class \nodoc\ iso _TestScramSha256ComputeProof is UnitTest
  """
  Verify SCRAM-SHA-256 computation against known test vectors. Uses the
  RFC 5802 Section 5 inputs (password "pencil", known nonces and salt) with
  independently computed SHA-256 expected values (the RFC's expected values
  are for SHA-1 and don't apply here).
  """
  fun name(): String =>
    "SCRAM/ComputeProof"

  fun apply(h: TestHelper) ? =>
    let password = "pencil"
    let salt = Base64.decode[Array[U8] iso]("W22ZaJ0SNY7soEsUEjb6gQ==")?
    let iterations: U32 = 4096
    let client_first_bare = "n=,r=rOprNGfwEbeRWgbNEkqO"
    let server_first_iso =
      "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0" +
      ",s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
    let server_first: String val = consume server_first_iso
    let combined_nonce =
      "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"

    (let client_proof, let server_signature) =
      _ScramSha256.compute_proof(password, consume salt, iterations,
        client_first_bare, server_first, combined_nonce)?

    let expected_proof: Array[U8] val = [
      170; 244; 246; 73; 103; 68; 31; 148; 52; 233; 169; 91; 47; 232; 99; 73
      139; 148; 132; 33; 187; 86; 119; 69; 203; 50; 27; 236; 34; 184; 159; 217
    ]

    let expected_signature: Array[U8] val = [
      220; 115; 186; 66; 221; 76; 224; 194; 137; 174; 105; 74; 106; 131; 170
      44; 2; 52; 255; 68; 213; 208; 118; 94; 236; 159; 71; 220; 192; 109; 72
      232
    ]

    h.assert_eq[USize](32, client_proof.size())
    h.assert_eq[USize](32, server_signature.size())
    h.assert_array_eq[U8](expected_proof, client_proof)
    h.assert_array_eq[U8](expected_signature, server_signature)

    // Also verify Base64-encoded proof matches expected
    let proof_b64_iso = Base64.encode(client_proof)
    let proof_b64: String val = consume proof_b64_iso
    h.assert_eq[String]("qvT2SWdEH5Q06albL+hjSYuUhCG7VndFyzIb7CK4n9k=",
      proof_b64)
