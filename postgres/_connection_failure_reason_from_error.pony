primitive _ConnectionFailureReasonFromError
  fun apply(msg: ErrorResponseMessage): ConnectionFailureReason =>
    match msg.code
    | "28P01" => InvalidPassword(msg)
    | "28000" => InvalidAuthorizationSpecification(msg)
    | "53300" => TooManyConnections(msg)
    | "3D000" => InvalidDatabaseName(msg)
    else
      ServerRejected(msg)
    end
