use "cli"
use lori = "lori"
use "pony_check"
use "pony_test"

actor \nodoc\ Main is TestList
  new create(env: Env) =>
    PonyTest(env, this)

  new make() =>
    None

  fun tag tests(test: PonyTest) =>
    test(_TestAuthenticate)
    test(_TestAuthenticateFailure)
    test(_TestConnect)
    test(_TestConnectFailure)
    test(_TestCreateAndDropTable)
    test(_TestEmptyQuery)
    test(_TestHandlingJunkMessages)
    test(_TestInsertAndDelete)
    test(_TestFrontendMessagePassword)
    test(_TestFrontendMessageQuery)
    test(_TestFrontendMessageStartup)
    test(_TestQueryAfterAuthenticationFailure)
    test(_TestQueryAfterConnectionFailure)
    test(_TestQueryAfterSessionHasBeenClosed)
    test(_TestQueryResults)
    test(_TestQueryByteaResults)
    test(_TestQueryOfNonExistentTable)
    test(_TestResponseParserAuthenticationMD5PasswordMessage)
    test(_TestResponseParserAuthenticationOkMessage)
    test(_TestResponseParserCommandCompleteMessage)
    test(_TestResponseParserDataRowMessage)
    test(_TestResponseParserEmptyBuffer)
    test(_TestResponseParserEmptyQueryResponseMessage)
    test(_TestResponseParserErrorResponseMessage)
    test(_TestResponseParserIncompleteMessage)
    test(_TestResponseParserJunkMessage)
    test(_TestResponseParserMultipleMessagesAuthenticationMD5PasswordFirst)
    test(_TestResponseParserMultipleMessagesAuthenticationOkFirst)
    test(_TestResponseParserMultipleMessagesErrorResponseFirst)
    test(_TestResponseParserReadyForQueryMessage)
    test(_TestResponseParserRowDescriptionMessage)
    test(_TestResponseParserRowDescriptionBinaryFormat)
    test(_TestResponseParserParseCompleteMessage)
    test(_TestResponseParserBindCompleteMessage)
    test(_TestResponseParserNoDataMessage)
    test(_TestResponseParserCloseCompleteMessage)
    test(_TestResponseParserParameterDescriptionMessage)
    test(_TestResponseParserPortalSuspendedMessage)
    test(_TestResponseParserBackendKeyDataMessage)
    test(_TestResponseParserDigitMessageTypeNotJunk)
    test(_TestResponseParserMultipleMessagesParseCompleteFirst)
    test(_TestResponseParserMultipleMessagesBackendKeyDataFirst)
    test(_TestResponseParserParameterStatusMessage)
    test(_TestResponseParserNoticeResponseMessage)
    test(_TestResponseParserNotificationResponseMessage)
    test(_TestResponseParserMultipleMessagesAsyncThenAuth)
    test(_TestFrontendMessageParse)
    test(_TestFrontendMessageParseWithTypes)
    test(_TestFrontendMessageBind)
    test(_TestFrontendMessageBindWithNull)
    test(_TestFrontendMessageDescribePortal)
    test(_TestFrontendMessageExecute)
    test(_TestFrontendMessageDescribeStatement)
    test(_TestFrontendMessageCloseStatement)
    test(_TestFrontendMessageSync)
    test(_TestFrontendMessageSSLRequest)
    test(_TestFrontendMessageFlush)
    test(_TestFrontendMessageTerminate)
    test(_TestTerminateSentOnClose)
    test(_TestSSLNegotiationRefused)
    test(_TestSSLNegotiationJunkResponse)
    test(_TestSSLNegotiationSuccess)
    test(_TestUnansweredQueriesFailOnShutdown)
    test(_TestPrepareShutdownDrainsPrepareQueue)
    test(_TestZeroRowSelectReturnsResultSet)
    test(_TestByteaResultDecoding)
    test(_TestEmptyByteaResultDecoding)
    test(_TestZeroRowSelect)
    test(_TestMultiStatementMixedResults)
    test(_TestPreparedQueryResults)
    test(_TestPreparedQueryNullParam)
    test(_TestPreparedQueryNonExistentTable)
    test(_TestPreparedQueryInsertAndDelete)
    test(_TestPreparedQueryMixedWithSimple)
    test(_TestPrepareStatement)
    test(_TestPrepareAndExecute)
    test(_TestPrepareAndExecuteMultiple)
    test(_TestPrepareAndClose)
    test(_TestPrepareFails)
    test(_TestPrepareAfterClose)
    test(_TestCloseNonexistent)
    test(_TestPrepareDuplicateName)
    test(_TestPreparedStatementMixedWithSimpleAndPrepared)
    test(_TestSSLConnect)
    test(_TestSSLAuthenticate)
    test(_TestSSLQueryResults)
    test(_TestSSLRefused)
    test(_TestSSLPreferredFallback)
    test(_TestSSLPreferredSuccess)
    test(_TestSSLPreferredTLSFailure)
    test(_TestSSLPreferredCancelFallback)
    test(_TestSSLPreferredWithSSLServer)
    test(_TestSSLPreferredWithPlainServer)
    test(_TestFieldEqualityReflexive)
    test(_TestFieldEqualityStructural)
    test(_TestFieldEqualitySymmetric)
    test(_TestFieldInequality)
    test(_TestRowEquality)
    test(_TestRowInequality)
    test(_TestRowsEquality)
    test(_TestRowsInequality)
    test(Property1UnitTest[Field](_TestFieldReflexiveProperty))
    test(Property1UnitTest[FieldData](_TestFieldStructuralProperty))
    test(Property1UnitTest[(FieldData, FieldData)](
      _TestFieldSymmetricProperty))
    test(Property1UnitTest[Row](_TestRowReflexiveProperty))
    test(Property1UnitTest[Rows](_TestRowsReflexiveProperty))
    test(_TestFrontendMessageCancelRequest)
    test(_TestCancelQueryInFlight)
    test(_TestSSLCancelQueryInFlight)
    test(_TestCancelPgSleep)
    test(_TestCancelSSLPgSleep)
    test(_TestResponseParserAuthenticationSASLMessage)
    test(_TestResponseParserAuthenticationSASLContinueMessage)
    test(_TestResponseParserAuthenticationSASLFinalMessage)
    test(_TestResponseParserUnsupportedAuthenticationMessage)
    test(_TestResponseParserMultipleMessagesSASLFirst)
    test(Property1UnitTest[Array[Array[U8] val] val](
      _TestResponseParserMultipleMessagesChainProperty))
    test(_TestResponseParserMultipleMessagesChainSimpleQueryResult)
    test(_TestResponseParserMultipleMessagesChainCopyOutSequence)
    test(_TestResponseParserMultipleMessagesChainEmptyQuerySequence)
    test(_TestResponseParserMultipleMessagesChainPrepareSequence)
    test(_TestResponseParserMultipleMessagesChainCloseStatementSequence)
    test(_TestResponseParserMultipleMessagesChainSASLFullSequence)
    test(_TestResponseParserMultipleMessagesChainRemainingTypes)
    test(_TestResponseParserMultipleMessagesChainStreamingQuerySequence)
    test(_TestFrontendMessageSASLInitialResponse)
    test(_TestFrontendMessageSASLResponse)
    test(_TestScramSha256MessageBuilders)
    test(_TestScramSha256ComputeProof)
    test(_TestSCRAMAuthenticationSuccess)
    test(_TestSCRAMUnsupportedMechanism)
    test(_TestSCRAMServerVerificationFailed)
    test(_TestSCRAMErrorDuringAuth)
    test(_TestUnsupportedAuthentication)
    test(_TestMD5Authenticate)
    test(_TestMD5AuthenticateFailure)
    test(_TestMD5QueryResults)
    test(_TestTransactionCommit)
    test(_TestTransactionRollbackAfterFailure)
    test(_TestTransactionStatusOnAuthentication)
    test(_TestTransactionStatusDuringTransaction)
    test(_TestTransactionStatusOnFailedTransaction)
    test(_TestNotificationDelivery)
    test(_TestNotificationDuringDataRows)
    test(_TestListenNotify)
    test(_TestNoticeDelivery)
    test(_TestNoticeDuringDataRows)
    test(_TestNoticeOnDropIfExists)
    test(_TestParameterStatusDelivery)
    test(_TestParameterStatusDuringDataRows)
    test(_TestParameterStatusOnStartup)
    test(_TestParameterStatusOnSet)
    test(_TestResponseParserCopyInResponseMessage)
    test(_TestFrontendMessageCopyData)
    test(_TestFrontendMessageCopyDone)
    test(_TestFrontendMessageCopyFail)
    test(_TestCopyInSuccess)
    test(_TestCopyInAbort)
    test(_TestCopyInServerError)
    test(_TestCopyInShutdownDrainsCopyQueue)
    test(_TestCopyInAfterSessionClosed)
    test(_TestCopyInInsert)
    test(_TestCopyInAbortRollback)
    test(_TestResponseParserCopyOutResponseMessage)
    test(_TestResponseParserCopyDataMessage)
    test(_TestResponseParserCopyDoneMessage)
    test(_TestCopyOutSuccess)
    test(_TestCopyOutEmpty)
    test(_TestCopyOutServerError)
    test(_TestCopyOutShutdownDrainsCopyQueue)
    test(_TestCopyOutAfterSessionClosed)
    test(_TestCopyOutExport)
    test(_TestStreamingSuccess)
    test(_TestStreamingEmpty)
    test(_TestStreamingEarlyStop)
    test(_TestStreamingServerError)
    test(_TestStreamingShutdownDrainsQueue)
    test(_TestStreamingQueryResults)
    test(_TestStreamingAfterSessionClosed)
    test(_TestPipelineSuccess)
    test(_TestPipelineWithFailure)
    test(_TestPipelineEmpty)
    test(_TestPipelineSingleQuery)
    test(_TestPipelineShutdownDrainsQueue)
    test(_TestPipelineShutdownInFlight)
    test(_TestPipelineRowModifying)
    test(_TestPipelineMixedQueryTypes)
    test(_TestPipelineAllFail)
    test(_TestPipelineIntegration)
    test(_TestPipelineIntegrationWithFailure)
    test(_TestBoolBinaryCodecRoundtrip)
    test(_TestBoolBinaryCodecNonzeroTrue)
    test(_TestBoolBinaryCodecBadLength)
    test(_TestBoolBinaryCodecTypeMismatch)
    test(_TestInt2BinaryCodecRoundtrip)
    test(_TestInt2BinaryCodecBadLength)
    test(_TestInt2BinaryCodecTypeMismatch)
    test(_TestInt4BinaryCodecRoundtrip)
    test(_TestInt4BinaryCodecBadLength)
    test(_TestInt4BinaryCodecTypeMismatch)
    test(_TestInt8BinaryCodecRoundtrip)
    test(_TestInt8BinaryCodecBadLength)
    test(_TestInt8BinaryCodecTypeMismatch)
    test(_TestFloat4BinaryCodecRoundtrip)
    test(_TestFloat4BinaryCodecBadLength)
    test(_TestFloat4BinaryCodecTypeMismatch)
    test(_TestFloat8BinaryCodecRoundtrip)
    test(_TestFloat8BinaryCodecBadLength)
    test(_TestFloat8BinaryCodecTypeMismatch)
    test(_TestByteaBinaryCodecRoundtrip)
    test(_TestByteaBinaryCodecEmpty)
    test(_TestBoolTextCodecRoundtrip)
    test(_TestInt4TextCodecRoundtrip)
    test(_TestByteaTextCodecRoundtrip)
    test(_TestByteaTextCodecBadHex)
    test(_TestCodecRegistryDecodeUnknownText)
    test(_TestCodecRegistryDecodeUnknownBinary)
    test(_TestCodecRegistryDecodeKnown)
    test(_TestCodecRegistryHasBinaryCodec)
    test(_TestParamEncoderOids)
    test(_TestFrontendMessageBindWithBinaryI32)
    test(_TestFrontendMessageBindMixedParams)
    test(_TestFrontendMessageBindEmptyParams)
    test(_TestFrontendMessageBindTemporalParams)
    test(_TestTextPassthroughBinaryCodecDecode)
    test(_TestTextPassthroughBinaryCodecEncode)
    test(_TestTextPassthroughBinaryCodecEmpty)
    test(_TestTextPassthroughBinaryCodecTypeMismatch)
    test(_TestOidBinaryCodecRoundtrip)
    test(_TestOidBinaryCodecBadLength)
    test(_TestOidBinaryCodecTypeMismatch)
    test(_TestNumericBinaryCodecPositiveInteger)
    test(_TestNumericBinaryCodecNegative)
    test(_TestNumericBinaryCodecNaN)
    test(_TestNumericBinaryCodecZero)
    test(_TestNumericBinaryCodecFractional)
    test(_TestNumericBinaryCodecPreservedDscale)
    test(_TestNumericBinaryCodecLessThanOne)
    test(_TestNumericBinaryCodecTooShort)
    test(_TestNumericBinaryCodecNdigitsMismatch)
    test(_TestNumericBinaryCodecInfinity)
    test(_TestNumericBinaryCodecEncodeErrors)
    test(_TestNumericBinaryCodecBadSign)
    test(_TestNumericBinaryCodecMaxWeight)
    test(_TestUuidBinaryCodecRoundtrip)
    test(_TestUuidBinaryCodecAllZeros)
    test(_TestUuidBinaryCodecAllFF)
    test(_TestUuidBinaryCodecBadLength)
    test(_TestUuidBinaryCodecBadStringFormat)
    test(_TestJsonbBinaryCodecRoundtrip)
    test(_TestJsonbBinaryCodecBadVersion)
    test(_TestJsonbBinaryCodecEmpty)
    test(_TestJsonbBinaryCodecTypeMismatch)
    test(_TestDateBinaryCodecRoundtrip)
    test(_TestDateBinaryCodecInfinity)
    test(_TestDateBinaryCodecBadLength)
    test(_TestDateBinaryCodecTypeMismatch)
    test(_TestTimeBinaryCodecRoundtrip)
    test(_TestTimeBinaryCodecBadLength)
    test(_TestTimeBinaryCodecTypeMismatch)
    test(_TestTimestampBinaryCodecRoundtrip)
    test(_TestTimestampBinaryCodecInfinity)
    test(_TestTimestampBinaryCodecBadLength)
    test(_TestTimestampBinaryCodecTypeMismatch)
    test(_TestIntervalBinaryCodecRoundtrip)
    test(_TestIntervalBinaryCodecBadLength)
    test(_TestIntervalBinaryCodecTypeMismatch)
    test(_TestDateTextCodecDecode)
    test(_TestDateTextCodecInfinity)
    test(_TestDateTextCodecEncode)
    test(_TestDateTextCodecNegativeYear)
    test(_TestTimeTextCodecDecode)
    test(_TestTimeTextCodecFractional)
    test(_TestTimeTextCodecEncode)
    test(_TestTimestampTextCodecDecode)
    test(_TestTimestampTextCodecFractional)
    test(_TestTimestampTextCodecInfinity)
    test(_TestTimestampTextCodecEncode)
    test(_TestTimestampTextCodecNegativeYear)
    test(_TestTimestamptzTextCodecDecodeUTC)
    test(_TestTimestamptzTextCodecDecodePositiveOffset)
    test(_TestTimestamptzTextCodecDecodeNegativeOffset)
    test(_TestTimestamptzTextCodecDecodeFractionalWithTz)
    test(_TestTimestamptzTextCodecInfinity)
    test(_TestTimestamptzTextCodecEncode)
    test(_TestTimestamptzTextCodecNegativeYear)
    test(_TestIntervalTextCodecFullFormat)
    test(_TestIntervalTextCodecTimeOnly)
    test(_TestIntervalTextCodecDaysOnly)
    test(_TestIntervalTextCodecYearsOnly)
    test(_TestIntervalTextCodecNegativeTime)
    test(_TestIntervalTextCodecNegativeDays)
    test(_TestIntervalTextCodecFractionalSeconds)
    test(_TestIntervalTextCodecEncode)
    test(_TestIntervalTextCodecPostgresMixedSign)
    test(_TestIntervalTextCodecISO8601Full)
    test(_TestIntervalTextCodecISO8601YearOnly)
    test(_TestIntervalTextCodecISO8601TimeOnly)
    test(_TestIntervalTextCodecISO8601DaysOnly)
    test(_TestIntervalTextCodecISO8601Negative)
    test(_TestIntervalTextCodecISO8601Fractional)
    test(_TestIntervalTextCodecISO8601Zero)
    test(_TestIntervalTextCodecISO8601FullFractional)
    test(_TestIntervalTextCodecISO8601NegativeFractional)
    test(_TestIntervalTextCodecVerboseFull)
    test(_TestIntervalTextCodecVerboseAgo)
    test(_TestIntervalTextCodecVerboseMixedAgo)
    test(_TestIntervalTextCodecVerboseZero)
    test(_TestIntervalTextCodecVerboseFractional)
    test(_TestIntervalTextCodecVerboseDaysOnly)
    test(_TestIntervalTextCodecVerboseNegNoAgo)
    test(_TestIntervalTextCodecVerboseNegFractional)
    test(_TestIntervalTextCodecSQLFullMixed)
    test(_TestIntervalTextCodecSQLYearMonthOnly)
    test(_TestIntervalTextCodecSQLNegYearMonth)
    test(_TestIntervalTextCodecSQLDayTime)
    test(_TestIntervalTextCodecSQLTimeOnly)
    test(_TestIntervalTextCodecSQLZero)
    test(_TestIntervalTextCodecSQLNegDayTime)
    test(_TestIntervalTextCodecSQLFractional)
    test(_TestIntervalTextCodecSQLZeroYM)
    test(_TestPgTimestampString)
    test(_TestPgDateString)
    test(_TestPgTimeString)
    test(_TestPgTimeValidation)
    test(_TestPgIntervalString)
    test(_TestFieldEqualityTemporal)
    test(_TestFieldInequalityCrossTypeTemporal)
    test(_TestRowsBuilderBinaryFormat)
    test(_TestRowsBuilderTextFormat)
    test(_TestRowsBuilderNullHandling)
    test(_TestRowsBuilderBinaryTemporalTypes)
    test(_TestCodecRegistryDecodeBinaryDate)
    test(_TestCodecRegistryDecodeBinaryTimestamp)
    test(_TestCodecRegistryDecodeBinaryTimestamptz)
    test(_TestCodecRegistryDecodeTextDate)
    test(_TestCodecRegistryDecodeTextTimestamptz)
    test(_TestCodecRegistryDecodeTextInterval)
    test(_TestCodecRegistryDecodeBinaryUuid)
    test(_TestCodecRegistryDecodeBinaryJsonb)
    test(_TestCodecRegistryDecodeBinaryOid)
    test(_TestCodecRegistryDecodeBinaryTextPassthrough)
    test(_TestInt2TextCodecRoundtrip)
    test(_TestInt8TextCodecRoundtrip)
    test(_TestFloat4TextCodecRoundtrip)
    test(_TestFloat8TextCodecRoundtrip)
    test(_TestTextPassthroughTextCodecRoundtrip)
    test(_TestOidTextCodecRoundtrip)
    test(_TestNumericTextCodecRoundtrip)
    test(_TestUuidTextCodecRoundtrip)
    test(_TestJsonbTextCodecRoundtrip)
    test(_TestDateTextCodecTypeMismatch)
    test(_TestTimeTextCodecTypeMismatch)
    test(_TestTimestampTextCodecTypeMismatch)
    test(_TestTimestamptzTextCodecTypeMismatch)
    test(_TestIntervalTextCodecTypeMismatch)
    test(_TestDateTextCodecBadInput)
    test(_TestTimeTextCodecBadInput)
    test(_TestTimeTextCodecOutOfRange)
    test(_TestTimestampTextCodecBadInput)
    test(_TestTimestamptzTextCodecBadInput)
    test(_TestIntervalTextCodecBadInput)
    test(_TestCodecRegistryDecodeBinaryTime)
    test(_TestCodecRegistryDecodeBinaryInterval)
    test(_TestCodecRegistryDecodeTextTimestamp)
    test(_TestCodecRegistryDecodeTextTime)
    test(_TestPreparedQueryTypedResults)
    test(_TestByteaBinaryCodecTypeMismatch)
    test(_TestTimeBinaryCodecOutOfRange)
    test(_TestFloat4BinaryCodecNaN)
    test(_TestFloat8BinaryCodecNaN)
    test(_TestNumericBinaryCodecLargeNumber)
    test(_TestNumericBinaryCodecNegativeNdigits)
    test(_TestPgIntervalStringMinValue)
    test(_TestByteaString)
    test(_TestByteaEquality)
    test(_TestRawBytesString)
    test(_TestRawBytesEquality)
    test(_TestCodecRegistryWithCodecBinary)
    test(_TestCodecRegistryWithCodecText)
    test(_TestCodecRegistryWithCodecRejectsBuiltinOverride)
    test(_TestCodecRegistryWithCodecRejectsDuplicate)
    test(_TestCodecRegistryWithCodecChaining)
    test(_TestCodecRegistryWithCodecPreservesBuiltins)
    test(_TestRowsBuilderWithCustomCodec)
    test(_TestFieldEqualityCustomType)
    test(_TestFieldInequalityCustomType)
    test(_TestFieldEqualityCustomWithoutEquatable)
    test(_TestFieldEqualityCustomVsBuiltin)
    test(_TestFieldEqualityCustomEquatableVsNonEquatable)
    test(_TestCodecRegistryWithCodecCustomText)
    test(_TestCodecRegistryDecodeErrorPropagatesText)
    test(_TestCodecRegistryDecodeErrorPropagatesBinary)
    test(_TestCodecRegistryDecodeErrorPropagatesBuiltin)
    test(Property1UnitTest[(I64, I64)](
      _TestFieldCustomEqualityReflexiveProperty))
    test(_TestArrayOidMapElementOidFor)
    test(_TestArrayOidMapArrayOidFor)
    test(_TestArrayOidMapIsArrayOid)
    test(_TestBinaryDecodeInt4Array)
    test(_TestBinaryDecodeInt2Array)
    test(_TestBinaryDecodeBoolArray)
    test(_TestBinaryDecodeTextArray)
    test(_TestBinaryDecodeWithNulls)
    test(_TestBinaryDecodeEmptyArray)
    test(_TestBinaryDecodeValidationErrors)
    test(_TestBinaryArrayElementCodecErrorPropagates)
    test(_TestTextArrayElementCodecErrorPropagates)
    test(_TestTextDecodeSimpleArray)
    test(_TestTextDecodeNullArray)
    test(_TestTextDecodeQuotedArray)
    test(_TestTextDecodeEscapedArray)
    test(_TestTextDecodeEmptyStringArray)
    test(_TestTextDecodeEmptyArray)
    test(_TestTextDecodeMultiDimensionalRejected)
    test(_TestTextDecodeBoolArray)
    test(_TestTextDecodeCaseInsensitiveNull)
    test(_TestPgArrayEquality)
    test(_TestPgArrayInequalityDifferentOid)
    test(_TestPgArrayInequalityDifferentElements)
    test(_TestPgArrayEqualityWithNulls)
    test(_TestPgArrayString)
    test(_TestPgArrayStringQuoting)
    test(_TestArrayEncoderRoundtrip)
    test(_TestArrayEncoderEmptyRoundtrip)
    test(_TestArrayEncoderBoolRoundtrip)
    test(_TestArrayEncoderStringRoundtrip)
    test(_TestParamEncoderPgArrayOids)
    test(_TestParamEncoderPgArrayUnknownOid)
    test(_TestFrontendMessageBindWithPgArray)
    test(_TestFieldDataEqExtraction)
    test(_TestNumericBinaryCodecEncodeRoundtrip)
    test(_TestCodecRegistryHasBinaryCodecArray)
    test(_TestCodecRegistryWithArrayType)
    test(_TestCodecRegistryWithArrayTypeRejectsInvalid)
    test(_TestCodecRegistryArrayOidFor)
    test(_TestIntegrationArraySelectBinary)
    test(_TestIntegrationArraySelectText)
    test(_TestIntegrationArrayRoundtrip)
    test(_TestIntegrationArrayEmpty)
    test(_TestIntegrationArrayNulls)
    test(_TestIntegrationArrayMultipleTypes)
    test(Property1UnitTest[(PgArray, U32)](
      _TestArrayBinaryRoundtripProperty))
    test(Property1UnitTest[(PgArray, U32)](
      _TestPgArrayEqualityReflexiveProperty))
    test(_TestArrayEncoderI16Roundtrip)
    test(_TestArrayEncoderI64Roundtrip)
    test(_TestArrayEncoderF32Roundtrip)
    test(_TestArrayEncoderF64Roundtrip)
    test(_TestArrayEncoderByteaRoundtrip)
    test(_TestArrayEncoderDateRoundtrip)
    test(_TestArrayEncoderTimeRoundtrip)
    test(_TestArrayEncoderTimestampRoundtrip)
    test(_TestArrayEncoderIntervalRoundtrip)
    test(_TestArrayEncoderUuidRoundtrip)
    test(_TestArrayEncoderJsonbRoundtrip)
    test(_TestArrayEncoderOidRoundtrip)
    test(_TestArrayEncoderNumericRoundtrip)
    test(_TestTextDecodeInt8Array)
    test(_TestTextDecodeFloat8Array)
    test(_TestTextDecodeDateArray)
    test(_TestTextDecodeTimestampArray)
    test(_TestTextDecodeUuidArray)
    test(_TestTextDecodeEscapedBackslash)
    test(_TestTextDecodeQuotedNullString)
    test(_TestPgArrayEqualitySizeMismatch)
    test(_TestPgArrayEqualityEmpty)
    test(_TestPgArrayFieldDataEqNonPgArray)
    test(_TestPgArrayStringNull)
    test(_TestFieldDataEqNullableMismatch)
    test(_TestArrayEncoderUnsupportedType)

class \nodoc\ iso _TestAuthenticate is UnitTest
  """
  Test to verify that given correct login information we can authenticate with
  a Postgres server. This test assumes that connecting is working correctly and
  will fail if it isn't.
  """
  fun name(): String =>
    "integration/Authenicate"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _AuthenticateTestNotify(h, true))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestAuthenticateFailure is UnitTest
  """
  Test to verify when we fail to authenticate with a Postgres server that are
  handling the failure correctly. This test assumes that connecting is working
  correctly and will fail if it isn't.
  """
  fun name(): String =>
    "integration/AuthenicateFailure"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password + " " + info.password,
        info.database),
      _AuthenticateTestNotify(h, false))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _AuthenticateTestNotify is SessionStatusNotify
  let _h: TestHelper
  let _success_expected: Bool

  new create(h: TestHelper, success_expected: Bool) =>
    _h = h
    _success_expected = success_expected

  be pg_session_authenticated(session: Session) =>
    _h.complete(_success_expected == true)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.complete(_success_expected == false)

class \nodoc\ iso _TestConnect is UnitTest
  """
  Test to verify that given correct login information that we can connect to
  a Postgres server.
  """
  fun name(): String =>
    "integration/Connect"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _ConnectTestNotify(h, true))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestConnectFailure is UnitTest
  """
  Test to verify that connection failures are handled correctly. Currently,
  we set up a bad connect attempt by taking the valid port number that would
  allow a connect and reversing it to create an attempt to connect on a port
  that nothing should be listening on.
  """
  fun name(): String =>
    "integration/ConnectFailure"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)
    let host = ifdef linux then "127.0.0.2" else "localhost" end

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), host, info.port.reverse()),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _ConnectTestNotify(h, false))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _ConnectTestNotify is SessionStatusNotify
  let _h: TestHelper
  let _success_expected: Bool

  new create(h: TestHelper, success_expected: Bool) =>
    _h = h
    _success_expected = success_expected

  be pg_session_connected(session: Session) =>
    _h.complete(_success_expected == true)

  be pg_session_connection_failed(session: Session,
    reason: ConnectionFailureReason)
  =>
    _h.complete(_success_expected == false)

class \nodoc\ val _ConnectionTestConfiguration
  let host: String
  let port: String
  let ssl_host: String
  let ssl_port: String
  let username: String
  let password: String
  let database: String
  let md5_username: String
  let md5_password: String

  new val create(vars: (Array[String] val | None)) =>
    let e = EnvVars(vars)
    host = try e("POSTGRES_HOST")? else "127.0.0.1" end
    port = try e("POSTGRES_PORT")? else "5432" end
    ssl_host = try e("POSTGRES_SSL_HOST")? else host end
    ssl_port = try e("POSTGRES_SSL_PORT")? else "5433" end
    username = try e("POSTGRES_USERNAME")? else "postgres" end
    password = try e("POSTGRES_PASSWORD")? else "postgres" end
    database = try e("POSTGRES_DATABASE")? else "postgres" end
    md5_username = try e("POSTGRES_MD5_USERNAME")? else "md5user" end
    md5_password = try e("POSTGRES_MD5_PASSWORD")? else "md5pass" end
