unit Grijjy.MongoDB;
{ < Main interface to MongoDB }

{$INCLUDE 'Grijjy.inc'}

interface

uses
  System.SysUtils, System.SyncObjs, System.Generics.Collections, Grijjy.Bson, Grijjy.Bson.IO, Grijjy.MongoDB.Protocol,
  Grijjy.MongoDB.Queries;

type
  tgoDocEditor = reference to procedure(doc: tgoBsonDocument);

  { MongoDB validation types
    https://docs.mongodb.com/manual/reference/command/create/ }
  TgoMongoValidationLevel = (vlOff, vlStrict, vlModerate);

  TgoMongoValidationLevelHelper = record helper for TgoMongoValidationLevel
  public
    function ToString: string;
  end;

  TgoMongoValidationAction = (vaError, vaWarn);

  TgoMongoValidationActionHelper = record helper for TgoMongoValidationAction
  public
    function ToString: string;
  end;

  { MongoDB collation
    https://docs.mongodb.com/manual/reference/collation/ }
  TgoMongoCollationCaseFirst = (ccfUpper, ccfLower, ccfOff);

  TgoMongoCollationCaseFirstHelper = record helper for TgoMongoCollationCaseFirst
  public
    function ToString: string;
  end;

  TgoMongoCollationAlternate = (caNonIgnorable, caShifted);

  TgoMongoCollationAlternateHelper = record helper for TgoMongoCollationAlternate
  public
    function ToString: string;
  end;

  TgoMongoCollationMaxVariable = (cmvPunct, cmvSpace);

  TgoMongoCollationMaxVariableHelper = record helper for TgoMongoCollationMaxVariable
  public
    function ToString: string;
  end;

  TgoMongoCollation = record
  public
    Locale: string;
    CaseLevel: Boolean;
    CaseFirst: TgoMongoCollationCaseFirst;
    Strength: Integer;
    NumericOrdering: Boolean;
    Alternate: TgoMongoCollationAlternate;
    MaxVariable: TgoMongoCollationMaxVariable;
    Backwards: Boolean;
  end;

  { MongoDb dbStats
    https://docs.mongodb.com/manual/reference/command/dbStats/ }
  TgoMongoStatistics = record
  public
    Database: string;
    Collections: Integer;
    Views: Integer;
    Objects: Int64;
    AvgObjSize: Double;
    DataSize: Double;
    StorageSize: Double;
    NumExtents: Integer;
    Indexes: Integer;
    IndexSize: Double;
    ScaleFactor: Double;
    FsUsedSize: Double;
    FsTotalSize: Double;
  end;

  { MongoDb instances
    https://docs.mongodb.com/manual/reference/command/isMaster/ }
  TgoMongoInstance = record
  public
    Host: string;
    Port: Word;
  public
    constructor Create(AInstance: string); overload;
    constructor Create(AHost: string; APort: Word); overload;
  end;

  TgoMongoInstances = TArray<TgoMongoInstance>;

  TgoMongoInstanceInfo = record
  public
    Hosts: TgoMongoInstances;
    Arbiters: TgoMongoInstances;
    Primary: TgoMongoInstance;
    Me: TgoMongoInstance;
    SetName: string;
    SetVersion: Integer;
    IsMaster: Boolean;
    IsSecondary: Boolean;
    ArbiterOnly: Boolean;
    LocalTime: TDateTime;
    ConnectionId: Integer;
    ReadOnly: Boolean;
  end;

const
  MongoDefBatchSize = 101;

  { MongoDB collation default settings
    https://docs.mongodb.com/manual/reference/collation-locales-defaults/#collation-languages-locales }
  DEFAULTTGOMONGOCOLLATION: TgoMongoCollation = (
    Locale: 'en';
    CaseLevel: false;
    CaseFirst: TgoMongoCollationCaseFirst.ccfOff;
    Strength: 1;
    NumericOrdering: false;
    Alternate: TgoMongoCollationAlternate.caNonIgnorable;
    MaxVariable: TgoMongoCollationMaxVariable.cmvSpace;
    Backwards: false;
    );

type
  { MongoDB error codes (as of v 8.0) }
  TgoMongoErrorCode = (OK = 0, InternalError = 1, BadValue = 2, OBSOLETE_DuplicateKey = 3, NoSuchKey = 4,
    GraphContainsCycle = 5, HostUnreachable = 6, HostNotFound = 7, UnknownError = 8, FailedToParse = 9,
    CannotMutateObject = 10, UserNotFound = 11, UnsupportedFormat = 12, Unauthorized = 13, TypeMismatch = 14,
    Overflow = 15, InvalidLength = 16, ProtocolError = 17, AuthenticationFailed = 18, CannotReuseObject = 19,
    IllegalOperation = 20, EmptyArrayOperation = 21, InvalidBSON = 22, AlreadyInitialized = 23, LockTimeout = 24,
    RemoteValidationError = 25, NamespaceNotFound = 26, IndexNotFound = 27, PathNotViable = 28, NonExistentPath = 29,
    InvalidPath = 30, RoleNotFound = 31, RolesNotRelated = 32, PrivilegeNotFound = 33, CannotBackfillArray = 34,
    UserModificationFailed = 35, RemoteChangeDetected = 36, FileRenameFailed = 37, FileNotOpen = 38, FileStreamFailed = 39,
    ConflictingUpdateOperators = 40, FileAlreadyOpen = 41, LogWriteFailed = 42, CursorNotFound = 43, UserDataInconsistent = 45,
    LockBusy = 46, NoMatchingDocument = 47, NamespaceExists = 48, InvalidRoleModification = 49, MaxTimeMSExpired = 50,
    ManualInterventionRequired = 51, DollarPrefixedFieldName = 52, InvalidIdField = 53, NotSingleValueField = 54, InvalidDBRef = 55,
    EmptyFieldName = 56, DottedFieldName = 57, RoleModificationFailed = 58, CommandNotFound = 59, ShardKeyNotFound = 61,
    OplogOperationUnsupported = 62, StaleShardVersion = 63, WriteConcernFailed = 64, MultipleErrorsOccurred = 65, ImmutableField = 66,
    CannotCreateIndex = 67, IndexAlreadyExists = 68, AuthSchemaIncompatible = 69, ShardNotFound = 70, ReplicaSetNotFound = 71,
    InvalidOptions = 72, InvalidNamespace = 73, NodeNotFound = 74, WriteConcernLegacyOK = 75, NoReplicationEnabled = 76,
    OperationIncomplete = 77, CommandResultSchemaViolation = 78, UnknownReplWriteConcern = 79, RoleDataInconsistent = 80,
    NoMatchParseContext = 81, NoProgressMade = 82, RemoteResultsUnavailable = 83, IndexOptionsConflict = 85,
    IndexKeySpecsConflict = 86, CannotSplit = 87, NetworkTimeout = 89, CallbackCanceled = 90, ShutdownInProgress = 91,
    SecondaryAheadOfPrimary = 92, InvalidReplicaSetConfig = 93, NotYetInitialized = 94, NotSecondary = 95, OperationFailed = 96,
    NoProjectionFound = 97, DBPathInUse = 98, UnsatisfiableWriteConcern = 100, OutdatedClient = 101, IncompatibleAuditMetadata = 102,
    NewReplicaSetConfigurationIncompatible = 103, NodeNotElectable = 104, IncompatibleShardingMetadata = 105, DistributedClockSkewed = 106,
    LockFailed = 107, InconsistentReplicaSetNames = 108, ConfigurationInProgress = 109, CannotInitializeNodeWithData = 110,
    NotExactValueField = 111, WriteConflict = 112, InitialSyncFailure = 113, InitialSyncOplogSourceMissing = 114,
    CommandNotSupported = 115, DocTooLargeForCapped = 116, ConflictingOperationInProgress = 117, NamespaceNotSharded = 118,
    InvalidSyncSource = 119, OplogStartMissing = 120, DocumentValidationFailure = 121, NotAReplicaSet = 123,
    IncompatibleElectionProtocol = 124, CommandFailed = 125, RPCProtocolNegotiationFailed = 126, UnrecoverableRollbackError = 127,
    LockNotFound = 128, LockStateChangeFailed = 129, SymbolNotFound = 130, FailedToSatisfyReadPreference = 133,
    ReadConcernMajorityNotAvailableYet = 134, StaleTerm = 135, CappedPositionLost = 136, IncompatibleShardingConfigVersion = 137,
    RemoteOplogStale = 138, JSInterpreterFailure = 139, InvalidSSLConfiguration = 140, SSLHandshakeFailed = 141,
    JSUncatchableError = 142, CursorInUse = 143, IncompatibleCatalogManager = 144, PooledConnectionsDropped = 145,
    ExceededMemoryLimit = 146, ZLibError = 147, ReadConcernMajorityNotEnabled = 148, NoConfigPrimary = 149,
    StaleEpoch = 150, OperationCannotBeBatched = 151, OplogOutOfOrder = 152, ChunkTooBig = 153, InconsistentShardIdentity = 154,
    CannotApplyOplogWhilePrimary = 155, CanRepairToDowngrade = 157, MustUpgrade = 158, DurationOverflow = 159,
    MaxStalenessOutOfRange = 160, IncompatibleCollationVersion = 161, CollectionIsEmpty = 162, ZoneStillInUse = 163,
    InitialSyncActive = 164, ViewDepthLimitExceeded = 165, CommandNotSupportedOnView = 166, OptionNotSupportedOnView = 167,
    InvalidPipelineOperator = 168, CommandOnShardedViewNotSupportedOnMongod = 169, TooManyMatchingDocuments = 170,
    CannotIndexParallelArrays = 171, TransportSessionClosed = 172, TransportSessionNotFound = 173, TransportSessionUnknown = 174,
    QueryPlanKilled = 175, FileOpenFailed = 176, ZoneNotFound = 177, RangeOverlapConflict = 178, WindowsPdhError = 179,
    BadPerfCounterPath = 180, AmbiguousIndexKeyPattern = 181, InvalidViewDefinition = 182, ClientMetadataMissingField = 183,
    ClientMetadataAppNameTooLarge = 184, ClientMetadataDocumentTooLarge = 185, ClientMetadataCannotBeMutated = 186,
    LinearizableReadConcernError = 187, IncompatibleServerVersion = 188, PrimarySteppedDown = 189,
    MasterSlaveConnectionFailure = 190, FailPointEnabled = 192, NoShardingEnabled = 193, BalancerInterrupted = 194,
    ViewPipelineMaxSizeExceeded = 195, InvalidIndexSpecificationOption = 197, ReplicaSetMonitorRemoved = 199,
    ChunkRangeCleanupPending = 200, CannotBuildIndexKeys = 201, NetworkInterfaceExceededTimeLimit = 202,
    ShardingStateNotInitialized = 203, TimeProofMismatch = 204, ClusterTimeFailsRateLimiter = 205, NoSuchSession = 206,
    InvalidUUID = 207, TooManyLocks = 208, StaleClusterTime = 209, CannotVerifyAndSignLogicalTime = 210,
    KeyNotFound = 211, IncompatibleRollbackAlgorithm = 212, DuplicateSession = 213, AuthenticationRestrictionUnmet = 214,
    DatabaseDropPending = 215, ElectionInProgress = 216, IncompleteTransactionHistory = 217, UpdateOperationFailed = 218,
    FTDCPathNotSet = 219, FTDCPathAlreadySet = 220, IndexModified = 221, CloseChangeStream = 222,
    IllegalOpMsgFlag = 223, QueryFeatureNotAllowed = 224, TransactionTooOld = 225, AtomicityFailure = 226,
    CannotImplicitlyCreateCollection = 227, SessionTransferIncomplete = 228, MustDowngrade = 229, DNSHostNotFound = 230,
    DNSProtocolError = 231, MaxSubPipelineDepthExceeded = 232, TooManyDocumentSequences = 233, RetryChangeStream = 234,
    InternalErrorNotSupported = 235, ForTestingErrorExtraInfo = 236, CursorKilled = 237, NotImplemented = 238,
    SnapshotTooOld = 239, DNSRecordTypeMismatch = 240, ConversionFailure = 241, CannotCreateCollection = 242,
    IncompatibleWithUpgradedServer = 243, BrokenPromise = 245, SnapshotUnavailable = 246, ProducerConsumerQueueBatchTooLarge = 247,
    ProducerConsumerQueueEndClosed = 248, StaleDbVersion = 249, StaleChunkHistory = 250, NoSuchTransaction = 251,
    ReentrancyNotAllowed = 252, FreeMonHttpInFlight = 253, FreeMonHttpTemporaryFailure = 254, FreeMonHttpPermanentFailure = 255,
    TransactionCommitted = 256, TransactionTooLarge = 257, UnknownFeatureCompatibilityVersion = 258, KeyedExecutorRetry = 259,
    InvalidResumeToken = 260, TooManyLogicalSessions = 261, ExceededTimeLimit = 262, OperationNotSupportedInTransaction = 263,
    TooManyFilesOpen = 264, OrphanedRangeCleanUpFailed = 265, FailPointSetFailed = 266, PreparedTransactionInProgress = 267,
    CannotBackup = 268, DataModifiedByRepair = 269, RepairedReplicaSetNode = 270, JSInterpreterFailureWithStack = 271,
    MigrationConflict = 272, ProducerConsumerQueueProducerQueueDepthExceeded = 273, ProducerConsumerQueueConsumed = 274,
    ExchangePassthrough = 275, IndexBuildAborted = 276, AlarmAlreadyFulfilled = 277, UnsatisfiableCommitQuorum = 278,
    ClientDisconnect = 279, ChangeStreamFatalError = 280, TransactionCoordinatorSteppingDown = 281,
    TransactionCoordinatorReachedAbortDecision = 282, WouldChangeOwningShard = 283, ForTestingErrorExtraInfoWithExtraInfoInNamespace = 284,
    IndexBuildAlreadyInProgress = 285, ChangeStreamHistoryLost = 286, TransactionCoordinatorDeadlineTaskCanceled = 287,
    ChecksumMismatch = 288, WaitForMajorityServiceEarlierOpTimeAvailable = 289, TransactionExceededLifetimeLimitSeconds = 290,
    NoQueryExecutionPlans = 291, QueryExceededMemoryLimitNoDiskUseAllowed = 292, InvalidSeedList = 293, InvalidTopologyType = 294,
    InvalidHeartBeatFrequency = 295, TopologySetNameRequired = 296, HierarchicalAcquisitionLevelViolation = 297, InvalidServerType = 298,
    OCSPCertificateStatusRevoked = 299, RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist = 300, DataCorruptionDetected = 301,
    OCSPCertificateStatusUnknown = 302, SplitHorizonChange = 303, ShardInvalidatedForTargeting = 304,
    RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist = 307, CurrentConfigNotCommittedYet = 308, ExhaustCommandFinished = 309,
    PeriodicJobIsStopped = 310, TransactionCoordinatorCanceled = 311, OperationIsKilledAndDelisted = 312,
    ResumableRangeDeleterDisabled = 313, ObjectIsBusy = 314, TooStaleToSyncFromSource = 315, QueryTrialRunCompleted = 316,
    ConnectionPoolExpired = 317, ForTestingOptionalErrorExtraInfo = 318, MovePrimaryInProgress = 319, TenantMigrationConflict = 320,
    TenantMigrationCommitted = 321, APIVersionError = 322, APIStrictError = 323, APIDeprecationError = 324,
    TenantMigrationAborted = 325, OplogQueryMinTsMissing = 326, NoSuchTenantMigration = 327,
    TenantMigrationAccessBlockerShuttingDown = 328, TenantMigrationInProgress = 329, SkipCommandExecution = 330,
    FailedToRunWithReplyBuilder = 331, CannotDowngrade = 332, ServiceExecutorInShutdown = 333, MechanismUnavailable = 334,
    TenantMigrationForgotten = 335, SocketException = 9001, OBSOLETE_RecvStaleConfig = 9996,
    CannotGrowDocumentInCappedNamespace = 10003, NotWritablePrimary = 10107, BSONObjectTooLarge = 10334, DuplicateKey = 11000,
    InterruptedAtShutdown = 11600, Interrupted = 11601, InterruptedDueToReplStateChange = 11602,
    BackgroundOperationInProgressForDatabase = 12586, BackgroundOperationInProgressForNamespace = 12587,
    OBSOLETE_PrepareConfigsFailed = 13104, MergeStageNoMatchingDocument = 13113, DatabaseDifferCase = 13297,
    StaleConfig = 13388, NotPrimaryNoSecondaryOk = 13435, NotPrimaryOrSecondary = 13436, OutOfDiskSpace = 14031,
    ClientMarkedKilled = 46841);

type

  { Is raised when there is an error writing to the database }
  EgoMongoDBGeneralError = class(EgoMongoDBError)
{$REGION 'Internal Declarations'}
  private
    FErrorCode: TgoMongoErrorCode;
{$ENDREGION 'Internal Declarations'}
  public
    constructor Create(const AErrorCode: TgoMongoErrorCode; const AErrorMsg: string);
    { The MongoDB error code }
    property ErrorCode: TgoMongoErrorCode read FErrorCode;
  end;

  EgoMongoDBProtocolError = class(EgoMongoDBGeneralError);
  EGoMongoDBWriteError = class(EgoMongoDBGeneralError);
  EGoMongoDBWriteConcernError = class(EGoMongoDBWriteError);

type
  { Forward declarations }
  IgoMongoDatabase = interface;

  IgoMongoCollection = interface;

  igoMongoCursor = interface;

  tWriteCmd = reference to procedure(Writer: IgoBsonWriter);

  { The client interface to MongoDB.
    This is the entry point for the MongoDB API.
    This interface is implemented in to TgoMongoClient class. }
  IgoMongoClient = interface
    ['{66FF5346-48F6-44E1-A46F-D8B958F06EA0}']
    { Returns an array with the names of all databases available to the client. }
    function ListDatabaseNames: TArray<string>;

    { Returns an array of documents describing all databases available to the
      client (one document per database). The structure of each document is
      described here:
      https://docs.mongodb.com/manual/reference/command/listDatabases/ }
    function ListDatabases: TArray<tgoBsonDocument>;

    { Returns a document that describes the role of the mongod instance. If the optional
      field saslSupportedMechs is specified, the command also returns an array of
      SASL mechanisms used to create the specified user�s credentials.
      If the instance is a member of a replica set, then isMaster returns a subset
      of the replica set configuration and status including whether or not the instance
      is the primary of the replica set.

      described here:
      https://docs.mongodb.com/manual/reference/command/isMaster/
 }
    function GetProtocol: tgoMongoProtocol;
    function GetInstanceInfo(const ASaslSupportedMechs: string = ''; const AComment: string = ''): TgoMongoInstanceInfo;
    function IsMaster: Boolean;

    { Issue an admin command that is supposed to return ONE document }
    function AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;
    { Issue a logRotate command.
      https://www.mongodb.com/docs/manual/reference/command/logRotate/ }
    function LogRotate: Boolean;
    { Query build info of the current Mongod
      https://www.mongodb.com/docs/manual/reference/command/buildInfo/ }
    function BuildInfo: tgoBsonDocument;
    { Query system/platform info of the current Mongod server
      https://www.mongodb.com/docs/manual/reference/command/hostInfo/ }
    function HostInfo: tgoBsonDocument;
    { Query build-level feature settings
      https://www.mongodb.com/docs/manual/reference/command/features/ }
    function Features: tgoBsonDocument;
    { Drops the database with the specified name.
      Parameters:
      AName: The name of the database to drop. }
    procedure DropDatabase(const AName: string);
    function getAvailable: Boolean;
    function getConnected: Boolean;
    { Gets a database.

      Parameters:
      AName: the name of the database.
      Returns:
      An implementation of the database.

      NOTE: If a database with the given name does not exist, then it will be
      automatically created as soon as you start writing to it.

      NOTE: This method is light weight and doesn't actually open the database
      yet. The database is only opened once you start reading, writing or
      querying it. }
    function GetDatabase(const AName: string): IgoMongoDatabase;
    function GetGlobalReadPreference: tgoMongoReadPreference;
    function getPooled: Boolean;
    procedure ReleaseToPool;
    procedure setAvailable(const Value: Boolean);
    procedure setConnected(const Value: Boolean);
    procedure SetGlobalReadPreference(const Value: tgoMongoReadPreference);
    procedure setPooled(const Value: Boolean);

    //high-level calls for transactions
    function SupportsTransactions: Boolean;
    procedure StartTransaction;
    Procedure CommitTransaction;
    Procedure AbortTransaction;
    procedure UnpinTransaction;

    //internal call for transactions
    procedure IncludeTransactionDetails(const Writer: IgoBsonWriter);
    property Available: Boolean read getAvailable write setAvailable;
    property Connected: Boolean read getConnected write setConnected;
    { GlobalReadPreference sets the global ReadPreference for all objects (database, collection etc)
      that do not have an individual specific ReadPreference. }
    property GlobalReadPreference: tgoMongoReadPreference read GetGlobalReadPreference write SetGlobalReadPreference;
    property Pooled: Boolean read getPooled write setPooled;
    property Protocol: tgoMongoProtocol read GetProtocol;
  end;

  { Represents a database in MongoDB.
    Instances of this interface are aquired by calling
    IgoMongoClient.GetDatabase. }
  IgoMongoDatabase = interface
    ['{5164D7B1-74F5-45F1-AE22-AB5FFC834590}']
{$REGION 'Internal Declarations'}
    function _GetClient: IgoMongoClient;
    function _GetName: string;
{$ENDREGION 'Internal Declarations'}
    { Returns an array with the names of all collections in the database. }
    function ListCollectionNames: TArray<string>;

    { Returns an array of documents describing all collections in the database
      (one document per collection). The structure of each document is
      described here:
      https://docs.mongodb.com/manual/reference/method/db.getCollectionInfos/ }
    function ListCollections: TArray<tgoBsonDocument>;

    { Drops the collection with the specified name.

      Parameters:
      AName: The name of the collection to drop. }
    procedure DropCollection(const AName: string);

    { Gets a collection.

      Parameters:
      AName: the name of the collection.

      Returns:
      An implementation of the collection.

      NOTE: If a collection with the given name does not exist in this database,
      then it will be automatically created as soon as you start writing to it.

      NOTE: This method is light weight and doesn't actually open the collection
      yet. The collection is only opened once you start reading, writing or
      querying it. }
    function GetCollection(const AName: string): IgoMongoCollection;

    { Creates a collection.

      All parameters are described here:
      https://docs.mongodb.com/manual/reference/command/create/ }
    function CreateCollection(const AName: string; const ACapped: Boolean; const AMaxSize: Int64; const AMaxDocuments: Int64; const
      AValidationLevel: TgoMongoValidationLevel; const AValidationAction: TgoMongoValidationAction; const AValidator: tgoBsonDocument; const
      ACollation: TgoMongoCollation): Boolean;

    { Rename a collection.

      All parameters are described here:
      https://docs.mongodb.com/manual/reference/command/renameCollection/ }
    function RenameCollection(const AFromNamespace, AToNamespace: string; const ADropTarget: Boolean = false): Boolean;

    { Get database statistics.

      All parameters are described here:
      https://docs.mongodb.com/manual/reference/command/dbStats/ }
    function GetDbStats(const AScale: Integer): TgoMongoStatistics;

    { Issue a command against the MongoDB instance that returns a cursor. }
    function AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;

    { Issue a command against the database that returns a cursor.
      Similar to AdminCommand. }
    function Command(CommandToIssue: tWriteCmd): igoMongoCursor;
    function GetReadPreference: tgoMongoReadPreference;
    procedure SetReadPreference(const Value: tgoMongoReadPreference);

    { The client used for this database. }
    property Client: IgoMongoClient read _GetClient;

    {Returns the protocol of the CLIENT}
    function GetProtocol: tgoMongoProtocol;

    { The name of the database. }
    property name: string read _GetName;
    { setting ReadPreference on the database will override the global readpreference }
    property ReadPreference: tgoMongoReadPreference read GetReadPreference write SetReadPreference;
    property Protocol: tgoMongoProtocol read GetProtocol;
  end;

  { Represents a cursor to the documents returned from one of the
    IgoMongoCollection.Find methods. }
  igoMongoCursor = interface
    ['{18813F27-1B41-453C-86FE-E98AFEB3D905}']
    { Allows for..in enumeration over all documents in the cursor. }
    function GetEnumerator: TEnumerator<tgoBsonDocument>;

    { Converts all documents in the cursor to an array.
      Note that this can be time consuming and result in a large array,
      depending on the number of documents in the cursor.

      Returns:
      An array of documents in the cursor. }
    function ToArray: TArray<tgoBsonDocument>;
  end;

  { Fluent Interface for igoMongoCollection.find(),
    see https://www.mongodb.com/docs/manual/reference/command/find/
    the class factory is function "findOptions" }
  igoMongoFindOptions = interface
    ['{5E3602BD-90EE-493A-9A91-50E7209707E4}']
    function getfilter: tgoMongoFilter;
    function getbatchSize: Integer;
    { Filter: Optional. The query predicate. If unspecified, then all documents
      in the collection will match the predicate. }
    function filter(const AValue: tgoMongoFilter): igoMongoFindOptions; overload;
    function filter(const aJsonDoc: string): igoMongoFindOptions; overload;
    { Sort:Optional. The sort specification for the ordering of the results. }
    function sort(const AValue: TgoMongoSort): igoMongoFindOptions; overload;
    function sort(const aJsonDoc: string): igoMongoFindOptions; overload;
    { Projection: Optional. The projection specification to determine which fields to include
      in the returned documents. See Projection and Projection Operators. }
    function projection(const AValue: TgoMongoProjection): igoMongoFindOptions; overload;
    function projection(const aJsonDoc: string): igoMongoFindOptions; overload;
    { hint:Optional. Index specification. Specify either the index name as a string or
      the index key pattern. If specified, then the query system will only consider
      plans using the hinted index. }
    function hint(AValue: string): igoMongoFindOptions;
    { Skip: Optional. Number of documents to skip. Defaults to 0. }
    function skip(AValue: Integer): igoMongoFindOptions;
    { limit: Optional. The maximum number of documents to return.
      If unspecified, then defaults to no limit.
      A limit of 0 is equivalent to setting no limit. }
    function limit(AValue: Integer): igoMongoFindOptions;
    { batchSize: Optional. The number of documents to return in the first batch.
      Defaults to 101.  A batchSize of 0 means that the cursor will be established, but
      no documents  will be returned in the first batch. Unlike the previous wire protocol
      version,  a batchSize of 1 for the find command does not close the cursor. }
    function batchSize(AValue: Integer): igoMongoFindOptions;
    { singleBatch: Optional. Determines whether to close the cursor
      after the first batch. Defaults to false. }
    function singleBatch(AValue: Boolean): igoMongoFindOptions;
    function comment(const AValue: string): igoMongoFindOptions;
    { maxTimeMS: Optional. The cumulative time limit in milliseconds for processing
      operations on the cursor. MongoDB aborts the operation at the earliest following
      interrupt point. }
    function maxTimeMS(AValue: Integer): igoMongoFindOptions;
    { readConcern: See https://www.mongodb.com/docs/manual/reference/glossary/#std-term-read-concern }
    function readConcern(const AValue: tgoBsonDocument): igoMongoFindOptions; overload;
    function readConcern(const aJsonDoc: string): igoMongoFindOptions; overload;
    { returnKey: Optional. If true, returns only the index keys in the resulting documents.
      Default value is false. If returnKey is true and the   find  command does not use an
      index, the returned documents will be empty. }
    function returnKey(Value: Boolean): igoMongoFindOptions;
    { showRecordID: Optional. Determines whether to return the record identifier for each document.
      If true, adds a field $recordId to the returned documents. }
    function showRecordId(Value: Boolean): igoMongoFindOptions;
    { noCursorTimeout: Optional. Prevents the server from timing out idle
      cursors after an inactivity period (10 minutes). }
    function noCursorTimeout(Value: Boolean): igoMongoFindOptions;
    function allowPartialResults(Value: Boolean): igoMongoFindOptions;
    { min:optional. The inclusive lower bound for a specific index. See cursor.min()
      for details. Starting in MongoDB 4.2, to use the min field, the command must also use
      hint unless the specified filter is an equality condition on the _id field { _id: <value> }
    function min(const AValue: tgoBsonDocument): igoMongoFindOptions; overload;
    function min(const aJsonDoc: string): igoMongoFindOptions; overload;
    { max: Optional. The exclusive upper bound for a specific index. See
      cursor.max() for details. Starting in MongoDB 4.2, to use the max field,
      the command must also use hint unless the specified filter is an equality
      condition on the _id field { _id: <value> }
    function max(const AValue: tgoBsonDocument): igoMongoFindOptions; overload;
    function max(const aJsonDoc: string): igoMongoFindOptions; overload;
    { collation: Optional. Specifies the collation to use for the operation. }
    function collation(const AValue: tgoBsonDocument): igoMongoFindOptions; overload;
    function collation(const aJsonDoc: string): igoMongoFindOptions; overload;
    { allowDiskUse:Optional. Use this option to override allowDiskUseByDefault for a specific query. }
    function allowDiskUse(Value: Boolean): igoMongoFindOptions;
    procedure WriteOptions(const Writer: IgoBsonWriter);
    function asBsonDocument: tgoBsonDocument;
    function asJson: string;
    procedure fromBson(aBson: tgoBsonDocument);
    procedure fromJson(const aJson: string);
  end;

  (*igoAggregationPipeline           !!! UNDER DEVELOPMENT !!!

  Fluent aggregate pipeline builder for MongoDB collection.aggregate().

  See https://www.mongodb.com/docs/manual/reference/operator/aggregation/

  MongoDB Aggregates have lots and lots of possible stages and options,
  therefore this interface can only implement a subset and will be extended
  in the future. You can use the "stage" method to support stages that are
  not part of this interface yet.

  --> Please Use the "aggPipeline" class factory to create an instance.

  Methods:

  "BatchSize" Not a stage, this optionally specifies the number of elements per batch
              in the cursor, but this does not seem to be working somehow.

  "MaxTimeMS" Not a stage, this optionally specifies the timeout of the filter

  "Stage"     Manually creates a new pipeline stage. Please omit the leading $
              in the stage name. It either
              - takes a tgoBsonDocument as input,
              - OR a string that contains such a document in JSON syntax,
              - OR an anonymous method that fills the document with data.

  "Match"     see https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/#mongodb-pipeline-pipe.-match
              Implements the $match stage and either:
              - takes a tgoMongofilter as input,
              - OR a string that contains a document in JSON syntax,
              - OR an anonymous method that fills the document with data.

  "Limit"     see https://www.mongodb.com/docs/manual/reference/operator/aggregation/limit/#mongodb-pipeline-pipe.-limit
              Implements the $limit stage, which limits the number of results,
              it lets at most N records pass.

  "AddFields" see https://www.mongodb.com/docs/manual/reference/operator/aggregation/addFields/
              Implements the $addFields stage. Used to add new (possibly
              calculated) fields to the documents.
              References to field contents must have a leading $.
              It either
              - takes a tgoBsonDocument as input,
              - OR a string that contains a document in JSON syntax,
              - OR an anonymous method that fills the document with data.

  "Set"       Implements the $set stage and is a synonym for $addfields.

  "Unset"     Implements the $unset stage, removes fields.

  "Sort"      Implements the $sort stage, it either
                - takes a tgoMongoSort as input,
                - OR a tgoBsonDocument,
                - OR a string that contains a document in JSON syntax,
                - OR an anonymous method that fills the document with data.

  "Project"   see https://www.mongodb.com/docs/manual/reference/operator/aggregation/project/#mongodb-pipeline-pipe.-project
              Implements the $project stage. It either:
                - takes a tgoMongoProjection as input,
                - OR a tgoBsonDocument,
                - OR a string that contains a document in JSON syntax,
                - OR an anonymous method that fills the document with data.

  "Group"     see https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/#mongodb-pipeline-pipe.-group

              Implements the $group stage, which is used to define a grouping
              key ("_id") and to compute stuff like min/max/avg/totals over the
              grouping interval. References to field contents MUST be preceded
              with a dollar sign.  It either:

                - takes a tgoBsonDocument as input,
                - OR a string that contains a document in JSON syntax,
                - OR an anonymous method that fills the document with data.

  "Out"       see https://www.mongodb.com/docs/manual/reference/operator/aggregation/out/#mongodb-pipeline-pipe.-out
              The $out stage routes the output of the aggregation pipeline into a new
              collection. It must be the LAST item in the pipeline and the
              collection.aggregate() method will return an empty cursor after
              the out stage. This stage is practical for debugging because
              you can easily review the data.

  "Edit"      This is not a stage but it lets you edit/modify the stage that was last defined,
              using an anonymous callback method. This is extremely practical if you want
              to keep a complex stage such as "group" simple and add aggregated fields
              later in a separate anonymous method.

  "Pipeline"  This function retrieves all the stages of the pipeline as a tgoBsonArray.
              However, you are advised to pass the whole igoAggregationPipeline interface
              directly to the collection.aggregate() method, because that will
              include the "BatchSize" and "MaxTimeMS" properties.
              Those properties are not stages.
  *)

  igoAggregationPipeline = interface
    ['{CE7BE794-0D4C-4B0F-88AA-223976F58CE4}']

    function Stage(const aStageName: string; aStageContent: tgoBsonValue): igoAggregationPipeline; overload;
    function Stage(const aStageName: string; aStageDocJS: string): igoAggregationPipeline; overload;
    function Stage(const aStageName: string; aStageProc: tgoDocEditor): igoAggregationPipeline; overload;

    function AddFields(aNewfieldsDoc: tgoBsonDocument): igoAggregationPipeline; overload;
    function AddFields(aNewfieldsDocJS: string): igoAggregationPipeline; overload;
    function AddFields(aNewFieldsProc: tgoDocEditor): igoAggregationPipeline; overload;

    function Match(aFilter: tgoMongoFilter): igoAggregationPipeline; overload;
    function Match(aFilterDocJs: string): igoAggregationPipeline; overload;
    function Match(aFilterProc: tgoDocEditor): igoAggregationPipeline; overload;

    function Group(aGroupDoc: tgoBsonDocument): igoAggregationPipeline; overload;
    function Group(aGroupDocJS: string): igoAggregationPipeline; overload;
    function Group(aGroupProc: tgoDocEditor): igoAggregationPipeline; overload;

    function sort(aSort: TgoMongoSort): igoAggregationPipeline; overload;
    function sort(aSortDoc: tgoBsonDocument): igoAggregationPipeline; overload;
    function sort(aSortDocJS: string): igoAggregationPipeline; overload;
    function sort(aSortProc: tgoDocEditor): igoAggregationPipeline; overload;

    function limit(n: Integer): igoAggregationPipeline;
    function &Set(aFields: array of tgoBsonElement): igoAggregationPipeline;
    function UnSet(aFields: array of string): igoAggregationPipeline;

    function Edit(aEditor: tgoDocEditor): igoAggregationPipeline; overload;
    function Edit(aStagenr: Integer; aEditor: tgoDocEditor): igoAggregationPipeline; overload;

    function Project(aProjection: TgoMongoProjection): igoAggregationPipeline; overload;
    function Project(aProjectionDoc: tgoBsonDocument): igoAggregationPipeline; overload;
    function Project(aProjectionDocJS: string): igoAggregationPipeline; overload;
    function Project(aProjectionProc: tgoDocEditor): igoAggregationPipeline; overload;

    function &Out(const aOutputCollection: string): igoAggregationPipeline; overload;
    function &Out(const aOutputDB, aOutputCollection: string): igoAggregationPipeline; overload;

    function batchSize(aBatchsize: Integer): igoAggregationPipeline;
    function maxTimeMS(aMS: Integer): igoAggregationPipeline;
    function Stages: TgoBsonArray;
  end;

  { Represents a collection in a MongoDB database.
    Instances of this interface are aquired by calling
    IgoMongoDatabase.GetCollection. }

  IgoMongoCollection = interface
    ['{9822579B-1682-4FAC-81CF-A4B239777812}']
{$REGION 'Internal Declarations'}
    function _GetDatabase: IgoMongoDatabase;
    function _GetName: string;
{$ENDREGION 'Internal Declarations'}
    { InsertOne: Inserts a single document.

      Parameters:
      ADocument: The document to insert.

      Returns:
      True if document has been successfully inserted. False if not. }
    function InsertOne(const ADocument: tgoBsonDocument): Boolean;

    { InsertMany: Inserts many documents.

      Parameters:
      ADocuments: The documents to insert.

      AOrdered: Optional. If True, perform an ordered insert of the documents
      in the array, and if an error occurs with one of documents, MongoDB
      will return without processing the remaining documents in the array.
      If False, perform an unordered insert, and if an error occurs with one
      of documents, continue processing the remaining documents in the
      array.

      Defaults to true.

      Returns:
      The number of inserted documents. }

    function InsertMany(const ADocuments: array of tgoBsonDocument; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TArray<tgoBsonDocument>; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TEnumerable<tgoBsonDocument>; const AOrdered: Boolean = True): Integer; overload;

    { DeleteOne: Deletes a single document.

      Parameters:
      AFilter: filter containing query operators to search for the document
      to delete.

      Returns:
      True if a document matching the filter has been found and it has
      been successfully deleted. }

    function DeleteOne(const aFilter: tgoMongoFilter): Boolean;

    { DeleteMany: Deletes all documents that match a filter.

      Parameters:
      AFilter: filter containing query operators to search for the documents
      to delete.

      AOrdered: Optional. If True, then when a delete statement fails, return
      without performing the remaining delete statements. If False, then
      when a delete statement fails, continue with the remaining delete
      statements, if any.

      Defaults to true.

      Returns:
      The number of documents deleted. }

    function DeleteMany(const aFilter: tgoMongoFilter; const AOrdered: Boolean = True): Integer;

    { UpdateOne: Updates a single document.

      Parameters:
      AFilter: filter containing query operators to search for the document
      to update.
      AUpdate: the update definition that specifies how the document should
      be updated.
      AUpsert: (optional) upsert flag. If True, perform an insert if no
      documents match the query. Defaults to False.

      Returns:
      True if a document matching the filter has been found and it has
      been successfully updated. }

    function UpdateOne(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = false): Boolean; overload;
    function UpdateOne(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; AUpsert: Boolean; out AUpserted: Boolean): Boolean;
      overload;

    { UpdateMany: Updates all documents that match a filter.

      Parameters:
      AFilter: filter containing query operators to search for the documents
      to update.
      AUpdate: the update definition that specifies how the documents should
      be updated.
      AUpsert: (optional) upsert flag. If True, perform an insert if no
      documents match the query. Defaults to False.
      AOrdered: Optional. If True, then when an update statement fails, return
      without performing the remaining update statements. If False, then
      when an update statement fails, continue with the remaining update
      statements, if any.
      Defaults to true.

      Returns:

      The number of documents that match the filter. The number of documents
      that is actually updated may be less than this in case an update did
      not result in the change of one or more documents.

      AUpserted: will be TRUE if the filter found no matches and a new document was created.

      }
    function UpdateMany(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = false; const AOrdered:
      Boolean = True): Integer; overload;
    function UpdateMany(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; AUpsert, AOrdered: Boolean; out AUpserted: Boolean):
      Integer; overload;

    { Find: Finds the documents matching the filter.

      Parameters:

      AOptions: a fluent interface that will let you specify all options such
      as the filter, projection, sorting.

      Legacy parameters, overloaded :

      AFilter: (optional) filter containing query operators to search for
      documents that match the filter. If not specified, then all documents
      in the collection are returned.

      AProjection: (optional) projection that specifies the fields to return
      in the documents that match the query filter. If not specified, then
      all fields are returned.

      ASort: (optional) sort modifier, used to sort the results. Note: an
      exception is raised when the result set is very large (32MB or larger)
      and cannot be sorted.

      Returns:
      An enumerable of documents that match the filter. The enumerable will
      be empty if there are no documents that match the filter.
      Enumerating over the result may trigger additional calls to the MongoDB
      server. }

    function Find(AOptions: igoMongoFindOptions): igoMongoCursor; overload;
    function Find: igoMongoCursor; overload;
    function Find(const aFilter: tgoMongoFilter): igoMongoCursor; overload;
    function Find(const aProjection: TgoMongoProjection): igoMongoCursor; overload;
    function Find(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection): igoMongoCursor; overload;
    function Find(const aFilter: tgoMongoFilter; const aSort: TgoMongoSort): igoMongoCursor; overload;
    function Find(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection; const aSort: TgoMongoSort; aSkip: Integer = 0):
      igoMongoCursor; overload;

    {Aggregate: First basic implementation of MongoDB Aggregates.
    See igoAggregationPipeline for details}

    function Aggregate(APipeLine: igoAggregationPipeline): igoMongoCursor; overload;
    function Aggregate(APipeLine: TgoBsonArray; aBatchsize: Integer = 0; aMaxTimeMS: Integer = 0): igoMongoCursor; overload;

    { FindOne: Finds the first document matching the filter.

      Parameters:

      AOptions: a fluent interface that will let you specify all options such
      as the filter, projection, sorting.

      Legacy Parameters:

      AFilter: filter containing query operators to search for the document
      that matches the filter.

      ASort: (optional) use this to find the maximum or minimum value of a field.
      An empty filter (tgomongofilter.Empty) with ASort=tgomongosort.Descending('price')
      will return the document having the highest 'price'.
      For best performance, use indexes in the collection.

      AProjection: (optional) projection that specifies the fields to return
      in the document that matches the query filter. If not specified, then
      all fields are returned.

      Returns:
      The first document that matches the filter. If no documents match the
      filter, then a null-documents is returned (call its IsNil method to
      check for this). }

    function FindOne(AOptions: igoMongoFindOptions): tgoBsonDocument; overload;
    function FindOne(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection): tgoBsonDocument; overload;
    function FindOne(const aFilter: tgoMongoFilter): tgoBsonDocument; overload;
    function FindOne(const aFilter: tgoMongoFilter; const aSort: TgoMongoSort): tgoBsonDocument; overload;
    function FindOne(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection; const aSort: TgoMongoSort): tgoBsonDocument;
      overload;

    { Counts the number of documents matching the filter.

      Parameters:
      AFilter: (optional) filter containing query operators to search for
      documents that match the filter. If not specified, then the total
      number of documents in the collection is returned.

      Returns:
      The number of documents that match the filter. }
    function Count: Integer; overload;
    function Count(const aFilter: tgoMongoFilter): Integer; overload;

    {Faster than count, but returns only an estimate of the number of documents}
    function EstimatedDocumentCount: Integer;

    {Creates an empty cursor. Used in routines that return an empty result set.}
    function EmptyCursor: igoMongoCursor;

    { Creates an index in the current collection.

      Parameters:
      AName: Name of the index.
      AKeyFields: List of fields to build the index.
      AUnique: Defines a unique index.

      Returns:
      Created or not. }
    function CreateIndex(const AName: string; const AKeyFields: array of string; const AUnique: Boolean = false): Boolean;

    { Creates an text index in the current collection.

      Parameters:
      AName: Name of the index.
      AFields: List of fields to build the index.
      ALanguageOverwriteField: Defines a field that contains the language to
      use for a specific document.
      ADefaultLanguage: Defines the default language for internal indexing.

      See https://docs.mongodb.com/manual/reference/text-search-languages/#text-search-languages
      for language definitions.

      Returns:
      Created or not. }
    function CreateTextIndex(const AName: string; const aFields: array of string; const ALanguageOverwriteField: string = ''; const
      ADefaultLanguage: string = 'en'): Boolean;

    { Drops an index in the current collection.

      Parameters:
      AName: Name of the index.

      Returns:
      Dropped or not. }
    function DropIndex(const AName: string): Boolean; overload;

    { List all index names in the current collection.

      Returns:
      TArray<String> of index names. }
    function ListIndexNames: TArray<string>; overload;
    function ListIndexes: TArray<tgoBsonDocument>; overload;

    { Return statistics about the collection, see
      https://www.mongodb.com/docs/manual/reference/command/collStats }
    function Stats: tgoBsonDocument;
    function GetReadPreference: tgoMongoReadPreference;
    procedure SetReadPreference(const Value: tgoMongoReadPreference);

    {Returns the protocol of database.CLIENT}
    function GetProtocol: tgoMongoProtocol;

    { The database that contains this collection. }
    property Database: IgoMongoDatabase read _GetDatabase;

    { The name of the collection. }
    property name: string read _GetName;

    { setting ReadPreference on the collection will override the global readpreference }
    property ReadPreference: tgoMongoReadPreference read GetReadPreference write SetReadPreference;
    property Protocol: tgoMongoProtocol read GetProtocol;
  end;

type
  { Can be passed to the constructor of TgoMongoClient to customize the
    client settings. }
  TgoMongoClientSettings = record
  public
    { Timeout waiting for connection, in milliseconds.
      Defaults to 5000 (5 seconds) }
    ConnectionTimeout: Integer;

    { Timeout waiting for partial or complete reply events, in milliseconds.
      Defaults to 30000 (30 seconds) }
    ReplyTimeout: Integer;

    { Default query flags }
    QueryFlags: TgoMongoQueryFlags;

    { Tls enabled }
    Secure: Boolean;

    { X.509 Certificate in PEM format, if any }
    Certificate: TBytes;

    { X.509 Private key in PEM format, if any }
    PrivateKey: TBytes;

    { Password for private key, optional }
    PrivateKeyPassword: string;

    { Authentication mechanism }
    AuthMechanism: TgoMongoAuthMechanism;

    { Authentication database }
    AuthDatabase: string;

    { Authentication username }
    Username: string;

    { Authentication password }
    Password: string;

    ApplicationName: string;
    UseSnappyCompression: Boolean;
    UseZlibCompression: Boolean;
    GlobalReadPreference: tgoMongoReadPreference;
  public
    { Creates a settings record with the default settings }
    class function Create: TgoMongoClientSettings; static;
  end;

type
  { Implements IgoMongoClient.
    This is the main entry point to the MongoDB API. }
  TgoMongoClient = class(TInterfacedObject, IgoMongoClient)
  public
    const
      { Default host address of the MongoDB server. }
      DEFAULT_HOST = 'localhost';
      { Default connection port. }
      DEFAULT_PORT = 27017;
{$REGION 'Internal Declarations'}
  private
    FProtocol: tgoMongoProtocol;
    fAvailable: Boolean;
    fPooled: Boolean;

    fInTransaction: Boolean;
    fTransactionActivated: Boolean;
    fTransactionNumber: Int64;
    flsID: tGuid;

    function GetGlobalReadPreference: tgoMongoReadPreference;
    procedure SetGlobalReadPreference(const Value: tgoMongoReadPreference);
    function GetProtocol: tgoMongoProtocol;
    function getAvailable: Boolean;
    function getPooled: Boolean;
    procedure setAvailable(const Value: Boolean);
    procedure setPooled(const Value: Boolean);

    procedure _EmitTransactionMetadata(const Writer: IgoBsonWriter);

  protected
    { IgoMongoClient }
    function ListDatabaseNames: TArray<string>;
    function ListDatabases: TArray<tgoBsonDocument>;
    procedure DropDatabase(const AName: string);
    function GetDatabase(const AName: string): IgoMongoDatabase;
    function GetInstanceInfo(const ASaslSupportedMechs: string = ''; const AComment: string = ''): TgoMongoInstanceInfo;
    function IsMaster: Boolean;
    function AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;
    function LogRotate: Boolean;
    function BuildInfo: tgoBsonDocument;
    function HostInfo: tgoBsonDocument;
    function Features: tgoBsonDocument;
    function getConnected: Boolean;
    procedure setConnected(const Value: Boolean);
    procedure ReleaseToPool;
    function EmptyCursor: igoMongoCursor;
    procedure IncludeTransactionDetails(const Writer: IgoBsonWriter);
    function SupportsTransactions: Boolean;
    procedure AbortTransaction;
    procedure StartTransaction;
    procedure CommitTransaction;
    procedure UnpinTransaction;
{$ENDREGION 'Internal Declarations'}
  public

    { Creates a client interface to MongoDB.

      Parameters:
      AHost: (optional) host address of the MongoDB server to connect to.
      Defaults to 'localhost'.
      APort: (optional) connection port. Defaults to 27017.
      ASettings: (optional) client settings.

      NOTE: The constructor is light weight and does NOT connect to the server
      until the first read, write or query operation. }
    constructor Create(const AHost: string = DEFAULT_HOST; const APort: Integer = DEFAULT_PORT); overload;
    constructor Create(const AHost: string; const APort: Integer; const ASettings: TgoMongoClientSettings); overload;
    constructor Create(const ASettings: TgoMongoClientSettings); overload;
    destructor Destroy; override;

    { GlobalReadPreference sets the global ReadPreference for all objects (database, collection etc)
      that do not have an individual specific ReadPreference. }
    property GlobalReadPreference: tgoMongoReadPreference read GetGlobalReadPreference write SetGlobalReadPreference;
    property Pooled: Boolean read getPooled write setPooled; //Client is inside a connection pool
    property Available: Boolean read getAvailable write setAvailable; //Client is available (not in use)
    property Connected: Boolean read getConnected write setConnected;
    property Protocol: tgoMongoProtocol read GetProtocol;
  end;

  {igoConnectionPool is a connection pool of client connections,
   intended for a multi-tasking environment where worker threads
   may temporarily need connections and where it is advantageous
   (performance-wise and latency-wise) to re-use existing connections
   instead of having to establish new ones: The whole connection
   sequence is skipped if the client is already connected.}

  igoConnectionPool = interface
    ['{D4ED8586-16BF-4F3C-86A4-13DDA92694AA}']
    function GetConnectionSettings: TgoMongoClientSettings;
    function getHost: string;
    function getPort: Integer;
    function GetAvailableClient: IgoMongoClient; //grabs an available client connection from the pool.
    procedure Remove(Client: IgoMongoClient);
    procedure ReleaseToPool(const Client: IgoMongoClient); //Releases the connection back to the pool
    procedure ClearAll; //Removes ALL connections;
    procedure Purge; //Deletes currently unused connections
    property ConnectionSettings: TgoMongoClientSettings read GetConnectionSettings;
    property Host: string read getHost;
    property Port: Integer read getPort;
  end;

  tgoConnectionPool = class(TInterfacedObject, igoConnectionPool)
    flock: tCriticalSection;
    fHost: string;
    fPort: Integer;
    fMaxItems: Integer;
    fConnectionSettings: TgoMongoClientSettings;
    flist: tlist<IgoMongoClient>;
    function GetConnectionSettings: TgoMongoClientSettings;
    function getHost: string;
    function getPort: Integer;
  public
    constructor Create(const AHost: string; APort: Integer; const ASettings: TgoMongoClientSettings; aMaxitems: Integer);
    destructor Destroy; override;
    function GetAvailableClient: IgoMongoClient; //grabs an available client connection from the pool.
    procedure ReleaseToPool(const Client: IgoMongoClient); //Releases the connection back to the pool
    procedure Remove(Client: IgoMongoClient);
    procedure ClearAll;
    procedure Purge; //Deletes currently unused connections
    property ConnectionSettings: TgoMongoClientSettings read GetConnectionSettings;
    property Host: string read getHost;
    property Port: Integer read getPort;
  end;

type
  {tgoCursorhelper can determine if a BSON DOC contains a cursor and create an igoMongoCursor from it}
  tgoCursorhelper = class
    class function HasCursor(const ADoc: tgoBsonDocument; var Cursor: tgoBsonDocument; var CursorID: Int64; var Namespace: string):
      Boolean;
      inline;
    class function CreateCursor(const ADoc: tgoBsonDocument; AProtocol: tgoMongoProtocol; AReadPreference: tgoMongoReadPreference):
      igoMongoCursor;
    class function ToDocArray(const aCursor: igoMongoCursor): TArray<tgoBsonDocument>;
    class function ToBsonArray(const DocArray: TArray<tgoBsonDocument>): TgoBsonArray; overload;
    class function ToBsonArray(const aCursor: igoMongoCursor): TgoBsonArray; overload;
    class function FirstDoc(const Docs: TArray<tgoBsonDocument>): tgoBsonDocument;
  end;

  (*tgoMongoExpression

   Helper class that lets you build a MongoDB AGGREGATE EXPRESSION.

   It maps Mongo functions into Delphi functions and the nested
   functions will build the Json string.

   The advantage of this is that there are fewer worries about
   syntax, parentheses, braces and quotes. The code simply won't
   compile if a mistake was made.

   Also, this is one of the few cases where you might want to use the "WITH"
   statement and save a lot of typing.

   Beware:

     -Expressions that are constant values (strings, numbers, bools) must be passed
      as &CONST(value)

     -Expressions that are references to field contents must be passed
      as REF(fieldname)

     "Literals" are an internal MongoDB concept. They represent expressions
     that must be taken literally and not expanded.

   Example:

   WITH tgoMongoExpression do
   begin
      MyDoc ['FullName'] := asBson  (concat( [ ref('FirstName'),  &const(' '), ref('LastName') ]));

      is identical to JSON:    { FullName: { $concat: [ $FirstName, " ", $LastName] } }
     ...
   end;

   The implementation is not yet feature complete.  For example, trigonometry is still missing.

   *)

  tgoMongoExpression = class

    //Converts Json into a Bson Value. The result may be a document, a string, an array ....
    class function asBson(const json: string): tgoBsonValue; static;

    //reference the contents of a field
    class function ref(const FieldName: string): string; static; //reference to a field. fieldname --> "$fieldname"

    class function Count(): string; static; // for group stage, counts the documents.

    //&Const : pass a string, number or boolean
    class function &const(const ConstantValue: string): string; overload; static; //  string.  John  --> "John".
    class function &const(const ConstantValue: Int64): string; overload; static; // integer. 500   --> 500
    class function &const(const ConstantValue: Boolean): string; overload; static; // boolean. input --> true or false.
    class function &const(const ConstantValue: Double; Decimals: Integer): string; overload; static; //Float. pi,4 --> 3.1415

    // see https://www.mongodb.com/docs/v7.0/reference/operator/aggregation/literal/#mongodb-expression-exp.-literal
    // "literal" means: "do not interpret/expand this expression"
    class function literal(const Expr: string): string; overload; static;

    //Type determination
    class function isNumber(const Expr: string): string;

    // see https://www.mongodb.com/docs/v7.0/reference/operator/aggregation/type/#mongodb-expression-exp.-type
    // missing, string, regex, double, int, long, object, array, date, ObjectId, bool, timestamp,decimal,
    // null, minKey, maxKey, javascript ....
    class function &type(const Expr: string): string;

    //Conversions
    class function convert(const input: string; const &to: string; const onError: string = ''; const onNull: string = ''): string; static;
    class function toBool(const Expr: string): string; static; //convert expression to boolean
    class function ToString(const Expr: string): string; reintroduce; static; //convert expression to string
    class function toInt(const Expr: string): string; static; //convert expression to int32
    class function toLong(const Expr: string): string; static; //convert expression (date...) to long
    class function toDecimal(const Expr: string): string; static; //convert expression to decimal
    class function toDouble(const Expr: string): string; static; //convert expression to double
    class function toObjectId(const Expr: string): string; static; //convert expression to objectid
    class function toLower(const Expr: string): string; static; //convert expression to lower case
    class function toUpper(const Expr: string): string; static; //convert expression to upper case
    class function toUUID(const Expr: string): string; static; //convert expression to UUID
    class function toDate(const Expr: string): string; static; //convert expression to date

    //Rounding of floats
    class function abs(const Expr: string): string; static; //calculate the absolute value of a number.
    class function ceil(const Expr: string): string; static; //round a float or decimal up if it isn't an integer
    class function floor(const Expr: string): string; static; //round a float or decimal down if it isn't an integer
    class function round(const Expr: string; const place: string = '0'): string; static; //round a number up or down to "place" decimals
    class function trunc(const Expr: string; const place: string = '0'): string; static; //truncate a float or decimal to "place" decimals

    //math functions
    class function exp(const Expr: string): string; static; //calculate e^x
    class function ln(const Expr: string): string; static; // calculate natural logarithm
    class function log(const Expr1, Expr2: string): string; static; // calculate logarithm for any positive vase
    class function log10(const Expr: string): string; static; // calculate 10log(x)
    class function pow(const Expr1, Expr2: string): string; static; // calculate a^b
    class function sqrt(const Expr: string): string; static; //calculate square root

    //Bit operations (new in 6.3)

    // Handling of date/time . not feature complete.

    class function dateToString(const DateExpr: string; //UTC! must evaluate to Date, Timestamp or ObjectID
      const fmt: string = '"%Y-%m-%dT%H:%M:%S.%LZ"'; //format of the result
      const timezone: string = ''; //timezone of the result. e.g.  &const('+04:45') or &const('Europe/London').
      const onNull: string = '' //the value to return if date is null. Default is null.
      ): string; static;

    //new in 5.0
    class function dateAdd(const StartDate: string; //UTC! must evaluate to Date, Timestamp or ObjectID
      const &unit: string; //e.g. const('year') , quarter, week, month, day, hour, minute,second,millisecond
      const amount: string = ''; //MUST EVALUATE TO INTEGER OR LONG.  e.g. &const(1).
      const timezone: string = '' //timezone of the result. e.g.  &const('+04:45') or &const('Europe/London').
      ): string; static;

    //new in 5.0
    class function dateSubtract(const StartDate: string; //UTC! must evaluate to Date, Timestamp or ObjectID
      const &unit: string; //e.g. const('year') , quarter, week, month, day, hour, minute,second,millisecond
      const amount: string = ''; //MUST EVALUATE TO INTEGER OR LONG.  e.g. &const(1).
      const timezone: string = '' //timezone of the result. e.g.  &const('+04:45') or &const('Europe/London').
      ): string; static;

    class function millisecond(const DateExpr: string; const timezone: string = ''): string; static;
    //extract the milliseconds of a date (0-999)
    class function second(const DateExpr: string; const timezone: string = ''): string; static; //extract the seconds of a date (0-59)
    class function minute(const DateExpr: string; const timezone: string = ''): string; static; //extract the minutes of a date (0-59)
    class function hour(const DateExpr: string; const timezone: string = ''): string; static; //extract the hours of a date (0-23)
    class function dayOfMonth(const DateExpr: string; const timezone: string = ''): string; static;
    //extract the day number of the month (1-31) of a date.
    class function dayOfYear(const DateExpr: string; const timezone: string = ''): string; static;
    //extract the year's day number (1-366) of a date.
    class function dayOfWeek(const DateExpr: string; const timezone: string = ''): string; static;
    //extract the day number of the week (1=sunday - 7=saturday) of a date.
    class function week(const DateExpr: string; const timezone: string = ''): string; static;
    //extract the week (0-53) of a date. The first week of a year begins on a sunday.
    class function month(const DateExpr: string; const timezone: string = ''): string; static;
    //extract the month (1-12) of a date. 1=january
    class function year(const DateExpr: string; const timezone: string = ''): string; static; //extract the year of a date.

    // Note: Isoweek: the week containing the first THURSDAY of a year is week 1 and starts on a MONDAY.
    class function ISOweek(const DateExpr: string; const timezone: string = ''): string; static; //extract the week (1-53) of a date.

    //Note: the ISO year starts on the MONDAY of ISO week 1, so it may even start in the previous year ...
    class function ISOweekYear(const DateExpr: string; const timezone: string = ''): string; static; //extract the Iso Year of a date.

    //Basic math
    class function add(const Expr1, Expr2: string): string; static; //add two numbers
    class function subtract(const Expr1, Expr2: string): string; static; //subtract two numbers Expr2-Expr1
    class function multiply(const Expr1, Expr2: string): string; static; //multiply two numbers
    class function divide(const Expr1, Expr2: string): string; static; //divide two numbers Expr1/Expr2
    class function &mod(const Expr1, Expr2: string): string; static; //divide two numbers Expr1/Expr2

    //Aggregation
    class function first(const Expr: string): string; static; //Eval expression for first record in a group
    class function last(const Expr: string): string; static; //Eval expression for last record in a group
    class function avg(const Expr: string): string; static; //calc avg value or expression for all records in a group
    class function max(const Expr: string): string; static; //calc max value or expression for all records in a group
    class function min(const Expr: string): string; static; //calc min value or expression for all records in a group

    //Basic string manipulation
    class function concat(const Expr: array of string): string; static; //concatenate strings
    class function &array(const Expr: array of string): string; static; //convert x expressions into a json array
    class function split(const Expr, Delimiter: string): string; static; //Split a string into an array
    class function substr(const Expr, start, len: string): string; static; // Extract a substring
    class function ltrim(const Expr: string; const NumChars: string = ''): string; static; //Trim whitespace left OR remove N chars left
    class function rtrim(const Expr: string; const NumChars: string = ''): string; static; //Trim whitespace right OR remove N chars right

    //comparison with integer result
    class function cmp(const Expr1, Expr2: string): string; static; //compare two numbers, return an integer

    //Comparison with boolean result
    class function &eq(const Expr1, Expr2: string): string; static; //Result= (a=b)
    class function gt(const Expr1, Expr2: string): string; static; //Result= (a>b)
    class function gte(const Expr1, Expr2: string): string; static; //Result= (a>=b)
    class function lt(const Expr1, Expr2: string): string; static; //Result= (a<b)
    class function lte(const Expr1, Expr2: string): string; static; //Result= (a<=b)

    //boolean operators
    class function &and(const Expr: array of string): string; static; //true if logical AND of all elements is true.
    class function &or(const Expr: array of string): string; static; //true if any element is true
    class function &not(const Expr: string): string; static; //negates one boolean expression

    //boolean comparison with result of any type
    class function cond(const aIf, aThen, aElse: string): string; static; //if aIf is true, it returns aThen, else aElse.

    //Bitwise operators, new in 6.3
    class function bitAnd(const Expr: array of string): string; static;
    class function bitOr(const Expr: array of string): string; static;
    class function bitXor(const Expr: array of string): string; static;
    class function bitNot(const Expr1: string): string; static;

    //Array functions
    class function arrayElementAt(const Expr1: array of string; const Expr2: string): string; static; //lookup
    class function &in(const Expr1: string; const Expr2: array of string): string; static; //lookup with boolean result
    class function ifNull(const Expr: array of string): string; static; //returns the first item of the expression that is not null.

  end;
  tgoMongoExpressionClass = class of tgoMongoExpression;

function FindOptions: igoMongoFindOptions; // class factory

type
  Aggregate = class
  public
    class function CreatePipeline: igoAggregationPipeline; //class factory for fluid interface
    class function Expression: tgoMongoExpressionClass; //class factory for fluid interface
  end;

resourcestring
  RS_MONGODB_CONNECTION_ERROR = 'Error connecting to the MongoDB database';
  RS_MONGODB_GENERIC_ERROR = 'Unspecified error while performing MongoDB operation';

implementation

uses
  System.Math;

const
  NoCursorID = 0;

{$POINTERMATH ON}

  { If timeout, or error message, throw exception }
function HandleCommandReply(AProtocol: tgoMongoProtocol; const AReply: IgoMongoReply; const AErrorToIgnore: TgoMongoErrorCode =
  TgoMongoErrorCode.OK): Integer;
var
  doc, ErrorDoc: tgoBsonDocument;
  Value: tgoBsonValue;
  Values: TgoBsonArray;
  OK: Boolean;
  ErrorCode: TgoMongoErrorCode;
  ErrorMsg, codeName: string;

  procedure GetErrorCodes(const ADoc: tgoBsonDocument);
  begin
    Word(ErrorCode) := ADoc['code'];
    ErrorMsg := ADoc['errmsg'];
    codeName := ADoc['codeName'];
    if codeName <> '' then
      codeName := ' [' + codeName + ']';
  end;

begin
  if (AReply = nil) then { usually means timeout }
  begin
    AProtocol.Connected := false;
    AProtocol.ConnectionFailedException(RS_MONGODB_CONNECTION_ERROR);
  end;

  doc := AReply.FirstDoc;
  if doc.IsNil then
    Exit(0); { No document - just assume that everything OK }

  Result := doc['n']; { Return number of documents affected. If "n" is missing, result is 0}

  OK := doc['ok']; {"ok" should normally be present, except in some extreme aggregation pipeline
  corner cases in MongoDB versions < 4.2}

  if (not OK) then {is ok missing or 0?}
  begin
    { First check for top-level error }
    GetErrorCodes(doc);
    if (ErrorCode <> TgoMongoErrorCode.OK) then
    begin
      if (ErrorCode = AErrorToIgnore) then
        Exit { ignore an "expected" error code}
      else
      begin
        if tgoMongoProtocol.IsInternalError(ord(ErrorCode)) then
          raise EgoMongoDBProtocolError.Create(ErrorCode, ErrorMsg + codeName)

        else
          raise EgoMongoDBGeneralError.Create(ErrorCode, ErrorMsg + codeName);
      end;
    end;

    { If there is no top-level error, then check for Write Error(s).
      Raise exception for first write error found. }

    if (doc.TryGetValue('writeErrors', Value)) then
    begin
      Values := Value.AsBsonArray;
      if (Values.Count > 0) then
      begin
        ErrorDoc := Values.Items[0].asBsonDocument;
        GetErrorCodes(ErrorDoc);
        raise EGoMongoDBWriteError.Create(ErrorCode, ErrorMsg + codeName);
      end;
    end;

    { If there are no write errors either, then check for write concern error. }

    if (doc.TryGetValue('writeConcernError', Value)) then
    begin
      ErrorDoc := Value.asBsonDocument;
      GetErrorCodes(ErrorDoc);
      raise EGoMongoDBWriteConcernError.Create(ErrorCode, ErrorMsg);
    end;

    { Could not detect any errors in reply.
      Raise generic error because "ok" is either missing or 0. }

    raise EgoMongoDBError.Create(RS_MONGODB_GENERIC_ERROR);
  end;
end;

procedure DoSpecifyReadPreference(AReadPreference: tgoMongoReadPreference; const AWriter: IgoBsonWriter);
begin
  if AReadPreference <> tgoMongoReadPreference.Primary then
  begin
    AWriter.WriteStartDocument('$readPreference');
    case AReadPreference of
      tgoMongoReadPreference.Primary:
        AWriter.WriteString('mode', 'primary');
      tgoMongoReadPreference.primaryPreferred:
        AWriter.WriteString('mode', 'primaryPreferred');
      tgoMongoReadPreference.secondary:
        AWriter.WriteString('mode', 'secondary');
      tgoMongoReadPreference.secondaryPreferred:
        AWriter.WriteString('mode', 'secondaryPreferred');
      tgoMongoReadPreference.nearest:
        AWriter.WriteString('mode', 'nearest');
    end;
    AWriter.WriteEndDocument;
  end;
end;

type
  {tgoMongoCursor is a cursor engine. It implements a for ... in enumerator that automatically
  retrieves the next batch of documents if its buffer is exhausted.}
  TgoMongoCursor = class(TInterfacedObject, igoMongoCursor)
{$REGION 'Internal Declarations'}
  private
    type
      TEnumerator = class(TEnumerator<tgoBsonDocument>)
      private
        FProtocol: tgoMongoProtocol; // Reference
        FDatabaseName: string;
        FCollectionName: string;
        FPage: TArray<TBytes>;
        FCursorId: Int64;
        FIndex: Integer;
        FReadPreference: tgoMongoReadPreference;
      private
        procedure GetMore;
        procedure SpecifyDB(const Writer: IgoBsonWriter);
        procedure SpecifyReadPreference(const AWriter: IgoBsonWriter);
      protected
        function DoGetCurrent: tgoBsonDocument; override;
        function DoMoveNext: Boolean; override;
      public
        destructor Destroy; override;
        constructor Create(const AProtocol: tgoMongoProtocol; AReadPreference: tgoMongoReadPreference; const ADatabaseName,
          ACollectionName:
          string; const APage: TArray<TBytes>; const ACursorId: Int64);
      end;
  private
    FProtocol: tgoMongoProtocol; // Reference
    FDatabaseName: string;
    FCollectionName: string;
    FInitialPage: TArray<TBytes>;
    FInitialCursorId: Int64;
    FReadPreference: tgoMongoReadPreference;
  public
    { IgoMongoCursor }
    function GetEnumerator: TEnumerator<tgoBsonDocument>;
    function ToArray: TArray<tgoBsonDocument>;
  public
    constructor Create(const AProtocol: tgoMongoProtocol; AReadPreference: tgoMongoReadPreference; const ADatabaseName, ACollectionName:
      string; const AInitialPage: TArray<TBytes>; const AInitialCursorId: Int64); overload;

    constructor Create(const AProtocol: tgoMongoProtocol; AReadPreference: tgoMongoReadPreference; const aNameSpace: string; const
      AInitialPage: TArray<TBytes>; const AInitialCursorId: Int64); overload;

{$ENDREGION 'Internal Declarations'}
  end;

  {tgoMongoDatabase represents a database inside MongoDB.
  It knows the name of the database and holds a reference
  to igoMongoClient whose "protocol" it uses for communication}

  TgoMongoDatabase = class(TInterfacedObject, IgoMongoDatabase)
{$REGION 'Internal Declarations'}
  private
    FClient: IgoMongoClient;
    FName: string;
    FReadPreference: tgoMongoReadPreference;

    function GetReadPreference: tgoMongoReadPreference;
    procedure SetReadPreference(const Value: tgoMongoReadPreference);
    procedure SpecifyDB(const AWriter: IgoBsonWriter);
    procedure SpecifyReadPreference(const AWriter: IgoBsonWriter);
    function GetProtocol: tgoMongoProtocol;

  protected
    { IgoMongoDatabase }
    function _GetClient: IgoMongoClient;
    function _GetName: string;

    function ListCollectionNames: TArray<string>;
    function ListCollections: TArray<tgoBsonDocument>;
    procedure DropCollection(const AName: string);
    function GetCollection(const AName: string): IgoMongoCollection;
    function CreateCollection(const AName: string; const ACapped: Boolean; const AMaxSize: Int64; const AMaxDocuments: Int64; const
      AValidationLevel: TgoMongoValidationLevel; const AValidationAction: TgoMongoValidationAction; const AValidator: tgoBsonDocument; const
      ACollation: TgoMongoCollation): Boolean;
    function RenameCollection(const AFromNamespace, AToNamespace: string; const ADropTarget: Boolean = false): Boolean;
    function GetDbStats(const AScale: Integer): TgoMongoStatistics;
    function Command(CommandToIssue: tWriteCmd): igoMongoCursor;
    function AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;

  protected
    property Protocol: tgoMongoProtocol read GetProtocol;
    property name: string read FName;
{$ENDREGION 'Internal Declarations'}
  public
    constructor Create(const AClient: TgoMongoClient; const AName: string);
    property ReadPreference: tgoMongoReadPreference read GetReadPreference write SetReadPreference;
  end;

  {tgoMongoCollection represents a collection inside a database.
  It knows the name of the collection and holds a reference to an
  igoMongoDatabase}

  TgoMongoCollection = class(TInterfacedObject, IgoMongoCollection)
{$REGION 'Internal Declarations'}
  private
    type
      PgoBsonDocument = ^tgoBsonDocument;
  private
    FDatabase: IgoMongoDatabase;
    FName: string;
    FReadPreference: tgoMongoReadPreference;
  private
    procedure AddWriteConcern(const AWriter: IgoBsonWriter {TODO: Parameterlist});
    procedure SpecifyDB(const AWriter: IgoBsonWriter);
    procedure SpecifyReadPreference(const AWriter: IgoBsonWriter);

    function InsertMany(const ADocuments: PgoBsonDocument; const ACount: Integer; const AOrdered: Boolean): Integer; overload;
    function Delete(const aFilter: tgoMongoFilter; const AOrdered: Boolean; const ALimit: Integer): Integer;
    function Update(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert, AOrdered, AMulti: Boolean; out Upserted:
      Boolean): Integer;
    function GetReadPreference: tgoMongoReadPreference;
    procedure SetReadPreference(const Value: tgoMongoReadPreference);
    function GetProtocol: tgoMongoProtocol;
  protected
    { IgoMongoCollection }
    function _GetDatabase: IgoMongoDatabase;
    function _GetName: string;

    function InsertOne(const ADocument: tgoBsonDocument): Boolean;
    function InsertMany(const ADocuments: array of tgoBsonDocument; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TArray<tgoBsonDocument>; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TEnumerable<tgoBsonDocument>; const AOrdered: Boolean = True): Integer; overload;

    function DeleteOne(const aFilter: tgoMongoFilter): Boolean;
    function DeleteMany(const aFilter: tgoMongoFilter; const AOrdered: Boolean = True): Integer;

    function UpdateOne(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = false): Boolean; overload;
    function UpdateOne(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; AUpsert: Boolean; out AUpserted: Boolean): Boolean;
      overload;

    function UpdateMany(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = false; const AOrdered:
      Boolean = True): Integer; overload;
    function UpdateMany(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; AUpsert, AOrdered: Boolean; out AUpserted: Boolean):
      Integer; overload;

    function Aggregate(APipeLine: TgoBsonArray; aBatchsize: Integer = 0; aMaxTimeMS: Integer = 0): igoMongoCursor; overload;
    function Aggregate(APipeLine: igoAggregationPipeline): igoMongoCursor; overload;

    function EmptyCursor: igoMongoCursor;
    function Find: igoMongoCursor; overload;
    function Find(const aProjection: TgoMongoProjection): igoMongoCursor; overload;
    function Find(const aFilter: tgoMongoFilter): igoMongoCursor; overload;
    function Find(const aFilter: tgoMongoFilter; const aSort: TgoMongoSort): igoMongoCursor; overload;
    function Find(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection): igoMongoCursor; overload;
    function Find(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection; const aSort: TgoMongoSort; aSkip: Integer = 0):
      igoMongoCursor; overload;

    function Find(AOptions: igoMongoFindOptions): igoMongoCursor; overload;
    function FindOne(AOptions: igoMongoFindOptions): tgoBsonDocument; overload;
    function FindOne(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection): tgoBsonDocument; overload;
    function FindOne(const aFilter: tgoMongoFilter): tgoBsonDocument; overload;
    function FindOne(const aFilter: tgoMongoFilter; const aSort: TgoMongoSort): tgoBsonDocument; overload;
    function FindOne(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection; const aSort: TgoMongoSort): tgoBsonDocument;
      overload;

    function Count: Integer; overload;
    function Count(const aFilter: tgoMongoFilter): Integer; overload;
    function EstimatedDocumentCount: Integer;

    function CreateIndex(const AName: string; const AKeyFields: array of string; const AUnique: Boolean = false): Boolean;
    function CreateTextIndex(const AName: string; const aFields: array of string; const ALanguageOverwriteField: string = ''; const
      ADefaultLanguage: string = 'en'): Boolean;
    function DropIndex(const AName: string): Boolean;
    function ListIndexNames: TArray<string>;
    function ListIndexes: TArray<tgoBsonDocument>;
    function Stats: tgoBsonDocument;
{$ENDREGION 'Internal Declarations'}
  public
    property Protocol: tgoMongoProtocol read GetProtocol;
    property ReadPreference: tgoMongoReadPreference read GetReadPreference write SetReadPreference;
    constructor Create(const ADatabase: TgoMongoDatabase; const AName: string);
  end;

  //
{$REGION 'EgoMongoDBGeneralError'}

constructor EgoMongoDBGeneralError.Create(const AErrorCode: TgoMongoErrorCode; const AErrorMsg: string);
begin
  inherited Create(AErrorMsg + Format(' (error %d)', [ord(AErrorCode)]));
  FErrorCode := AErrorCode;
end;

{$ENDREGION}
//
{$REGION 'tgoCursorhelper'}

{hascursor determines if a BSON DOC contains a cursor}
class function tgoCursorhelper.HasCursor(const ADoc: tgoBsonDocument; var Cursor: tgoBsonDocument; var CursorID: Int64; var Namespace:
  string): Boolean;
var
  temp: tgoBsonValue;
begin
  Cursor.SetNil;
  CursorID := 0;
  Namespace := '';
  Result := (ADoc.TryGetValue('cursor', temp));
  if Result then
  begin
    Cursor := temp.asBsonDocument;
    CursorID := Cursor['id']; // 0=cursor exhausted, else more data can be pulled
    Namespace := Cursor.Get('ns', '').ToString(); // databasename.CollectionNameOrCommand
  end;
end;

{CreateCursor analyzes a BSON document and makes a cursor out of it.
 If the doc is NIL, it  returns NIL
 if the doc is empty, it returns an empty igoMongoCursor
 If the doc contains just one document, it creates a igoMongoCursor that will return just this one document.
 If the doc contains a "cursor" element, it will create a fully functional igoMongoCursor }

class function tgoCursorhelper.CreateCursor(const ADoc: tgoBsonDocument; AProtocol: tgoMongoProtocol; AReadPreference:
  tgoMongoReadPreference): igoMongoCursor;
var
  Cursor: tgoBsonDocument;
  Value: tgoBsonValue;
  I: Integer;
  CursorID: Int64;
  Namespace: string;
  Docs: TgoBsonArray;
  InitialPage: TArray<TBytes>;
begin
  if not ADoc.IsNil then
  begin
    if HasCursor(ADoc, Cursor, CursorID, Namespace) then
    begin
      if (Cursor.TryGetValue('firstBatch', Value)) then
      begin
        // Note: The firstBatch array may be an empty resultset.
        Docs := Value.AsBsonArray;
        SetLength(InitialPage, Docs.Count);
        for I := 0 to Docs.Count - 1 do
          InitialPage[I] := Docs[I].asBsonDocument.ToBson;
        Result := TgoMongoCursor.Create(AProtocol, AReadPreference, Namespace, InitialPage, CursorID);
      end;
    end
    else // Some admin queries return just one document
    begin
      SetLength(InitialPage, 1);
      InitialPage[0] := ADoc.ToBson;
      Result := TgoMongoCursor.Create(AProtocol, AReadPreference, 'null.null', InitialPage, NoCursorID);
    end;
  end
  else
  begin
    // Empty Cursor
    SetLength(InitialPage, 0);
    Result := TgoMongoCursor.Create(AProtocol, AReadPreference, 'null.null', InitialPage, NoCursorID);
  end;
end;

class function tgoCursorhelper.ToBsonArray(const DocArray: TArray<tgoBsonDocument>): TgoBsonArray;
var
  I: Integer;
begin
  { TODO : UNTESTED }
  Result := TgoBsonArray.Create(length(DocArray));
  for I := 0 to high(DocArray) do
    Result.add((DocArray[I]));
end;

class function tgoCursorhelper.ToBsonArray(const aCursor: igoMongoCursor): TgoBsonArray;
begin
  Result := ToBsonArray(ToDocArray(aCursor));
end;

{Fully exhausts a cursor and puts all elements into an array of docs}
class function tgoCursorhelper.ToDocArray(const aCursor: igoMongoCursor): TArray<tgoBsonDocument>;
begin
  SetLength(Result, 0);
  if assigned(aCursor) then
    Result := aCursor.ToArray;
end;

{Returns the first doc from an array. If the array is empty, it returns a NIL document}
class function tgoCursorhelper.FirstDoc(const Docs: TArray<tgoBsonDocument>): tgoBsonDocument;
begin
  if length(Docs) > 0 then
    Result := Docs[0]
  else
    Result.SetNil;
end;

{$ENDREGION}
//
{$REGION 'tgoMongoCursor (contains a tgoMongoProtocol reference to fetch new records)'}

{ TgoMongoCursor }

constructor TgoMongoCursor.Create(const AProtocol: tgoMongoProtocol; AReadPreference: tgoMongoReadPreference; const ADatabaseName,
  ACollectionName: string; const AInitialPage: TArray<TBytes>; const AInitialCursorId: Int64);
begin
  inherited Create;
  FProtocol := AProtocol;
  FDatabaseName := ADatabaseName;
  FCollectionName := ACollectionName;
  FInitialPage := AInitialPage;
  FInitialCursorId := AInitialCursorId;
  FReadPreference := AReadPreference;
end;

constructor TgoMongoCursor.Create(const AProtocol: tgoMongoProtocol; AReadPreference: tgoMongoReadPreference; const aNameSpace: string;
  const AInitialPage: TArray<TBytes>; const AInitialCursorId: Int64);
var
  dotpos: Integer;
begin
  inherited Create;
  dotpos := Pos('.', aNameSpace);
  FProtocol := AProtocol;
  FDatabaseName := copy(aNameSpace, 1, dotpos - 1);
  FCollectionName := copy(aNameSpace, dotpos + 1, length(aNameSpace));
  FInitialPage := AInitialPage;
  FInitialCursorId := AInitialCursorId;
  FReadPreference := AReadPreference;
end;

function TgoMongoCursor.GetEnumerator: TEnumerator<tgoBsonDocument>;
begin
  Result := TEnumerator.Create(FProtocol, FReadPreference, FDatabaseName, FCollectionName, FInitialPage, FInitialCursorId);
end;

function TgoMongoCursor.ToArray: TArray<tgoBsonDocument>;
var
  Count, Capacity: Integer;
  doc: tgoBsonDocument;
begin
  Count := 0; //element size is sizeof(interface) =8 bytes in 64 bit mode
  Capacity := 16; //256 bytes in 64 bit mode
  SetLength(Result, Capacity);
  for doc in Self do
  begin
    if (Count >= Capacity) then
    begin
      Capacity := Capacity * 2; //there is exponential growth risk here
      SetLength(Result, Capacity);
    end;
    Result[Count] := doc;
    Inc(Count);
  end;

  SetLength(Result, Count); //Truncate
end;

{ TgoMongoCursor.TEnumerator }

procedure TgoMongoCursor.TEnumerator.SpecifyDB(const Writer: IgoBsonWriter);
begin
  Writer.WriteString('$db', FDatabaseName);
end;

procedure TgoMongoCursor.TEnumerator.SpecifyReadPreference(const AWriter: IgoBsonWriter);
begin
  DoSpecifyReadPreference(FReadPreference, AWriter);
end;

constructor TgoMongoCursor.TEnumerator.Create(const AProtocol: tgoMongoProtocol; AReadPreference: tgoMongoReadPreference; const
  ADatabaseName, ACollectionName: string; const APage: TArray<TBytes>; const ACursorId: Int64);
begin
  inherited Create;
  FProtocol := AProtocol;
  FDatabaseName := ADatabaseName;
  FCollectionName := ACollectionName;
  FPage := APage;
  FCursorId := ACursorId;
  FReadPreference := AReadPreference;
  FIndex := -1;
end;

destructor TgoMongoCursor.TEnumerator.Destroy;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  if FCursorId <> 0 then // we exited the for...in loop before the cursor was exhausted
  begin
    try
      Writer := TgoBsonWriter.Create;
      Writer.WriteStartDocument;
      Writer.WriteString('killCursors', FCollectionName);
      Writer.WriteStartArray('cursors');
      Writer.WriteInt64(FCursorId);
      Writer.WriteEndArray;
      SpecifyDB(Writer);
      SpecifyReadPreference(Writer);
      Writer.WriteEndDocument;
      Reply := FProtocol.OpMsg(Writer.ToBson, nil, false, FProtocol.ReplyTimeout);
      HandleCommandReply(FProtocol, Reply); //TEST , added 9.1.2025
    except
      // always ignore exceptions in a destructor!
    end;
  end;
  inherited;
end;

function TgoMongoCursor.TEnumerator.DoGetCurrent: tgoBsonDocument;
begin
  Result := tgoBsonDocument.Load(FPage[FIndex]);
end;

function TgoMongoCursor.TEnumerator.DoMoveNext: Boolean;
begin
  Result := (FIndex < (length(FPage) - 1));
  if Result then
    Inc(FIndex)
  else if (FCursorId <> NoCursorID) then
  begin
    { Get next page from server.
      Note: if FCursorId = NoCursorID, then all documents did fit in the reply, so there
      is no need to get more data from the server. }
    GetMore;
    Result := (FPage <> nil);
  end;
end;

procedure TgoMongoCursor.TEnumerator.GetMore;
var
  Reply: IgoMongoReply;
  Writer: IgoBsonWriter;
  ADoc, Cursor: tgoBsonDocument;
  Docs: TgoBsonArray;
  Value: tgoBsonValue;
  I: Integer;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt64('getMore', FCursorId);
  Writer.WriteString('collection', FCollectionName);
  Writer.WriteInt32('batchSize', length(FPage));
  { MaxTimeMS ?}
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil, false, FProtocol.ReplyTimeout);
  HandleCommandReply(FProtocol, Reply);

  FIndex := 0;
  SetLength(FPage, 0);
  ADoc := Reply.FirstDoc;
  if not ADoc.IsNil then
  begin
    if ADoc.Contains('cursor') then
    begin
      Cursor := ADoc['cursor'].asBsonDocument;
      // The cursor ID should become 0 when it is exhausted
      FCursorId := Cursor['id']; // less overhead to do it here, than query reply.cursorid
      // Namespace:=Cursor.Get('ns','').ToString();   --> does not change
      Docs := Cursor['nextBatch'].AsBsonArray;
      SetLength(FPage, Docs.Count);
      I := 0;
      for Value in Docs do
      begin
        FPage[I] := Value.asBsonDocument.ToBson;
        Inc(I);
      end;
    end;
  end;
end;

{$ENDREGION}

{$REGION 'tgoMongoClient (owns the tgoMongoProtocol)'}

class function TgoMongoClientSettings.Create: TgoMongoClientSettings;
begin
  Fillchar(Result, sizeof(Result), 0);
  Result.ConnectionTimeout := 5000;
  Result.ReplyTimeout := 30000;
  Result.QueryFlags := [];
  Result.Secure := false;
  Result.Certificate := nil;
  Result.PrivateKey := nil;
  Result.PrivateKeyPassword := '';
  Result.AuthMechanism := TgoMongoAuthMechanism.None;
  Result.AuthDatabase := '';
  Result.Username := '';
  Result.Password := '';
  Result.GlobalReadPreference := tgoMongoReadPreference.Primary;
  Result.ApplicationName := '';
  Result.UseSnappyCompression := True; // snappy has priority over zlib if both are set
  Result.UseZlibCompression := false;
end;

function TgoMongoClient.EmptyCursor: igoMongoCursor;
var
  doc: tgoBsonDocument;
begin
  doc.SetNil;
  with tgoCursorhelper do
    Result := CreateCursor(doc, Protocol, Protocol.GlobalReadPreference);
end;

constructor TgoMongoClient.Create(const AHost: string; const APort: Integer);
begin
  Create(AHost, APort, TgoMongoClientSettings.Create);
end;

constructor TgoMongoClient.Create(const AHost: string; const APort: Integer; const ASettings: TgoMongoClientSettings);
var
  ProtocolSettings: TgoMongoProtocolSettings;
begin
  inherited Create;

  ProtocolSettings.GlobalReadPreference := ASettings.GlobalReadPreference;
  if ProtocolSettings.GlobalReadPreference = tgoMongoReadPreference.fromParent then
    ProtocolSettings.GlobalReadPreference := tgoMongoReadPreference.Primary;
  ProtocolSettings.ConnectionTimeout := ASettings.ConnectionTimeout;
  ProtocolSettings.ReplyTimeout := ASettings.ReplyTimeout;
  ProtocolSettings.QueryFlags := ASettings.QueryFlags;
  ProtocolSettings.Secure := ASettings.Secure;
  ProtocolSettings.Certificate := ASettings.Certificate;
  ProtocolSettings.PrivateKey := ASettings.PrivateKey;
  ProtocolSettings.PrivateKeyPassword := ASettings.PrivateKeyPassword;
  ProtocolSettings.AuthMechanism := ASettings.AuthMechanism;
  ProtocolSettings.AuthDatabase := ASettings.AuthDatabase;
  ProtocolSettings.Username := ASettings.Username;
  ProtocolSettings.Password := ASettings.Password;
  ProtocolSettings.ApplicationName := ASettings.ApplicationName;
  ProtocolSettings.UseSnappyCompression := ASettings.UseSnappyCompression;
  ProtocolSettings.UseZlibCompression := ASettings.UseZlibCompression;
  FProtocol := tgoMongoProtocol.Create(AHost, APort, ProtocolSettings);
  CreateGuid(flsID);
  fInTransaction := false;
  fTransactionNumber := 0;
end;

constructor TgoMongoClient.Create(const ASettings: TgoMongoClientSettings);
begin
  Create(DEFAULT_HOST, DEFAULT_PORT, ASettings);
end;

destructor TgoMongoClient.Destroy;
begin
  {The client OWNS the protocol. The client is CONTAINED as a reference in tgoMongoDatabase, }
  FProtocol.Free;
  inherited;
end;

procedure TgoMongoClient.DropDatabase(const AName: string);
// https://docs.mongodb.com/manual/reference/command/dropDatabase/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('dropDatabase', 1);
  Writer.WriteString('$db', AName);
  { TODO : Readpreference??? }
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  HandleCommandReply(FProtocol, Reply);
end;

function TgoMongoClient.GetDatabase(const AName: string): IgoMongoDatabase;
begin
  Result := TgoMongoDatabase.Create(Self, AName);
end;

function TgoMongoClient.ListDatabaseNames: TArray<string>;
var
  Docs: TArray<tgoBsonDocument>;
  I: Integer;
begin
  Docs := ListDatabases;
  SetLength(Result, length(Docs));
  for I := 0 to length(Docs) - 1 do
    Result[I] := Docs[I]['name'];
end;

function TgoMongoClient.ListDatabases: TArray<tgoBsonDocument>;
// https://docs.mongodb.com/manual/reference/command/listDatabases/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  doc: tgoBsonDocument;
  Databases: TgoBsonArray;
  Value: tgoBsonValue;
  I: Integer;
begin
  Result := nil;
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('listDatabases', 1);
  Writer.WriteString('$db', DB_ADMIN);
  { TODO : Readpreference??? }
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  HandleCommandReply(FProtocol, Reply);
  doc := Reply.FirstDoc;
  if not doc.IsNil then
  begin
    if (doc.TryGetValue('databases', Value)) then
    begin
      Databases := Value.AsBsonArray;
      SetLength(Result, Databases.Count);
      for I := 0 to Databases.Count - 1 do
        Result[I] := Databases[I].asBsonDocument;
    end;
  end;
end;

function TgoMongoClient.GetInstanceInfo(const ASaslSupportedMechs: string = ''; const AComment: string = ''): TgoMongoInstanceInfo;
// https://docs.mongodb.com/manual/reference/command/isMaster/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  doc: tgoBsonDocument;
  InstArray: TgoBsonArray;
  I: Integer;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('isMaster', 1);
  Writer.WriteString('$db', DB_ADMIN);
  if (length(ASaslSupportedMechs) > 0) then
  begin
    Writer.WriteString('saslSupportedMechs', ASaslSupportedMechs);
    if (length(AComment) > 0) then
      Writer.WriteString('Comment', AComment);
  end;
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  HandleCommandReply(FProtocol, Reply);

  doc := Reply.FirstDoc;
  if not (doc.IsNil) then
  begin
    Result.Primary := TgoMongoInstance.Create(doc.Get('primary', '').ToString);
    Result.Me := TgoMongoInstance.Create(doc.Get('me', '').ToString);
    Result.SetName := doc.Get('setName', '').ToString;
    Result.SetVersion := doc.Get('setVersion', 0).ToInteger;
    Result.IsMaster := doc.Get('ismaster', false).ToBoolean;
    Result.IsSecondary := doc.Get('secondary', false).ToBoolean;
    Result.ArbiterOnly := doc.Get('arbiterOnly', false).ToBoolean;
    Result.LocalTime := doc.Get('localTime', 0).ToUniversalTime;
    Result.ConnectionId := doc.Get('connectionId', 0).ToInteger;
    Result.ReadOnly := doc.Get('readOnly', True).ToBoolean;

    if doc.Contains('hosts') then
    begin
      InstArray := doc.Get('hosts', '').AsBsonArray;
      SetLength(Result.Hosts, InstArray.Count);
      for I := 0 to InstArray.Count - 1 do
        Result.Hosts[I] := TgoMongoInstance.Create(InstArray.Items[I].ToString);
    end
    else
      Result.Hosts := nil;

    if doc.Contains('arbiters') then
    begin
      InstArray := doc.Get('arbiters', '').AsBsonArray;
      SetLength(Result.Arbiters, InstArray.Count);
      for I := 0 to InstArray.Count - 1 do
        Result.Arbiters[I] := TgoMongoInstance.Create(InstArray.Items[I].ToString);
    end
    else
      Result.Arbiters := nil;
  end
  else
    raise Exception.Create('invalid response');
end;

function TgoMongoClient.GetProtocol: tgoMongoProtocol;
begin
  Result := FProtocol;
end;

function TgoMongoClient.IsMaster: Boolean;
begin
  Result := Self.GetInstanceInfo().IsMaster;
end;

{ AdminCommand performs an administrative command and returns ONE document.
  It uses dependency injection by calling an anonymous method that "injects"
  commands into the BSON document }

function TgoMongoClient.AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  CommandToIssue(Writer); // let the anonymous method write the commands
  Writer.WriteString('$db', DB_ADMIN);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  HandleCommandReply(FProtocol, Reply);
  with tgoCursorhelper do
    Result := CreateCursor(Reply.FirstDoc, FProtocol, FProtocol.GlobalReadPreference);
end;

function TgoMongoClient.BuildInfo: tgoBsonDocument;
var
  doc: tgoBsonDocument;
begin
  Result.SetNil;
  for doc in AdminCommand(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteInt32('buildInfo', 1);
    end) do
    Result := doc;
end;

function TgoMongoClient.Features: tgoBsonDocument;
var
  doc: tgoBsonDocument;
begin
  Result.SetNil;
  for doc in AdminCommand(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteInt32('features', 1);
    end) do
    Result := doc;
end;

function TgoMongoClient.getAvailable: Boolean;
begin
  Result := fAvailable;
end;

function TgoMongoClient.getConnected: Boolean;
begin
  Result := FProtocol.Connected;
end;

function TgoMongoClient.GetGlobalReadPreference: tgoMongoReadPreference;
begin
  Result := FProtocol.GlobalReadPreference;
end;

function TgoMongoClient.getPooled: Boolean;
begin
  Result := fPooled;
end;

function TgoMongoClient.HostInfo: tgoBsonDocument;
var
  doc: tgoBsonDocument;
begin
  Result.SetNil;
  for doc in AdminCommand(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteInt32('hostInfo', 1);
    end) do
    Result := doc;
end;

function TgoMongoClient.LogRotate: Boolean;
var
  doc: tgoBsonDocument;
begin
  Result := false;
  for doc in AdminCommand(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteInt32('logRotate', 1);
    end) do
    if not doc.IsNil then
      Result := doc['ok']
end;

procedure TgoMongoClient.ReleaseToPool;
begin
  FProtocol.PrepareForReuse;
  fAvailable := True;
end;

procedure TgoMongoClient.setAvailable(const Value: Boolean);
begin
  fAvailable := Value;
end;

procedure TgoMongoClient.setConnected(const Value: Boolean);
begin
  FProtocol.Connected := Value; //may throw exception
end;

procedure TgoMongoClient.SetGlobalReadPreference(const Value: tgoMongoReadPreference);
begin
  FProtocol.GlobalReadPreference := Value;
end;

procedure TgoMongoClient.setPooled(const Value: Boolean);
begin
  fPooled := Value;
end;

// startTransaction initiates a transaction but does not send anything over the wire.
// The real transaction starts when tgoMongoCollection performs a find, insert, update
// or delete.

function TgoMongoClient.SupportsTransactions: Boolean;
begin
  Result := Protocol.SupportsTransactions; //this query also auto reconnects
end;

procedure TgoMongoClient.UnpinTransaction;
begin
  try
    if fInTransaction then
    begin
      if fTransactionActivated then
      begin
        AdminCommand(procedure(Writer: IgoBsonWriter)
          begin
            Writer.WriteInt64('abortTransaction', 1); //VERB - command. camel case
            _EmitTransactionMetadata(Writer);
          end)
      end;
    end;
  except
    // ignore exceptions
  end;
  fInTransaction := false;
  fTransactionActivated := false;
end;

procedure TgoMongoClient.StartTransaction;
begin
  if fInTransaction then
    raise Exception.Create('Already in a transaction');

  if SupportsTransactions then
  begin
    fInTransaction := True;
    fTransactionActivated := false;
    Inc(fTransactionNumber);
  end
  else
    raise Exception.Create('Server does not support transactions, it is not member of a replica set.');
end;

{AbortTransaction is called by the user
commitTransaction and abortTransaction must be run on the "admin" database,
even if the transaction�s operations targeted a different database.
See https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.md}

procedure TgoMongoClient.AbortTransaction;
begin
  try
    if fInTransaction then
    begin
      if fTransactionActivated then
        AdminCommand(procedure(Writer: IgoBsonWriter)
          begin
            Writer.WriteInt64('abortTransaction', 1); //VERB - command. camel case
            _EmitTransactionMetadata(Writer);
          end);
    end
    else
      raise Exception.Create('Not in a transaction');
  finally
    fInTransaction := false;
    fTransactionActivated := false;
  end;
end;

{CommitTransaction is called by the user
 commitTransaction and abortTransaction must be run on the "admin" database,
 even if the transaction�s operations targeted a different database.
 see https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.md}

procedure TgoMongoClient.CommitTransaction;
begin
  try
    if fInTransaction then
    begin
      if fTransactionActivated then
        AdminCommand(procedure(Writer: IgoBsonWriter)
          begin
            Writer.WriteInt64('commitTransaction', 1); //VERB - command. camel case
            _EmitTransactionMetadata(Writer);
          end);
    end
    else
      raise Exception.Create('Not in a transaction');
  finally
    fInTransaction := false;
    fTransactionActivated := false;
  end;
end;

// "IncludeTransactionDetails" is called by tgoMongoCollection.find, insert, update or delete.
// it physically starts the transaction *just once *.

procedure TgoMongoClient.IncludeTransactionDetails(const Writer: IgoBsonWriter);
var
  doc: tgoBsonDocument;
begin
  if fInTransaction then
  begin
    if not fTransactionActivated then
    begin
      fTransactionActivated := True;
      Writer.WriteBoolean('startTransaction', True); //NOT A VERB but a parameter, therefore true and not 1
    end;
    _EmitTransactionMetadata(Writer);
  end;
end;

procedure TgoMongoClient._EmitTransactionMetadata(const Writer: IgoBsonWriter);
var
  doc: tgoBsonDocument;
begin
  Writer.WriteInt64('txnNumber', fTransactionNumber); //camel case
  doc := tgoBsonDocument.Create;
  doc['id'] := flsID; //Session ID
  Writer.writename('lsid');
  Writer.WriteValue(doc);
  Writer.WriteBoolean('autocommit', false); //lower case...
end;

{$ENDREGION}
//
{$REGION 'TgoMongoDatabase (contains an igoMongoClient reference)'}

procedure TgoMongoDatabase.SpecifyDB(const AWriter: IgoBsonWriter);
begin
  AWriter.WriteString('$db', name);
end;

procedure TgoMongoDatabase.SpecifyReadPreference(const AWriter: IgoBsonWriter);
begin
  DoSpecifyReadPreference(GetReadPreference, AWriter);
end;

constructor TgoMongoDatabase.Create(const AClient: TgoMongoClient; const AName: string);
begin
  Assert(AClient <> nil);
  Assert(AName <> '');
  inherited Create;
  FClient := AClient;
  FName := AName;
  FReadPreference := tgoMongoReadPreference.fromParent;

end;

function TgoMongoDatabase.AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;
begin
  Result := FClient.AdminCommand(CommandToIssue);
end;

function TgoMongoDatabase.Command(CommandToIssue: tWriteCmd): igoMongoCursor;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  CommandToIssue(Writer); // let the anonymous method write the commands
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  HandleCommandReply(Protocol, Reply);
  with tgoCursorhelper do
    Result := CreateCursor(Reply.FirstDoc, Protocol, GetReadPreference);
end;

procedure TgoMongoDatabase.DropCollection(const AName: string);
// https://docs.mongodb.com/manual/reference/command/drop/#dbcmd.drop
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('drop', AName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  HandleCommandReply(Protocol, Reply, TgoMongoErrorCode.NamespaceNotFound);
end;

function TgoMongoDatabase.GetCollection(const AName: string): IgoMongoCollection;
begin
  Result := TgoMongoCollection.Create(Self, AName);
end;

function TgoMongoDatabase.GetDbStats(const AScale: Integer): TgoMongoStatistics;
// https://docs.mongodb.com/manual/reference/command/dbStats/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  doc: tgoBsonDocument;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('dbStats', 1);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteInt32('scale', AScale);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  HandleCommandReply(Protocol, Reply);
  doc := Reply.FirstDoc;
  if doc.IsNil then
    raise EgoMongoDBError.Create(RS_MONGODB_GENERIC_ERROR);

  Result.Database := doc.Get('db', '').ToString;
  Result.Collections := doc.Get('collections', 0).ToInteger;
  Result.Views := doc.Get('views', 0).ToInteger;
  Result.Objects := doc.Get('objects', 0).ToInt64;
  Result.AvgObjSize := doc.Get('avgObjSize', 0).toDouble;
  Result.DataSize := doc.Get('dataSize', 0).toDouble;
  Result.StorageSize := doc.Get('storageSize', 0).toDouble;
  Result.NumExtents := doc.Get('numExtents', 0).ToInteger;
  Result.Indexes := doc.Get('indexes', 0).ToInteger;
  Result.IndexSize := doc.Get('indexSize', 0).toDouble;
  Result.ScaleFactor := doc.Get('scaleFactor', 0).toDouble;
  Result.FsUsedSize := doc.Get('fsUsedSize', 0).toDouble;
  Result.FsTotalSize := doc.Get('fsTotalSize', 0).toDouble;
end;

function TgoMongoDatabase.GetProtocol: tgoMongoProtocol;
begin
  Result := FClient.Protocol;
end;

function TgoMongoDatabase.CreateCollection(const AName: string; const ACapped: Boolean; const AMaxSize, AMaxDocuments: Int64; const
  AValidationLevel: TgoMongoValidationLevel; const AValidationAction: TgoMongoValidationAction; const AValidator: tgoBsonDocument; const
  ACollation: TgoMongoCollation): Boolean;
// https://docs.mongodb.com/manual/reference/method/db.createCollection/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('create', AName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);

  Writer.WriteBoolean('capped', ACapped);
  if ACapped = True then
  begin
    Writer.WriteInt64('size', AMaxSize);
    Writer.WriteInt64('max', AMaxDocuments);
  end;
  // timeSeries...

  if AValidator.IsNil = false then
  begin
    Writer.writename('validator');
    Writer.WriteRawBsonDocument(AValidator.ToBson);
    Writer.WriteString('validationLevel', AValidationLevel.ToString);
    Writer.WriteString('validationAction', AValidationAction.ToString);
  end;

  Writer.writename('collation');
  Writer.WriteStartDocument;
  Writer.WriteString('locale', ACollation.Locale);
  Writer.WriteBoolean('caseLevel', ACollation.CaseLevel);
  Writer.WriteString('caseFirst', ACollation.CaseFirst.ToString);
  Writer.WriteInt32('strength', ACollation.Strength);
  Writer.WriteBoolean('numericOrdering', ACollation.NumericOrdering);
  Writer.WriteString('alternate', ACollation.Alternate.ToString);
  Writer.WriteString('maxVariable', ACollation.MaxVariable.ToString);
  Writer.WriteBoolean('backwards', ACollation.Backwards);
  Writer.WriteEndDocument;
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  Result := (HandleCommandReply(Protocol, Reply) = 0);
end;

function TgoMongoDatabase.GetReadPreference: tgoMongoReadPreference;
begin
  Result := FReadPreference;
  if Result = tgoMongoReadPreference.fromParent then
    Result := Protocol.GlobalReadPreference;
end;

function TgoMongoDatabase.ListCollectionNames: TArray<string>;
var
  Docs: TArray<tgoBsonDocument>;
  I: Integer;
begin
  Docs := ListCollections;
  SetLength(Result, length(Docs));
  for I := 0 to length(Docs) - 1 do
    Result[I] := Docs[I]['name'];
end;

function TgoMongoDatabase.ListCollections: TArray<tgoBsonDocument>;
// https://docs.mongodb.com/manual/reference/command/listCollections/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Result := nil;
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('listCollections', 1);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);

  FClient.IncludeTransactionDetails(Writer);

  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  HandleCommandReply(Protocol, Reply);
  with tgoCursorhelper do
    Result := ToDocArray(CreateCursor(Reply.FirstDoc, Protocol, GetReadPreference));
end;

function TgoMongoDatabase.RenameCollection(const AFromNamespace, AToNamespace: string; const ADropTarget: Boolean): Boolean;
// https://docs.mongodb.com/manual/reference/command/renameCollection/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('renameCollection', AFromNamespace);
  Writer.WriteString('to', AToNamespace);
  Writer.WriteBoolean('dropTarget', ADropTarget);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  Result := (HandleCommandReply(Protocol, Reply) = 0);
end;

procedure TgoMongoDatabase.SetReadPreference(const Value: tgoMongoReadPreference);
begin
  FReadPreference := Value;
end;

function TgoMongoDatabase._GetClient: IgoMongoClient;
begin
  Result := FClient;
end;

function TgoMongoDatabase._GetName: string;
begin
  Result := FName;
end;

{$ENDREGION}
//
{$REGION 'tgoMongoCollection (contains an igoMongoDatabase reference)'}

function TgoMongoCollection.EmptyCursor: igoMongoCursor;
var
  doc: tgoBsonDocument;
begin
  doc.SetNil;
  with tgoCursorhelper do
    Result := CreateCursor(doc, Protocol, Protocol.GlobalReadPreference);
end;

procedure TgoMongoCollection.SetReadPreference(const Value: tgoMongoReadPreference);
begin
  FReadPreference := Value;
end;

procedure TgoMongoCollection.SpecifyDB(const AWriter: IgoBsonWriter);
begin
  AWriter.WriteString('$db', FDatabase.name);
end;

procedure TgoMongoCollection.SpecifyReadPreference(const AWriter: IgoBsonWriter);
begin
  DoSpecifyReadPreference(GetReadPreference, AWriter);
end;

(*AddWriteConcern (not implemented yet)

 when performing operations that perform writes in the database, you
 can specify

 - IF you want a reply for confirmation (the default is true).
   Omitting the confirmation would save latency time if you perform
   many individual writes.

 - Optionally, a timeout for the confirmation. (Default is infinity)
   If the server is unable to confirm the operation within this
   client-specified timeout, the server will send a writeConcernError
   error message back to the client.

   If you specify a long timeout X for a lengthy write/update operation,
   the OPMSG call should wait long enough, e.g. (X + 5000 ms)
   and not give up sooner.
*)

procedure TgoMongoCollection.AddWriteConcern(const AWriter: IgoBsonWriter {TODO: Parameterlist});
begin
  (* TODO : Implement Writeconcern, something like

   var doc:tgoBsonDocument;
   doc:=tgobsondocument.create;
   doc['w']:=1;                                //Default
   doc['wtimeout']:= protocol.replytimeout;    //Optional
   aWriter.WriteName('writeConcern');
   aWriter.WriteRawBsonDocument(doc.ToBson);
   *)
end;

function TgoMongoCollection.Count: Integer;
begin
  Result := Count(tgoMongoFilter.Empty);
end;

function TgoMongoCollection.Count(const aFilter: tgoMongoFilter): Integer;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('count', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.writename('query');
  Writer.WriteRawBsonDocument(aFilter.ToBson);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  Result := HandleCommandReply(Protocol, Reply);
end;

function TgoMongoCollection.EstimatedDocumentCount: Integer;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  doc: tgoBsonDocument;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('count', FName);
  SpecifyDB(Writer);

  doc := tgoBsonDocument.Create;
  Writer.writename('query');
  Writer.WriteRawBsonDocument(doc.ToBson); //empty query document

  Writer.WriteInt32('limit', 0);
  Writer.WriteInt32('skip', 0);

  doc['level'] := 'local';
  Writer.writename('readConcern');
  Writer.WriteRawBsonDocument(doc.ToBson);

  Writer.WriteEndDocument;

  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  Result := HandleCommandReply(Protocol, Reply);
end;

{

  TODO: FindAndModify
  https://www.mongodb.com/docs/manual/reference/command/findAndModify/#mongodb-dbcommand-dbcmd.findAndModify

}

function TgoMongoCollection.Aggregate(APipeLine: igoAggregationPipeline): igoMongoCursor;
begin
  Result := Aggregate(APipeLine.Stages);
end;

function TgoMongoCollection.Aggregate(APipeLine: TgoBsonArray; aBatchsize: Integer = 0; aMaxTimeMS: Integer = 0): igoMongoCursor;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  cursorDoc: tgoBsonDocument;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;

  Writer.WriteString('aggregate', FName); // the collection name
  SpecifyDB(Writer);

  Writer.writename('pipeline');
  Writer.WriteValue(APipeLine);

  Writer.writename('cursor');
  cursorDoc := tgoBsonDocument.Create;
  if aBatchsize <> 0 then
    cursorDoc['batchSize'] := aBatchsize; // does not seem to work
  Writer.WriteValue(cursorDoc);

  if aMaxTimeMS <> 0 then
  begin
    Writer.writename('maxTimeMS'); //untested
    Writer.WriteValue(aMaxTimeMS);
  end;

  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;

  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, max(Protocol.ReplyTimeout, aMaxTimeMS));
  HandleCommandReply(Protocol, Reply);
  Result := tgoCursorhelper.CreateCursor(Reply.FirstDoc, Protocol, GetReadPreference);
end;

constructor TgoMongoCollection.Create(const ADatabase: TgoMongoDatabase; const AName: string);
begin
  Assert(assigned(ADatabase));
  Assert(AName <> '');
  inherited Create;
  FDatabase := ADatabase;
  FName := AName;
  FReadPreference := tgoMongoReadPreference.fromParent;
  Assert(assigned(Protocol));
end;

function TgoMongoCollection.CreateIndex(const AName: string; const AKeyFields: array of string; const AUnique: Boolean = false): Boolean;
// https://docs.mongodb.com/manual/reference/command/createIndexes/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  I: Integer;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('createIndexes', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);

  Writer.WriteStartArray('indexes');
  Writer.WriteStartDocument;
  Writer.WriteStartDocument('key');
  for I := 0 to high(AKeyFields) do
    Writer.WriteInt32(AKeyFields[I], 1);
  Writer.WriteEndDocument;
  Writer.WriteString('name', AName);
  Writer.WriteBoolean('unique', AUnique);
  Writer.WriteEndDocument;
  Writer.WriteEndArray;
  AddWriteConcern(Writer {TODO: Parameterlist});
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  Result := (HandleCommandReply(Protocol, Reply) = 0);
end;

function TgoMongoCollection.CreateTextIndex(const AName: string; const aFields: array of string; const ALanguageOverwriteField: string =
  '';
  const ADefaultLanguage: string = 'en'): Boolean;
// https://docs.mongodb.com/manual/core/index-text/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  I: Integer;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('createIndexes', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteStartArray('indexes');
  Writer.WriteStartDocument;
  Writer.WriteStartDocument('key');
  for I := 0 to high(aFields) do
    Writer.WriteString(aFields[I], 'text');
  Writer.WriteEndDocument;
  Writer.WriteString('name', AName);

  if ADefaultLanguage.IsEmpty = false then
    Writer.WriteString('default_language', ADefaultLanguage);

  if ALanguageOverwriteField.IsEmpty = false then
    Writer.WriteString('language_override', ALanguageOverwriteField);
  Writer.WriteEndDocument;
  Writer.WriteEndArray;
  AddWriteConcern(Writer {TODO: Parameterlist});
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  Result := (HandleCommandReply(Protocol, Reply) = 0);
end;

function TgoMongoCollection.DropIndex(const AName: string): Boolean;
// https://docs.mongodb.com/manual/reference/command/dropIndexes/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('dropIndexes', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteString('index', AName);
  AddWriteConcern(Writer {TODO: Parameterlist});
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  Result := (HandleCommandReply(Protocol, Reply) = 0);
end;

function TgoMongoCollection.ListIndexNames: TArray<string>;
// https://docs.mongodb.com/manual/reference/command/listIndexes/
var
  Docs: TArray<tgoBsonDocument>;
  I: Integer;
begin
  Docs := ListIndexes;
  SetLength(Result, length(Docs));
  for I := 0 to length(Docs) - 1 do
    Result[I] := Docs[I]['name'];
end;

function TgoMongoCollection.Stats: tgoBsonDocument;
// https://www.mongodb.com/docs/manual/reference/command/collStats/
var
  doc: tgoBsonDocument;
begin
  doc.SetNil;
  for doc in FDatabase.Command(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteString('collStats', FName);
      FDatabase.Client.IncludeTransactionDetails(Writer);
    end) do
    ;
  Result := doc;
end;

function TgoMongoCollection.ListIndexes: TArray<tgoBsonDocument>;
// https://docs.mongodb.com/manual/reference/command/listIndexes/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Result := nil;
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('listIndexes', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  FDatabase.Client.IncludeTransactionDetails(Writer);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  HandleCommandReply(Protocol, Reply);
  with tgoCursorhelper do
    Result := ToDocArray(CreateCursor(Reply.FirstDoc, Protocol, GetReadPreference));
end;

function TgoMongoCollection.Delete(const aFilter: tgoMongoFilter; const AOrdered: Boolean; const ALimit: Integer): Integer;
// https://docs.mongodb.com/manual/reference/command/delete/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('delete', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);

  FDatabase.Client.IncludeTransactionDetails(Writer);

  Writer.WriteStartArray('deletes');
  Writer.WriteStartDocument;
  Writer.writename('q');
  Writer.WriteRawBsonDocument(aFilter.ToBson);
  Writer.WriteInt32('limit', ALimit);

  Writer.WriteEndDocument;
  Writer.WriteEndArray;
  AddWriteConcern(Writer {TODO: Parameterlist});
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  Result := HandleCommandReply(Protocol, Reply);
end;

function TgoMongoCollection.DeleteMany(const aFilter: tgoMongoFilter; const AOrdered: Boolean): Integer;
begin
  Result := Delete(aFilter, AOrdered, 0);
end;

function TgoMongoCollection.DeleteOne(const aFilter: tgoMongoFilter): Boolean;
begin
  Result := (Delete(aFilter, True, 1) = 1);
end;

function TgoMongoCollection.Find: igoMongoCursor;
begin
  Result := Find(FindOptions);
end;

function TgoMongoCollection.Find(const aFilter: tgoMongoFilter): igoMongoCursor;
begin
  Result := Find(FindOptions.filter(aFilter));
end;

function TgoMongoCollection.Find(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection): igoMongoCursor;
begin
  Result := Find(FindOptions.filter(aFilter).projection(aProjection));
end;

function TgoMongoCollection.Find(const aProjection: TgoMongoProjection): igoMongoCursor;
begin
  Result := Find(FindOptions.projection(aProjection));
end;

function TgoMongoCollection.Find(const aFilter: tgoMongoFilter; const aSort: TgoMongoSort): igoMongoCursor;
begin
  Result := Find(FindOptions.filter(aFilter).sort(aSort));
end;

// https://docs.mongodb.com/manual/reference/method/db.collection.find
function TgoMongoCollection.Find(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection; const aSort: TgoMongoSort; aSkip:
  Integer = 0): igoMongoCursor;
begin
  Result := Find(FindOptions.filter(aFilter).projection(aProjection).sort(aSort).skip(aSkip));
end;

function TgoMongoCollection.Find(AOptions: igoMongoFindOptions): igoMongoCursor;
var
  Reply: IgoMongoReply;
  Writer: IgoBsonWriter;
begin
  Result := nil;
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('find', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  AOptions.WriteOptions(Writer);

  FDatabase.Client.IncludeTransactionDetails(Writer);

  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  HandleCommandReply(Protocol, Reply);
  with tgoCursorhelper do
    Result := CreateCursor(Reply.FirstDoc, Protocol, GetReadPreference);
end;

function TgoMongoCollection.FindOne(AOptions: igoMongoFindOptions): tgoBsonDocument;
begin
  with tgoCursorhelper do
    Result := FirstDoc(ToDocArray(Find(AOptions.singleBatch(True).limit(1))));
end;

function TgoMongoCollection.FindOne(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection; const aSort: TgoMongoSort):
  tgoBsonDocument;
begin
  Result := FindOne(FindOptions.filter(aFilter).projection(aProjection).sort(aSort));
end;

function TgoMongoCollection.FindOne(const aFilter: tgoMongoFilter; const aProjection: TgoMongoProjection): tgoBsonDocument;
begin
  Result := FindOne(FindOptions.filter(aFilter).projection(aProjection));
end;

function TgoMongoCollection.FindOne(const aFilter: tgoMongoFilter): tgoBsonDocument;
begin
  Result := FindOne(FindOptions.filter(aFilter));
end;

function TgoMongoCollection.FindOne(const aFilter: tgoMongoFilter; const aSort: TgoMongoSort): tgoBsonDocument;
begin
  Result := FindOne(FindOptions.filter(aFilter).sort(aSort));
end;

function TgoMongoCollection.InsertMany(const ADocuments: array of tgoBsonDocument; const AOrdered: Boolean): Integer;
begin
  if (length(ADocuments) > 0) then
    Result := InsertMany(@ADocuments[0], length(ADocuments), AOrdered)
  else
    Result := 0;
end;

function TgoMongoCollection.InsertMany(const ADocuments: TArray<tgoBsonDocument>; const AOrdered: Boolean): Integer;
begin
  if (length(ADocuments) > 0) then
    Result := InsertMany(@ADocuments[0], length(ADocuments), AOrdered)
  else
    Result := 0;
end;

function TgoMongoCollection.GetProtocol: tgoMongoProtocol;
begin
  Result := FDatabase.Protocol;
end;

function TgoMongoCollection.GetReadPreference: tgoMongoReadPreference;
begin
  Result := FReadPreference;
  if Result = tgoMongoReadPreference.fromParent then
    Result := FDatabase.ReadPreference;
end;

function TgoMongoCollection.InsertMany(const ADocuments: PgoBsonDocument; const ACount: Integer; const AOrdered: Boolean): Integer;
// https://docs.mongodb.com/manual/reference/command/insert/#dbcmd.insert
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  I, Remaining, ItemsInBatch, Index, BytesEncoded: Integer;
  tb: TBytes;
  Payload0: TBytes;
  Payload1: TArray<tgoPayloadType1>;
begin
  Remaining := ACount;
  Index := 0;
  Result := 0;
  while (Remaining > 0) do
  begin
    ItemsInBatch := min(Remaining, Protocol.MaxWriteBatchSize);
    SetLength(Payload1, 0);
    Writer := TgoBsonWriter.Create;
    Writer.WriteStartDocument;
    Writer.WriteString('insert', FName);
    SpecifyDB(Writer);
    SpecifyReadPreference(Writer);

    (* DEPRECATED
      {This is the SLOW/LEGACY method because the server needs to unpack
      an array contained inside a very large document.
      It is faster to "outsource" the "documents" array into a separate
      sequence of Payload type 1}
      Writer.WriteStartArray('documents');
      for I := 0 to ItemsInBatch - 1 do
      begin
      Writer.WriteValue(ADocuments[index]);
      Inc(index);
      Dec(Remaining);
      end;
      Writer.WriteEndArray;
 *)

    Writer.WriteBoolean('ordered', AOrdered);
    AddWriteConcern(Writer {TODO: Parameterlist});
    FDatabase.Client.IncludeTransactionDetails(Writer);
    Writer.WriteEndDocument;

    Payload0 := Writer.ToBson;
    BytesEncoded := length(Payload0) + 100; // overly generous estimation
    Writer := nil;

    { https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#command-arguments-as-payload
      "Bulk writes SHOULD use Payload Type 1, and MUST do so when the batch contains more than one entry."
      N.B.: This method is faster because the server can read the "documents" parameter as a
      simple sequential stream of small documents. }

    SetLength(Payload1, 1); // Send ONE sequence of Payload1 with multiple docs
    Payload1[0].name := 'documents';
    SetLength(Payload1[0].Docs, ItemsInBatch);
    for I := 0 to ItemsInBatch - 1 do
    begin
      tb := ADocuments[Index].ToBson;
      { Avoid excessive message size or batch count }
      if ((BytesEncoded + length(tb)) > Protocol.MaxMessageSizeBytes) then
      begin
        SetLength(Payload1[0].Docs, I);
        Break;
      end;
      Inc(BytesEncoded, length(tb));
      Payload1[0].Docs[I] := tb;
      Inc(Index);
      dec(Remaining);
    end; // FOR

    Reply := Protocol.OpMsg(Payload0, Payload1, false, Protocol.ReplyTimeout);
    Inc(Result, HandleCommandReply(Protocol, Reply));
  end; // While
  Assert(Index = ACount);
end;

function TgoMongoCollection.InsertMany(const ADocuments: TEnumerable<tgoBsonDocument>; const AOrdered: Boolean): Integer;
begin
  Result := InsertMany(ADocuments.ToArray, AOrdered);
end;

function TgoMongoCollection.InsertOne(const ADocument: tgoBsonDocument): Boolean;
// https://docs.mongodb.com/manual/reference/command/insert/#dbcmd.insert
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('insert', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteStartArray('documents');
  Writer.WriteValue(ADocument);
  Writer.WriteEndArray;
  AddWriteConcern(Writer {TODO: Parameterlist});
  FDatabase.Client.IncludeTransactionDetails(Writer);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  Result := (HandleCommandReply(Protocol, Reply) = 1);
end;

function TgoMongoCollection.Update(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert, AOrdered, AMulti: Boolean;
  out Upserted: Boolean):
  Integer;
// https://docs.mongodb.com/manual/reference/command/update
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Upserted := false;
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('update', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteStartArray('updates');
  Writer.WriteStartDocument;
  Writer.writename('q');
  Writer.WriteRawBsonDocument(aFilter.ToBson);
  Writer.writename('u');
  Writer.WriteRawBsonDocument(AUpdate.ToBson);
  Writer.WriteBoolean('upsert', AUpsert);
  Writer.WriteBoolean('multi', AMulti);
  Writer.WriteEndDocument;
  Writer.WriteEndArray;
  Writer.WriteBoolean('ordered', AOrdered);
  AddWriteConcern(Writer {TODO: Parameterlist});
  FDatabase.Client.IncludeTransactionDetails(Writer);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil, false, Protocol.ReplyTimeout);
  Result := HandleCommandReply(Protocol, Reply);

  if AUpsert then
  begin
    if not Reply.FirstDoc.IsNil then
      Upserted := Reply.FirstDoc.Contains('upserted');
  end;

end;

function TgoMongoCollection.UpdateMany(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert, AOrdered: Boolean):
  Integer;
var
  Upserted: Boolean;
begin
  Result := Update(aFilter, AUpdate, AUpsert, AOrdered, True, Upserted);
end;

function TgoMongoCollection.UpdateMany(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; AUpsert, AOrdered: Boolean; out
  AUpserted: Boolean): Integer;
begin
  Result := Update(aFilter, AUpdate, AUpsert, AOrdered, True, AUpserted);
end;

function TgoMongoCollection.UpdateOne(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert: Boolean): Boolean;
var
  Upserted: Boolean;
begin
  Result := (Update(aFilter, AUpdate, AUpsert, false, false, Upserted) = 1);
end;

function TgoMongoCollection.UpdateOne(const aFilter: tgoMongoFilter; const AUpdate: TgoMongoUpdate; AUpsert: Boolean; out AUpserted:
  Boolean): Boolean;
begin
  Result := (Update(aFilter, AUpdate, AUpsert, false, false, AUpserted) = 1);
end;

function TgoMongoCollection._GetDatabase: IgoMongoDatabase;
begin
  Result := FDatabase;
end;

function TgoMongoCollection._GetName: string;
begin
  Result := FName;
end;

{$ENDREGION}
//
{$REGION 'Helpers'}
{ TgoMongoValidationLevelHelper }

function TgoMongoValidationLevelHelper.ToString: string;
begin
  case Self of
    TgoMongoValidationLevel.vlOff:
      Result := 'off';
    TgoMongoValidationLevel.vlStrict:
      Result := 'strict';
    TgoMongoValidationLevel.vlModerate:
      Result := 'moderate';
  else
    raise Exception.Create('invalid type');
  end;
end;

{ TgoMongoValidationActionHelper }

function TgoMongoValidationActionHelper.ToString: string;
begin
  case Self of
    TgoMongoValidationAction.vaError:
      Result := 'error';
    TgoMongoValidationAction.vaWarn:
      Result := 'warn';
  else
    raise Exception.Create('invalid type');
  end;
end;

{ TgoMongoCollationCaseFirstHelper }

function TgoMongoCollationCaseFirstHelper.ToString: string;
begin
  case Self of
    TgoMongoCollationCaseFirst.ccfUpper:
      Result := 'upper';
    TgoMongoCollationCaseFirst.ccfLower:
      Result := 'lower';
    TgoMongoCollationCaseFirst.ccfOff:
      Result := 'off';
  else
    raise Exception.Create('invalid type');
  end;
end;

{ TgoMongoCollationAlternateHelper }

function TgoMongoCollationAlternateHelper.ToString: string;
begin
  case Self of
    TgoMongoCollationAlternate.caNonIgnorable:
      Result := 'non-ignorable';
    TgoMongoCollationAlternate.caShifted:
      Result := 'shifted';
  else
    raise Exception.Create('invalid type');
  end;
end;

{ TgoMongoCollationMaxVariableHelper }

function TgoMongoCollationMaxVariableHelper.ToString: string;
begin
  case Self of
    TgoMongoCollationMaxVariable.cmvPunct:
      Result := 'punct';
    TgoMongoCollationMaxVariable.cmvSpace:
      Result := 'space';
  else
    raise Exception.Create('invalid type');
  end;
end;

{$ENDREGION}
//
{$REGION 'tgoMongoInstance'}
{ TgoMongoInstance }

constructor TgoMongoInstance.Create(AHost: string; APort: Word);
begin
  Self.Host := AHost;
  Self.Port := APort;
end;

constructor TgoMongoInstance.Create(AInstance: string);
begin
  try
    if AInstance.Contains(':') = True then
    begin
      Self.Host := copy(AInstance, 1, Pos(':', AInstance) - 1).Trim;
      Self.Port := copy(AInstance, Pos(':', AInstance) + 1, AInstance.length).Trim.ToInteger;
    end;
  except
    Self.Host := '';
    Self.Port := 0;
  end;
end;

{$ENDREGION}
//
{$REGION 'tgoMongoFindOptions'}

type
  { Record that contains most function parameters for collection.Find(),
    see https://www.mongodb.com/docs/manual/reference/command/find/.
    We also implement a fluent interface for this. }

  tgoMongoFindOptionsRec = record
    skip: Integer;
    limit: Integer;
    batchSize: Integer;
    maxTimeMS: Integer;
    singleBatch: Boolean;
    returnKey: Boolean;
    showRecordId: Boolean;
    noCursorTimeout: Boolean;
    allowDiskUse: Boolean;
    allowPartialResults: Boolean;
    filter: tgoBsonDocument;
    sort: tgoBsonDocument;
    min: tgoBsonDocument;
    max: tgoBsonDocument;
    projection: tgoBsonDocument;
    collation: tgoBsonDocument;
    readConcern: tgoBsonDocument;
    hint: string;
    comment: string;
    procedure clear;
    procedure WriteOptions(const Writer: IgoBsonWriter);
    function asBsonDocument: tgoBsonDocument;
    function asJson: string;
    procedure fromBson(aBson: tgoBsonDocument);
    procedure fromJson(const aJson: string);
  end;

  { Class that implements fluent interface igoMongoFindOptions }

  tgoMongoFindOptions = class(TInterfacedObject, igoMongoFindOptions)
  private
    foptions: tgoMongoFindOptionsRec;
    function getbatchSize: Integer;
    function getfilter: tgoMongoFilter;
    function filter(const AValue: tgoMongoFilter): igoMongoFindOptions; overload;
    function filter(const aJsonDoc: string): igoMongoFindOptions; overload;
    function sort(const AValue: TgoMongoSort): igoMongoFindOptions; overload;
    function sort(const aJsonDoc: string): igoMongoFindOptions; overload;
    function projection(const AValue: TgoMongoProjection): igoMongoFindOptions; overload;
    function projection(const aJsonDoc: string): igoMongoFindOptions; overload;
    function hint(AValue: string): igoMongoFindOptions;
    function skip(AValue: Integer): igoMongoFindOptions;
    function limit(AValue: Integer): igoMongoFindOptions;
    function batchSize(AValue: Integer): igoMongoFindOptions;
    function singleBatch(AValue: Boolean): igoMongoFindOptions;
    function comment(const AValue: string): igoMongoFindOptions;
    function maxTimeMS(AValue: Integer): igoMongoFindOptions;
    function readConcern(const AValue: tgoBsonDocument): igoMongoFindOptions; overload;
    function readConcern(const aJsonDoc: string): igoMongoFindOptions; overload;
    function min(const AValue: tgoBsonDocument): igoMongoFindOptions; overload;
    function min(const aJsonDoc: string): igoMongoFindOptions; overload;
    function max(const AValue: tgoBsonDocument): igoMongoFindOptions; overload;
    function max(const aJsonDoc: string): igoMongoFindOptions; overload;
    function returnKey(AValue: Boolean): igoMongoFindOptions;
    function showRecordId(AValue: Boolean): igoMongoFindOptions;
    function noCursorTimeout(AValue: Boolean): igoMongoFindOptions;
    function allowPartialResults(AValue: Boolean): igoMongoFindOptions;
    function collation(const AValue: tgoBsonDocument): igoMongoFindOptions; overload;
    function collation(const aJsonDoc: string): igoMongoFindOptions; overload;
    function allowDiskUse(AValue: Boolean): igoMongoFindOptions;
    procedure WriteOptions(const Writer: IgoBsonWriter);
    function parse(const aJsonDoc: string; var Bson: tgoBsonDocument): igoMongoFindOptions;
    function asBsonDocument: tgoBsonDocument;
    function asJson: string;
    procedure fromBson(aBson: tgoBsonDocument);
    procedure fromJson(const aJson: string);

  public
    constructor Create;
  end;

function tgoMongoFindOptions.allowDiskUse(AValue: Boolean): igoMongoFindOptions;
begin
  foptions.allowDiskUse := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.allowPartialResults(AValue: Boolean): igoMongoFindOptions;
begin
  foptions.allowPartialResults := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.asBsonDocument: tgoBsonDocument;
begin
  Result := foptions.asBsonDocument;
end;

function tgoMongoFindOptions.asJson: string;
begin
  Result := foptions.asJson;
end;

function tgoMongoFindOptions.batchSize(AValue: Integer): igoMongoFindOptions;
begin
  foptions.batchSize := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.collation(const AValue: tgoBsonDocument): igoMongoFindOptions;
begin
  foptions.collation := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.parse(const aJsonDoc: string; var Bson: tgoBsonDocument): igoMongoFindOptions;
begin
  if aJsonDoc = '' then
    Bson.SetNil
  else
    Bson := tgoBsonDocument.parse(aJsonDoc);
  Result := Self;
end;

function tgoMongoFindOptions.collation(const aJsonDoc: string): igoMongoFindOptions;
begin
  Result := parse(aJsonDoc, foptions.collation);
end;

function tgoMongoFindOptions.comment(const AValue: string): igoMongoFindOptions;
begin
  foptions.comment := AValue;
  Result := Self;
end;

constructor tgoMongoFindOptions.Create;
begin
  inherited Create;
  // foptions is a "member" and should already be completely filled with 0. We just make sure ...
  Fillchar(foptions, sizeof(foptions), 0);
  foptions.batchSize := MongoDefBatchSize;
end;

function tgoMongoFindOptions.filter(const aJsonDoc: string): igoMongoFindOptions;
begin
  Result := parse(aJsonDoc, foptions.filter);
end;

procedure tgoMongoFindOptions.fromBson(aBson: tgoBsonDocument);
begin
  foptions.fromBson(aBson);
end;

procedure tgoMongoFindOptions.fromJson(const aJson: string);
begin
  foptions.fromJson(aJson);
end;

function tgoMongoFindOptions.filter(const AValue: tgoMongoFilter): igoMongoFindOptions;
begin
  if not AValue.IsNil then
    foptions.filter := AValue.Render
  else
    foptions.filter.SetNil;
  Result := Self;
end;

function tgoMongoFindOptions.getbatchSize: Integer;
begin
  Result := foptions.batchSize;
end;

function tgoMongoFindOptions.getfilter: tgoMongoFilter;
begin
  Result := foptions.filter;
end;

function tgoMongoFindOptions.hint(AValue: string): igoMongoFindOptions;
begin
  foptions.hint := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.limit(AValue: Integer): igoMongoFindOptions;
begin
  foptions.limit := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.max(const AValue: tgoBsonDocument): igoMongoFindOptions;
begin
  foptions.max := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.max(const aJsonDoc: string): igoMongoFindOptions;
begin
  Result := parse(aJsonDoc, foptions.max);
end;

function tgoMongoFindOptions.maxTimeMS(AValue: Integer): igoMongoFindOptions;
begin
  foptions.maxTimeMS := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.min(const AValue: tgoBsonDocument): igoMongoFindOptions;
begin
  foptions.max := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.min(const aJsonDoc: string): igoMongoFindOptions;
begin
  Result := parse(aJsonDoc, foptions.min);
end;

function tgoMongoFindOptions.noCursorTimeout(AValue: Boolean): igoMongoFindOptions;
begin
  foptions.noCursorTimeout := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.projection(const aJsonDoc: string): igoMongoFindOptions;
begin
  Result := parse(aJsonDoc, foptions.projection);
end;

function tgoMongoFindOptions.projection(const AValue: TgoMongoProjection): igoMongoFindOptions;
begin
  if not AValue.IsNil then
    foptions.projection := AValue.Render
  else
    foptions.projection.SetNil;
  Result := Self;
end;

function tgoMongoFindOptions.readConcern(const AValue: tgoBsonDocument): igoMongoFindOptions;
begin
  foptions.readConcern := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.readConcern(const aJsonDoc: string): igoMongoFindOptions;
begin
  Result := parse(aJsonDoc, foptions.readConcern);
end;

function tgoMongoFindOptions.returnKey(AValue: Boolean): igoMongoFindOptions;
begin
  foptions.returnKey := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.showRecordId(AValue: Boolean): igoMongoFindOptions;
begin
  foptions.showRecordId := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.singleBatch(AValue: Boolean): igoMongoFindOptions;
begin
  foptions.singleBatch := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.skip(AValue: Integer): igoMongoFindOptions;
begin
  foptions.skip := AValue;
  Result := Self;
end;

function tgoMongoFindOptions.sort(const AValue: TgoMongoSort): igoMongoFindOptions;
begin
  if not AValue.IsNil then
    foptions.sort := AValue.Render
  else
    foptions.sort.SetNil;
  Result := Self;
end;

function tgoMongoFindOptions.sort(const aJsonDoc: string): igoMongoFindOptions;
begin
  Result := parse(aJsonDoc, foptions.sort);
end;

procedure tgoMongoFindOptions.WriteOptions(const Writer: IgoBsonWriter);
begin
  foptions.WriteOptions(Writer);
end;

function FindOptions: igoMongoFindOptions;
begin
  Result := tgoMongoFindOptions.Create;
end;

// *********************************

procedure tgoMongoFindOptionsRec.clear;
begin
  finalize(Self); // dispose of existing strings and interfaces  - avoid memory leak
  Fillchar(Self, sizeof(Self), 0);
  batchSize := MongoDefBatchSize;
end;

function tgoMongoFindOptionsRec.asBsonDocument: tgoBsonDocument;
var
  Writer: IgoBsonWriter;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  WriteOptions(Writer);
  Writer.WriteEndDocument;
  Result := tgoBsonDocument.Load(Writer.ToBson);
end;

function tgoMongoFindOptionsRec.asJson: string;
var
  Bson: tgoBsonDocument;
begin
  Bson := asBsonDocument;
  Result := Bson.ToJson;
end;

procedure tgoMongoFindOptionsRec.fromBson(aBson: tgoBsonDocument);
begin
  clear;
  skip := aBson['skip'];
  limit := aBson['limit'];
  maxTimeMS := aBson['maxTimeMS'];
  singleBatch := aBson['singleBatch'];
  returnKey := aBson['returnKey'];
  showRecordId := aBson['showRecordID'];
  noCursorTimeout := aBson['noCursorTimeout'];
  allowDiskUse := aBson['allowDiskUse'];
  allowPartialResults := aBson['allowPartialResults'];

  if aBson.Contains('hint') then
    hint := aBson['hint'];

  if aBson.Contains('comment') then
    comment := aBson['comment'];

  if aBson.Contains('batchSize') then
    batchSize := aBson['batchSize'];
  if aBson.Contains('filter') then
    filter := aBson['filter'].asBsonDocument;
  if aBson.Contains('sort') then
    sort := aBson['sort'].asBsonDocument;
  if aBson.Contains('min') then
    min := aBson['min'].asBsonDocument;
  if aBson.Contains('max') then
    max := aBson['max'].asBsonDocument;
  if aBson.Contains('projection') then
    projection := aBson['projection'].asBsonDocument;
  if aBson.Contains('collation') then
    collation := aBson['collation'].asBsonDocument;
  if aBson.Contains('readConcern') then
    readConcern := aBson['readConcern'].asBsonDocument;
end;

procedure tgoMongoFindOptionsRec.fromJson(const aJson: string);
var
  Bson: tgoBsonDocument;
  reader: igojsonreader;
begin
  reader := tgojsonreader.Create(aJson, false);
  Bson := reader.ReadDocument;
  fromBson(Bson);
end;

procedure tgoMongoFindOptionsRec.WriteOptions(const Writer: IgoBsonWriter);
begin
  if not filter.IsNil then
  begin
    Writer.writename('filter');
    Writer.WriteRawBsonDocument(filter.ToBson);
  end;
  if not sort.IsNil then
  begin
    Writer.writename('sort');
    Writer.WriteRawBsonDocument(sort.ToBson);
  end;
  if not projection.IsNil then
  begin
    Writer.writename('projection');
    Writer.WriteRawBsonDocument(projection.ToBson);
  end;
  if hint <> '' then
    Writer.WriteString('hint', hint);
  if skip > 0 then
    Writer.WriteInt32('skip', skip);
  if limit > 0 then
    Writer.WriteInt32('limit', limit);
  if batchSize > 0 then
    Writer.WriteInt32('batchSize', batchSize);
  if singleBatch then
    Writer.WriteBoolean('singleBatch', singleBatch);
  if comment <> '' then
    Writer.WriteString('comment', comment);
  if maxTimeMS > 0 then
    Writer.WriteInt32('maxTimeMS', maxTimeMS);
  if not readConcern.IsNil then
  begin
    Writer.WriteStartDocument('readConcern');
    Writer.WriteRawBsonDocument(readConcern.ToBson);
    Writer.WriteEndDocument;
  end;
  if not max.IsNil then
  begin
    Writer.WriteStartDocument('max');
    Writer.WriteRawBsonDocument(max.ToBson);
    Writer.WriteEndDocument;
  end;

  if not min.IsNil then
  begin
    Writer.WriteStartDocument('min');
    Writer.WriteRawBsonDocument(min.ToBson);
    Writer.WriteEndDocument;
  end;

  if returnKey then
    Writer.WriteBoolean('returnKey', returnKey);
  if showRecordId then
    Writer.WriteBoolean('showRecordId', showRecordId);

  if allowPartialResults then
    Writer.WriteBoolean('allowPartialResults', allowPartialResults);

  if noCursorTimeout then
    Writer.WriteBoolean('noCursorTimeout', noCursorTimeout);

  if not collation.IsNil then
  begin
    Writer.WriteStartDocument('collation');
    Writer.WriteRawBsonDocument(collation.ToBson);
    Writer.WriteEndDocument;
  end;

  if allowDiskUse then
    Writer.WriteBoolean('allowDiskUse', allowDiskUse);
end;

{$ENDREGION}
//
{$REGION 'tgoConnectionPool - a connection pool for multithreaded applications.'}

constructor tgoConnectionPool.Create(const AHost: string; APort: Integer; const ASettings: TgoMongoClientSettings; aMaxitems: Integer);
begin
  inherited Create;
  flist := tlist<IgoMongoClient>.Create;
  fHost := AHost;
  fPort := APort;
  fConnectionSettings := ASettings;
  flock := tCriticalSection.Create;
  fMaxItems := aMaxitems;
end;

destructor tgoConnectionPool.Destroy;
begin
  flock.Free;
  flist.Free;
  inherited;
end;

{Get an available client connection from the connection pool and make it unavailable}

function tgoConnectionPool.GetAvailableClient: IgoMongoClient;
var
  item: IgoMongoClient;
begin
  Result := nil;
  repeat
    flock.Acquire;
    //Find the first available connection
    for item in flist do
      if (item.Available) then
      begin
        item.Available := false; //mark it "in use" and return the item
        flock.release;
        item.Protocol.PrepareForReuse; //remove any stale replies
        Exit(item);
      end;

    //No free connections available ? Create a new one if allowed.

    if (flist.Count < fMaxItems) then
    begin
      try
        //lock is active
        item := TgoMongoClient.Create(fHost, fPort, fConnectionSettings); // May throw exception if connection fails
        item.Pooled := True;
        item.Available := false;
        flist.add(item);
        Exit(item);
      finally
        flock.release;
      end;
    end
    else //Max number of connections reached - sleep until one becomes available.
    begin
      flock.release;
      sleep(1);
    end;
  until false;
end;

function tgoConnectionPool.GetConnectionSettings: TgoMongoClientSettings;
begin
  Result := fConnectionSettings;
end;

function tgoConnectionPool.getHost: string;
begin
  Result := fHost;
end;

function tgoConnectionPool.getPort: Integer;
begin
  Result := fPort;
end;

{Purge compacts the pool by removing all currently available/unused
 connections.}

procedure tgoConnectionPool.Purge;
var
  I: Integer;
  item: IgoMongoClient;
  ItemsToKill: TArray<IgoMongoClient>;
begin
  flock.Acquire;
  try
    for I := flist.Count - 1 downto 0 do
    begin
      item := flist[I];
      if (item.Available) then
      begin
        item.Pooled := false; //indicate that item is no longer in a pool
        item.Protocol.RecycleSocket := false; //we don't want to keep the socket either
        ItemsToKill := ItemsToKill + [item];
        flist.Delete(I);
        item := nil;
      end;
    end;
  finally
    flock.release;
  end;
  {The implicit finalization of itemstokill NILS all interfaces sequentially.
  The objects will be destroyed only if they aren't in use.
  It may cost some time but the list isn't locked.}
end;

{ClearAll empties the pool. The connections are only destroyed if they are not in use}

procedure tgoConnectionPool.ClearAll;
var
  I: Integer;
  item: IgoMongoClient;
  ItemsToKill: TArray<IgoMongoClient>;
begin
  flock.Acquire;
  try
    for I := flist.Count - 1 downto 0 do
    begin
      item := flist[I];
      item.Pooled := false; //indicate that item is no longer in a pool
      item.Protocol.RecycleSocket := false; //we don't want to keep the socket either
      ItemsToKill := ItemsToKill + [item];
      flist.Delete(I);
      item := nil;
    end;
  finally
    flock.release;
  end;

  {The implicit finalization of itemstokill NILS all interfaces sequentially.
  The objects will be destroyed only if they aren't in use.
  It may cost some time but the list isn't locked.}
end;

{Remove one client from the pool, for example when it is broken}

procedure tgoConnectionPool.Remove(Client: IgoMongoClient);
var
  I: Integer;
  item: IgoMongoClient;
begin
  flock.Acquire;
  try
    for I := 0 to flist.Count - 1 do
    begin
      item := flist[I];
      if (item = Client) then
      begin
        item.Available := false;
        item.Pooled := false; //indicate that item is no longer in a pool
        item.Protocol.RecycleSocket := false; //we don't want to re-use the socket
        flist.Delete(I);
        Break;
      end;
    end;
  finally
    flock.release;
  end;
end;

procedure tgoConnectionPool.ReleaseToPool(const Client: IgoMongoClient);
begin
  Client.ReleaseToPool;
end;

{$ENDREGION}
//
{$REGION 'igoAggregationPipeline - a fluent aggregation pipeline builder'}

type
  tgoAggregationPipeline = class(TInterfacedObject, igoAggregationPipeline)
  private
    var
      fStages: TgoBsonArray;
      fBatchSize: Integer;
      fMaxTimeMS: Integer;
  public
    function Stage(const aStageName: string; aStageDocJS: string): igoAggregationPipeline; overload;
    function Stage(const aStageName: string; aStageContent: tgoBsonValue): igoAggregationPipeline; overload;
    function Stage(const aStageName: string; aStageProc: tgoDocEditor): igoAggregationPipeline; overload;

    function AddFields(aNewfieldsDoc: tgoBsonDocument): igoAggregationPipeline; overload;
    function AddFields(aNewfieldsDocJS: string): igoAggregationPipeline; overload;
    function AddFields(aNewFieldsProc: tgoDocEditor): igoAggregationPipeline; overload;

    function Match(aFilter: tgoMongoFilter): igoAggregationPipeline; overload;
    function Match(aFilterDocJs: string): igoAggregationPipeline; overload;
    function Match(aFilterProc: tgoDocEditor): igoAggregationPipeline; overload;

    function Group(aGroupDoc: tgoBsonDocument): igoAggregationPipeline; overload;
    function Group(aGroupDocJS: string): igoAggregationPipeline; overload;
    function Group(aGroupProc: tgoDocEditor): igoAggregationPipeline; overload;

    function sort(aSort: TgoMongoSort): igoAggregationPipeline; overload;
    function sort(aSortDoc: tgoBsonDocument): igoAggregationPipeline; overload;
    function sort(aSortDocJS: string): igoAggregationPipeline; overload;
    function sort(aSortProc: tgoDocEditor): igoAggregationPipeline; overload;

    function limit(n: Integer): igoAggregationPipeline;
    function &Set(aFields: array of tgoBsonElement): igoAggregationPipeline;
    function UnSet(aFields: array of string): igoAggregationPipeline;

    function Project(aProjection: TgoMongoProjection): igoAggregationPipeline; overload;
    function Project(aProjectionDocJS: string): igoAggregationPipeline; overload;
    function Project(aProjectionProc: tgoDocEditor): igoAggregationPipeline; overload;
    function Project(aProjectionDoc: tgoBsonDocument): igoAggregationPipeline; overload;

    function batchSize(aBatchsize: Integer): igoAggregationPipeline;
    function maxTimeMS(aMS: Integer): igoAggregationPipeline;

    function &Out(const aOutputCollection: string): igoAggregationPipeline; overload;
    function &Out(const aOutputDB, aOutputCollection: string): igoAggregationPipeline; overload;

    function Edit(aEditor: tgoDocEditor): igoAggregationPipeline; overload;
    function Edit(aStagenr: Integer; aEditor: tgoDocEditor): igoAggregationPipeline; overload;

    function Stages: TgoBsonArray;
    constructor Create;
  end;

function tgoAggregationPipeline.Stages: TgoBsonArray;
begin
  Result := fStages;
end;

constructor tgoAggregationPipeline.Create;
begin
  inherited Create;
  fStages := TgoBsonArray.Create;
end;

function tgoAggregationPipeline.Stage(const aStageName: string; aStageContent: tgoBsonValue): igoAggregationPipeline;
var
  doc: tgoBsonDocument;
begin
  doc := tgoBsonDocument.Create;
  doc['$' + aStageName] := aStageContent;
  fStages.add(doc);
  Result := Self;
end;

function tgoAggregationPipeline.Stage(const aStageName: string; aStageDocJS: string): igoAggregationPipeline;
var
  reader: igojsonreader;
begin
  reader := tgojsonreader.Create(aStageDocJS);
  Result := Stage(aStageName, reader.ReadDocument);
end;

function tgoAggregationPipeline.Stage(const aStageName: string; aStageProc: tgoDocEditor): igoAggregationPipeline;
var
  doc: tgoBsonDocument;
begin
  Assert(assigned(aStageProc));
  doc := tgoBsonDocument.Create;
  aStageProc(doc);
  Result := Stage(aStageName, doc);
end;

function tgoAggregationPipeline.Match(aFilter: tgoMongoFilter): igoAggregationPipeline;
begin
  Result := Stage('match', aFilter.Render);
end;

function tgoAggregationPipeline.Match(aFilterDocJs: string): igoAggregationPipeline;
begin
  Result := Stage('match', aFilterDocJs);
end;

function tgoAggregationPipeline.Match(aFilterProc: tgoDocEditor): igoAggregationPipeline;
begin
  Result := Stage('match', aFilterProc);
end;

function tgoAggregationPipeline.sort(aSort: TgoMongoSort): igoAggregationPipeline;
begin
  Result := Stage('sort', aSort.Render);
end;

function tgoAggregationPipeline.sort(aSortDocJS: string):
  igoAggregationPipeline;
begin
  Result := Stage('sort', aSortDocJS);
end;

function tgoAggregationPipeline.sort(aSortProc: tgoDocEditor): igoAggregationPipeline;
begin
  Result := Stage('sort', aSortProc);
end;

function tgoAggregationPipeline.sort(aSortDoc: tgoBsonDocument): igoAggregationPipeline;
begin
  Result := Stage('sort', aSortDoc);
end;

function tgoAggregationPipeline.limit(n: Integer): igoAggregationPipeline;
var
  t: tgoBsonValue;
begin
  t := n;
  Result := Stage('limit', t);
end;

function tgoAggregationPipeline.&Set(aFields: array of tgoBsonElement): igoAggregationPipeline;
var
  doc: tgoBsonDocument;
  afield: tgoBsonElement;
begin
  doc := tgoBsonDocument.Create;
  for afield in aFields do
    doc.add(afield);
  Result := Stage('set', doc);
end;

function tgoAggregationPipeline.Group(aGroupDoc: tgoBsonDocument): igoAggregationPipeline;
begin
  Result := Stage('group', aGroupDoc);
end;

function tgoAggregationPipeline.Group(aGroupDocJS: string):
  igoAggregationPipeline;
begin
  Result := Stage('group', aGroupDocJS);
end;

function tgoAggregationPipeline.Group(aGroupProc: tgoDocEditor): igoAggregationPipeline;
begin
  Result := Stage('group', aGroupProc);
end;

function tgoAggregationPipeline.UnSet(aFields: array of string): igoAggregationPipeline;
var
  arr: TgoBsonArray;
  afield: string;
begin
  arr := TgoBsonArray.Create;
  for afield in aFields do
    arr.add(afield);
  Result := Stage('unset', arr);
end;

function tgoAggregationPipeline.Project(aProjection: TgoMongoProjection): igoAggregationPipeline;
begin
  Result := Stage('project', aProjection.Render);
end;

function tgoAggregationPipeline.Project(aProjectionDocJS: string): igoAggregationPipeline;
begin
  Result := Stage('project', aProjectionDocJS);
end;

function tgoAggregationPipeline.Project(aProjectionProc: tgoDocEditor): igoAggregationPipeline;
begin
  Result := Stage('project', aProjectionProc);
end;

function tgoAggregationPipeline.Project(aProjectionDoc: tgoBsonDocument): igoAggregationPipeline;
begin
  Result := Stage('project', aProjectionDoc);
end;

function tgoAggregationPipeline.AddFields(aNewfieldsDoc: tgoBsonDocument): igoAggregationPipeline;
begin
  Result := Stage('addFields', aNewfieldsDoc);
end;

function tgoAggregationPipeline.AddFields(aNewfieldsDocJS: string):
  igoAggregationPipeline;
begin
  Result := Stage('addFields', aNewfieldsDocJS);
end;

function tgoAggregationPipeline.AddFields(aNewFieldsProc: tgoDocEditor): igoAggregationPipeline;
begin
  Result := Stage('addFields', aNewFieldsProc);
end;

function tgoAggregationPipeline.batchSize(aBatchsize: Integer): igoAggregationPipeline;
begin
  Result := Self;
  fBatchSize := aBatchsize;
end;

function tgoAggregationPipeline.maxTimeMS(aMS: Integer): igoAggregationPipeline;
begin
  Result := Self;
  fMaxTimeMS := aMS;
end;

function tgoAggregationPipeline.out(const aOutputCollection: string): igoAggregationPipeline;
var
  t: tgoBsonValue;
begin
  t := aOutputCollection;
  Result := Stage('out', t)
end;

function tgoAggregationPipeline.&Out(const aOutputDB, aOutputCollection: string): igoAggregationPipeline;
var
  doc: tgoBsonDocument;
begin
  doc := tgoBsonDocument.Create;
  doc['db'] := aOutputDB;
  doc['coll'] := aOutputCollection;
  Result := Stage('out', doc);
end;

function tgoAggregationPipeline.Edit(aEditor: tgoDocEditor):
  igoAggregationPipeline;
begin
  Result := Edit(fStages.Count - 1, aEditor);
end;

function tgoAggregationPipeline.Edit(aStagenr: Integer; aEditor: tgoDocEditor): igoAggregationPipeline;
var
  doc: tgoBsonDocument;
begin
  Result := Self;
  if (aStagenr >= 0) and (aStagenr < fStages.Count) then
  begin
    doc := fStages[aStagenr].asBsonDocument;
    if doc.Count >= 1 then
      if doc.Elements[0].Value.IsBsonDocument then //  edit "value" of first field
        aEditor(doc.Elements[0].Value.asBsonDocument);
  end;
end;

{$ENDREGION}
//
{$REGION 'tgoMongoExpression'}

class function tgoMongoExpression.convert(const input, &to, onError, onNull: string): string;
begin
  Result := Format('{ "$convert": { input: %s , to: %s', [input, &to]);

  if onError <> '' then
    Result := Result + Format(' , onError: %s', [onError]);

  if onNull <> '' then
    Result := Result + Format(' , onNull: %s', [onNull]);

  Result := Result + ' } }';
end;

class function tgoMongoExpression.Count: string;
begin
  Result := '{ "$count": { } }';
end;

class function tgoMongoExpression.toLong(const Expr: string): string;
begin
  Result := Format('{ "$toLong": %s }', [Expr]);
end;

class function tgoMongoExpression.toLower(const Expr: string): string;
begin
  Result := Format('{ "$toLower": %s }', [Expr]);
end;

class function tgoMongoExpression.toObjectId(const Expr: string): string;
begin
  Result := Format('{ "$toObjectId": %s }', [Expr]);
end;

class function tgoMongoExpression.ToString(const Expr: string): string;
begin
  Result := Format('{ "$toString": %s }', [Expr]);
end;

class function tgoMongoExpression.toUpper(const Expr: string): string;
begin
  Result := Format('{ "$toUpper": %s }', [Expr]);
end;

class function tgoMongoExpression.toUUID(const Expr: string): string;
begin
  Result := Format('{ "$toUUID": %s }', [Expr]);
end;

class function tgoMongoExpression.toBool(const Expr: string): string;
begin
  Result := Format('{ "$toBool": %s }', [Expr]);
end;

class function tgoMongoExpression.toDate(const Expr: string): string;
begin
  Result := Format('{ "$toDate": %s }', [Expr]);
end;

class function tgoMongoExpression.toDecimal(const Expr: string): string;
begin
  Result := Format('{ "$toDecimal": %s }', [Expr]);
end;

class function tgoMongoExpression.toDouble(const Expr: string): string;
begin
  Result := Format('{ "$toDouble": %s }', [Expr]);
end;

class function tgoMongoExpression.toInt(const Expr: string): string;
begin
  Result := Format('{ "$toInt": %s }', [Expr]);
end;

class function tgoMongoExpression.trunc(const Expr: string; const place: string = '0'): string;
begin
  Result := Format('{ "$trunc": [%s, %s] }', [Expr, place]);
end;

function TzExpr(const DateExpr: string; const timezone: string = ''): string;
begin
  if timezone = '' then
    Result := Format('{ date: %s}', [DateExpr])
  else
    Result := Format('{ date: %s , timezone: %s }', [DateExpr, timezone]);
end;

class function tgoMongoExpression.millisecond(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$millisecond": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.second(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$second": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.minute(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$minute": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.hour(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$hour": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.isNumber(const Expr: string): string;
begin
  Result := Format('{ "$isNumber": %s }', [Expr]);
end;

class function tgoMongoExpression.&type(const Expr: string): string;
begin
  Result := Format('{ "$type": %s }', [Expr]);
end;

class function tgoMongoExpression.ISOweek(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$isoWeek": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.ISOweekYear(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$isoWeekYear": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.week(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$week": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.month(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$month": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.year(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$year": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.dayOfYear(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$dayOfYear": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.dayOfMonth(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$dayOfMonth": %s }', [TzExpr(DateExpr, timezone)]);
end;

class function tgoMongoExpression.dayOfWeek(const DateExpr: string; const timezone: string = ''): string;
begin
  Result := Format('{ "$dayOfWeek": %s }', [TzExpr(DateExpr, timezone)]);
end;

function DateAddExpr(const StartDate, &unit, amount: string; const timezone: string = ''): string;
begin
  Result := Format('{ startDate: %s , unit: %s , amount: %s', [StartDate, &unit, amount]);
  if timezone <> '' then
    Result := Result + Format(' , timezone: %s', [timezone]);
  Result := Result + ' }';
end;

class function tgoMongoExpression.dateToString(const DateExpr, fmt, timezone, onNull: string): string;
begin
  Result := Format('{ "$dateToString": { date: %s', [DateExpr]);

  if fmt <> '' then
    Result := Result + Format(' , format: %s', [fmt]);

  if timezone <> '' then
    Result := Result + Format(' , timezone: %s', [timezone]);

  if onNull <> '' then
    Result := Result + Format(' , onNull: %s', [onNull]);

  Result := Result + ' } }';
end;

class function tgoMongoExpression.dateAdd(const StartDate, &unit, &amount, timezone: string): string;
begin
  Result := Format('{ "$dateAdd": %s }', [DateAddExpr(StartDate, &unit, amount, timezone)]);
end;

class function tgoMongoExpression.dateSubtract(const StartDate, &unit, amount, timezone: string): string;
begin
  Result := Format('{ "$dateSubtract": %s }', [DateAddExpr(StartDate, &unit, amount, timezone)]);
end;

class function tgoMongoExpression.ceil(const Expr: string): string;
begin
  Result := Format('{ "$ceil": %s }', [Expr]);
end;

class function tgoMongoExpression.cmp(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$cmp": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.log(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$log": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.pow(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$pow": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.&array(const Expr: array of string): string;
var
  s: string;
begin
  Result := '[';
  for s in Expr do
    Result := Result + s + ',';
  SetLength(Result, length(Result) - 1);
  Result := Result + ']';
end;

class function tgoMongoExpression.asBson(const json: string): tgoBsonValue;
var
  reader: igojsonreader;
begin
  reader := tgojsonreader.Create(json, True);
  Result := reader.ReadValue;
end;

class function tgoMongoExpression.&and(const Expr: array of string): string;
begin
  Result := Format('{ "$and": %s }', [&array(Expr)]);
end;

class function tgoMongoExpression.&or(const Expr: array of string): string;
begin
  Result := Format('{ "$or": %s }', [&array(Expr)]);
end;

class function tgoMongoExpression.concat(const Expr: array of string): string;
begin
  Result := Format('{ "$concat": %s }', [&array(Expr)]);
end;

class function tgoMongoExpression.cond(const aIf, aThen, aElse: string): string;
begin
  Result := Format('{ "$cond": { if: %s , then: %s , else: %s } }', [aIf, aThen, aElse]);
end;

class function tgoMongoExpression.bitAnd(const Expr: array of string): string;
begin
  Result := Format('{ "$bitAnd": %s }', [&array(Expr)]);
end;

class function tgoMongoExpression.bitNot(const Expr1: string): string;
begin
  Result := Format('{ "$bitNot": %s }', [Expr1]);
end;

class function tgoMongoExpression.bitOr(const Expr: array of string): string;
begin
  Result := Format('{ "$bitOr": %s }', [&array(Expr)]);
end;

class function tgoMongoExpression.bitXor(const Expr: array of string): string;
begin
  Result := Format('{ "$bitXor": %s }', [&array(Expr)]);
end;

class function tgoMongoExpression.arrayElementAt(const Expr1: array of string; const Expr2: string): string;
begin
  Result := Format('{ "$arrayElementAt": [%s , %s]}', [&array(Expr1), Expr2]);
end;

class function tgoMongoExpression.ifNull(const Expr: array of string): string;
begin
  Result := Format('{ "$ifNull": %s }', [&array(Expr)]);
end;

class function tgoMongoExpression.&in(const Expr1: string; const Expr2: array of string): string;
begin
  Result := Format('{ "$in": [%s , %s]}', [Expr1, &array(Expr2)]);
end;

class function tgoMongoExpression.floor(const Expr: string): string;
begin
  Result := Format('{ "$floor": %s }', [Expr]);
end;

class function tgoMongoExpression.&const(const ConstantValue: Boolean): string;
begin
  if ConstantValue then
    Result := 'true'
  else
    Result := 'false';
end;

class function tgoMongoExpression.&const(const ConstantValue: string): string;
begin
  Result := Format('"%s"', [ConstantValue])
end;

class function tgoMongoExpression.&const(const ConstantValue: Int64): string;
begin
  Result := inttostr(ConstantValue);
end;

class function tgoMongoExpression.&const(const ConstantValue: Double; Decimals: Integer): string;
begin
  Str(ConstantValue: 0: Decimals, Result);
end;

class function tgoMongoExpression.ref(const FieldName: string): string;
begin
  Result := Format('"$%s"', [FieldName]);
end;

class function tgoMongoExpression.multiply(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$multiply": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.add(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$add": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.round(const Expr, place: string): string;
begin
  Result := Format('{ "$round": [%s, %s] }', [Expr, place]);
end;

class function tgoMongoExpression.split(const Expr, Delimiter: string): string;
begin
  Result := Format('{ "$split": [%s, %s] }', [Expr, Delimiter]);
end;

class function tgoMongoExpression.substr(const Expr, start, len: string): string;
begin
  Result := Format('{ "$substr": [%s, %s, %s] }', [Expr, start, len]);
end;

class function tgoMongoExpression.ltrim(const Expr: string; const NumChars: string = ''): string;
begin
  if NumChars <> '' then
    Result := Format('{ "$ltrim": {input: %s, chars: %s} }', [Expr, NumChars])
  else
    Result := Format('{ "$ltrim": {input: %s} }', [Expr, NumChars])
end;

class function tgoMongoExpression.rtrim(const Expr: string; const NumChars: string = ''): string;
begin
  if NumChars <> '' then
    Result := Format('{ "$rtrim": {input: %s, chars: %s} }', [Expr, NumChars])
  else
    Result := Format('{ "$rtrim": {input: %s} }', [Expr, NumChars])
end;

class function tgoMongoExpression.subtract(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$subtract": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.divide(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$divide": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.&mod(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$mod": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.eq(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$eq": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.gt(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$gt": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.gte(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$gte": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.lt(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$lt": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.lte(const Expr1, Expr2: string): string;
begin
  Result := Format('{ "$lte": [%s, %s] }', [Expr1, Expr2]);
end;

class function tgoMongoExpression.&not(const Expr: string): string;
begin
  Result := Format('{ "$not": %s }', [Expr]);
end;

class function tgoMongoExpression.first(const Expr: string): string;
begin
  Result := Format('{ "$first": %s }', [Expr]);
end;

class function tgoMongoExpression.last(const Expr: string): string;
begin
  Result := Format('{ "$last": %s }', [Expr]);
end;

class function tgoMongoExpression.literal(const Expr: string): string;
begin
  Result := Format('{ "$literal": %s }', [Expr]);
end;

class function tgoMongoExpression.avg(const Expr: string): string;
begin
  Result := Format('{ "$avg": %s }', [Expr]);
end;

class function tgoMongoExpression.min(const Expr: string): string;
begin
  Result := Format('{ "$min": %s }', [Expr]);
end;

class function tgoMongoExpression.max(const Expr: string): string;
begin
  Result := Format('{ "$max": %s }', [Expr]);
end;

class function tgoMongoExpression.abs(const Expr: string): string;
begin
  Result := Format('{ "$abs": %s }', [Expr]);
end;

class function tgoMongoExpression.exp(const Expr: string): string;
begin
  Result := Format('{ "$exp": %s }', [Expr]);
end;

class function tgoMongoExpression.ln(const Expr: string): string;
begin
  Result := Format('{ "$ln": %s }', [Expr]);
end;

class function tgoMongoExpression.log10(const Expr: string): string;
begin
  Result := Format('{ "$log10": %s }', [Expr]);
end;

class function tgoMongoExpression.sqrt(const Expr: string): string;
begin
  Result := Format('{ "$sqrt": %s }', [Expr]);
end;
{$ENDREGION}

//
class function Aggregate.Expression: tgoMongoExpressionClass;
begin
  {Not a "real" class factory, just returns a class type.
  Since all methods are static class methods, that's quite OK.}

  Result := tgoMongoExpression;
end;

class function Aggregate.CreatePipeline: igoAggregationPipeline;
begin
  Result := tgoAggregationPipeline.Create;
end;

end.

