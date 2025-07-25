unit Grijjy.MongoDB.Protocol;
{ < Implements the MongoDB Wire Protocol.
  This unit is only used internally. }

{$INCLUDE 'Grijjy.inc'}
{$B-}

interface

uses
  System.Diagnostics, System.Math, System.SyncObjs, System.SysUtils, System.Generics.Collections, Grijjy.SysUtils,
{$IF Defined(MSWINDOWS)}
  Grijjy.SocketPool.Win,
{$ELSEIF Defined(LINUX)}
  Grijjy.SocketPool.Linux,
{$ELSE}
{$MESSAGE Error 'The MongoDB driver is only supported on Windows and Linux'}
{$ENDIF}

  Grijjy.MongoDB.Compressors, Grijjy.Bson;

const
  { Virtual collection that is used for query commands }
  COLLECTION_COMMAND = '$cmd';
  { System collections }
  DB_ADMIN = 'admin';

const
  CompressorID_Snappy = 1;
  CompressorID_Zlib = 2;
  CompressorID_Highest = CompressorID_Zlib;

type

  { Base class for MongoDB errors }
  EgoMongoDBError = class(Exception);

  { Is raised when a connection error (or timeout) occurs. }
  EgoMongoDBConnectionError = class(EgoMongoDBError);

  tgoMongoReadPreference = (primary = 0, primaryPreferred, secondary, secondaryPreferred, nearest, fromParent = 31);

  { Query flags as used by TgoMongoProtocol.OpQuery }
  TgoMongoQueryFlag = (// OBSOLETE
    { Tailable means cursor is not closed when the last data is retrieved.
      Rather, the cursor marks the final object�s position.
      You can resume using the cursor later, from where it was located,
      if more data were received. Like any �latent cursor�, the cursor may
      become invalid at some point (CursorNotFound) � for example if the final
      object it references were deleted. }
    TailableCursor = 1, // =>DBQuery.Option.Tailable   bit 1 = value 2

    { Allow query of replica slave. Normally these return an error except for
      namespace �local�. }
    SlaveOk = 2, // =>DBQuery.Option.SlaveOK bit 2 = value 4

    { Internal replication use only - driver should not set. }
    OplogRelay = 3,

    { The server normally times out idle cursors after an inactivity period
      (10 minutes) to prevent excess memory use. Set this option to prevent
      that. }
    NoCursorTimeout = 4, // =>DBQuery.Option.NoTimeout   bit 4 = 16

    { Use with TailableCursor. If we are at the end of the data, block for a
      while rather than returning no data. After a timeout period, we do return
      as normal. }
    AwaitData = 5, // =>DBQuery.Option.AwaitData   bit 5=32

    { Stream the data down full blast in multiple �more� packages, on the
      assumption that the client will fully read all data queried. Faster when
      you are pulling a lot of data and know you want to pull it all down.
      Note: the client is not allowed to not read all the data unless it closes the connection. }
    Exhaust = 6, // =>DBQuery.Option.Exhaust  bit 6=64

    { Get partial results from a mongos if some shards are down (instead of
      throwing an error) }
    Partial = 7); // =>DBQuery.Option.Partial  bit 7=128

  TgoMongoQueryFlags = set of TgoMongoQueryFlag;

  { Flags for new OP_MSG protocol }

  TGoMongoMsgFlag = (msgfChecksumPresent, msgfMoreToCome, msgfExhaustAllowed = 16, msgfPadding = 31); // padded to ensure the SET is 32 bits

  TgoMongoMsgFlags = set of TGoMongoMsgFlag; // is 4 bytes in size

type
  { Payload type 1 for OP_MSG }
  tgoPayloadType1 = record
    Name: string;
    Docs: TArray<tBytes>;
    procedure WriteTo(buffer: tgoByteBuffer);
  end;

  { A reply to a query (see TgoMongoProtocol.OpMsg) }

  IgoMongoReply = interface
    ['{25CEF8E1-B023-4232-BE9A-1FBE9E51CE57}']
{$REGION 'Internal Declarations'}
    function _GetResponseTo: Integer;
    function _GetPayload0: TArray<tBytes>; { Every message should have exactly ONE document section of type 0 }
    function _GetPayload1: TArray<tgoPayloadType1>; { Every message can have any number of type 1 sections }
    function _FirstDoc: TgoBsonDocument;
{$ENDREGION 'Internal Declarations'}
    { The identifier of the message that this reply is response to. }
    property ResponseTo: Integer read _GetResponseTo;
    { First BSON document in the reply. Always of payload type 0 }
    property FirstDoc: TgoBsonDocument read _FirstDoc;
    property Payload0: TArray<tBytes>read _GetPayload0;
    property Payload1: TArray<tgoPayloadType1>read _GetPayload1;
  end;

type
  { Mongo authentication mechanism }
  TgoMongoAuthMechanism = (None, SCRAM_SHA_1, SCRAM_SHA_256);

  { Customizable protocol settings. }
  TgoMongoProtocolSettings = record
  public
    { Timeout waiting for connection, in milliseconds.
      Defaults to 5000 (5 seconds) }
    ConnectionTimeout: Integer;

    { Timeout waiting for partial or complete reply events, in milliseconds.
      Defaults to 5000 (5 seconds) }
    ReplyTimeout: Integer;

    { Default query flags }
    QueryFlags: TgoMongoQueryFlags; // OBSOLETE

    GlobalReadPreference: tgoMongoReadPreference;

    { Tls enabled }
    Secure: Boolean;

    { X.509 Certificate in PEM format, if any }
    Certificate: tBytes;

    { X.509 Private key in PEM format, if any }
    PrivateKey: tBytes;

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

  end;

const
  OP_MSG = 2013;
  OP_COMPRESSED = 2012;

type
  TgoMongoProtocol = class
{$REGION 'Internal Declarations'}
    type

  private
    const
      RECV_BUFFER_SIZE = 128 * 1024; //larger initial buffer to avoid excessive reallocation
      EMPTY_DOCUMENT: array[0..4] of Byte = (5, 0, 0, 0, 0);
  private
    class var
      FClientSocketManager: TgoClientSocketManager;
  private
    FHost: string;
    FPort: Integer;
    fRecycleSocket: Boolean;
    FSettings: TgoMongoProtocolSettings;
    FNextRequestId: Integer;
    FConnection: TgoSocketConnection;
    FConnectionLock: TCriticalSection;
    FCompletedReplies: TDictionary<Integer, IgoMongoReply>;
    FPartialReplies: TDictionary<Integer, tStopWatch>;
    fReplyEvents: TDictionary<Integer, TEvent>;
    fReplyEvent: TEvent; // currently only support request-response model, so need only one event
    FRepliesLock: TCriticalSection;
    FRecvBuffer: tBytes;
    FRecvSize: Integer;
    FRecvBufferLock: TCriticalSection;
    FAuthErrorMessage: string;
    FAuthErrorCode: Integer;
    FMinWireVersion: Integer;
    FMaxWireVersion: Integer;
    FMaxWriteBatchSize: Integer;
    FMaxMessageSizeBytes: Integer;
    fServerknowsZlib, fServerknowsSnappy: Boolean;
    rec__1: Integer;
    fSupportsReplication:Boolean;

  private
    { internal msg + reply handling}
    procedure Send(const adata: tBytes);
    procedure Recover;
    function WaitForReply(const ARequestId: Integer; const aReplyEvent: TEvent; aTimeoutMS: Integer = 0): IgoMongoReply;
    function TryGetReply(const ARequestId: Integer; out AReply: IgoMongoReply): Boolean; inline;
    function LastPartialReply(const ARequestId: Integer; out ALastRecv: tStopWatch): Boolean;
    function HaveReplyMsgHeader(out AMsgHeader; tb: tBytes; Size: Integer): Boolean; overload;
    function HaveReplyMsgHeader(out AMsgHeader): Boolean; overload;
    procedure RemoveReply(const ARequestId: Integer);
    procedure UpdateReplyTimeout(const ARequestId: Integer);
    procedure Compress(var Packet: tBytes);
    function EnsureCapacity(CapacityNeeded: Integer): Boolean;
    procedure ClearReplies;

    { Connection state }
    function GetConnected: Boolean;
    procedure SetConnected(Value: Boolean); // may throw exception


    function Reconnect: Boolean;
    function getRecycleSocket: Boolean;
    procedure setRecycleSocket(const Value: Boolean);

    { Connection internals. Routines starting with __  must be protected against recursion, wrapped in critical section.}

    function __Connected: Boolean;
    function __Reconnect: Boolean;
    procedure __DisposeConnection;
    procedure __RecycleConnection;
    function __RequestConnection: Boolean;
    function __ConnectSocket: Boolean;


    { authentication }
    function saslStart(const APayload: string): IgoMongoReply;
    function saslContinue(const AConversationId: Integer; const APayload: string): IgoMongoReply;
    function Authenticate: Boolean;

    {feature handshake}
    procedure ProtocolDefaults;
    procedure NegotiateProtocol;

    { Socket events }
    procedure SocketConnected;    //unused
    procedure SocketDisconnected; //unused
    procedure SocketRecv(const ABuffer: Pointer; const ASize: Integer);

  public
    class procedure ConnectionFailedException(aMessage: string=''); static;
    class function IsInternalError(const errorcode: Integer): Boolean; static;
    procedure PrepareForReuse;
    function CanUseCompression: Boolean;
    function ThisMoment: tStopWatch;
    class constructor Create;
    class destructor Destroy;
{$ENDREGION 'Internal Declarations'}
  public
    { Creates the protocol.

      Parameters:
      AHost: host address of the MongoDB server to connect to.
      APort: connection port.
      ASettings: custom protocol settings. }
    constructor Create(const AHost: string; const APort: Integer; const ASettings: TgoMongoProtocolSettings);
    destructor Destroy; override;
    function OpMsg(const ParamType0: tBytes; const ParamsType1: TArray<tgoPayloadType1>; NoResponse: Boolean; aTimeoutMS: Integer):
      IgoMongoReply;

    function EnsureConnected: Boolean; //auto-reconnect, used in opmsg
    function SupportsReplication:Boolean;
    function SupportsTransactions:Boolean;


    { Authenticate error message if failed }
    property AuthErrorMessage: string read FAuthErrorMessage;
    { Authenticate error code if failed }
    property AuthErrorCode: Integer read FAuthErrorCode;
    property MinWireVersion: Integer read FMinWireVersion;
    property MaxWireVersion: Integer read FMaxWireVersion;
    property MaxWriteBatchSize: Integer read FMaxWriteBatchSize write FMaxWriteBatchSize;
    property MaxMessageSizeBytes: Integer read FMaxMessageSizeBytes write FMaxMessageSizeBytes;
    property GlobalReadPreference: tgoMongoReadPreference read FSettings.GlobalReadPreference write FSettings.GlobalReadPreference;
    property Connected: Boolean read GetConnected write SetConnected;
    property RecycleSocket: Boolean read getRecycleSocket write setRecycleSocket;
    property ReplyTimeout: Integer read FSettings.ReplyTimeout write FSettings.ReplyTimeout;

  end;

resourcestring
  RS_MONGODB_AUTHENTICATION_ERROR = 'Error authenticating [%d] %s';

const
  { Maximum number of documents that can be written in bulk at once }
  DEF_MAX_BULK_SIZE = 1000;
  DEF_MAX_MSG_SIZE = 32 * 1024 * 1024;

implementation

uses
  System.DateUtils, Grijjy.Bson.IO, Grijjy.Scram;

type
  TMsgHeader = packed record
    MessageLength: Int32;
    RequestID: Int32;
    ResponseTo: Int32;
    OpCode: Int32;
    function ValidOpcode: Boolean; // 2012=op_msg; 2013= compressed
    function Compressed: Boolean;
    function DataStart: Pointer;
    function DataSize: Integer;
  end;

  PMsgHeader = ^TMsgHeader;

type
  TOPMSGHeader = packed record
    Header: TMsgHeader;
    flagbits: TgoMongoMsgFlags; // flagbits is part of the DATA. 32 bits because msgfExhaustAllowed = 16
    // Section+
    // checksum uint32
  end;

  TOPCompressedHeader = packed record
    Header: TMsgHeader; // having message type 2012
    OriginalOpCode: Int32;
    UncompressedSize: Int32;
    CompressorID: Byte;
    function UnCompressedMessageSize: Integer;
    function UnCompressedDataSize: Integer;
    function CompressedDataSize: Integer;
    function DataStart: Pointer;
    // compressed data follows.
    // Consisting of flagbits, followed by Section+ and maybe checksum.
  end;

  POpMsgHeader = ^TOPMSGHeader;

  POPCompressedHeader = ^TOPCompressedHeader;

  tCRC32 = Cardinal;

  tgoPayloadDecodeResult = (pdEOF, pdInvalidPayloadType, pdBufferOverrun, pdOK);

  tMsgPayload = class
    const
      EmptyDocSize = 5;
      MinSequenceSize = 1 + EmptyDocSize;

      // Try to read a BSON document from a buffer. if successful, update "Bytesread".
      // The BSON document itself is not validated.
      // If testmode=true, it is a testrun only, the returned array is empty.
    class function ReadBsonDoc(TestMode: Boolean; out Bson: tBytes; buffer: Pointer; BytesAvail: Integer; var BytesRead: Integer):
      tgoPayloadDecodeResult;

    // Try to read a TYPE 0 or Type1 payload from a buffer. If successful, update "Bytesread".
    // The BSON documents themselves are not validated.
    // If testmode=true, it is a testrun only, the returned array is empty.
    class function DecodeSequence(TestMode: Boolean; SeqStart: Pointer; SizeAvail: Integer; var SizeRead: Integer; var PayloadType: Byte;
      var Name: string; var data: TArray<tBytes>): tgoPayloadDecodeResult;
  end;

  tgoReplyValidationResult = (rvrOK, // Message decoded successfully
    rvrNoHeader, // Not enough bytes received for a header
    rvrOpcodeInvalid, // Invalid opcode in header
    rvrGrowing, // Message not complete yet, still growing in reception buffer
    rvrCompressorNotSupported, // do not know this compression algorithm
    rvrCompressorError, // unable to decompress, error
    rvrChecksumInvalid, // to be implemented
    rvrDataError); // decoding of message failed, badly formatted or data corruption

  { Implements IgoMongoReply - it is an OP_MSG sent by the server to the client. }

  TgoMongoMsgReply = class(TInterfacedObject, IgoMongoReply)
  private
    FHeader: TOPMSGHeader;
    FPayload0: TArray<tBytes>;
    FPayload1: TArray<tgoPayloadType1>;
    FFirstDoc: TgoBsonDocument;
  protected
    { IgoMongoReply }
    function _GetResponseTo: Integer;
    function _GetPayload0: TArray<tBytes>;
    function _GetPayload1: TArray<tgoPayloadType1>;
    function _FirstDoc: TgoBsonDocument;
  public
    class function ValidateOPMessage(const ABuffer: tBytes; const ASize: Integer; var aSizeRead: Integer; out AReply: IgoMongoReply):
      tgoReplyValidationResult;

    class function ValidateMessage(const ABuffer: tBytes; const ASize: Integer; var aSizeRead: Integer; out AReply: IgoMongoReply):
      tgoReplyValidationResult;

    procedure ReadData(const ABuffer: Pointer; const ASize: Integer);
    constructor Create(const ABuffer: Pointer; const ASize: Integer);
    constructor CreateFromError(aReplyTo, aCode: Integer; aErrMsg, aCodeName:
      string);

  end;

{ tgoPayloadType1 }

procedure tgoPayloadType1.WriteTo(buffer: tgoByteBuffer);
{ Convert an arbitrary number of bson documents into a MSG payload of type 1 }
var
  Cstring: utf8string;
  MarkPos, I, SomeInteger: Integer;
  pSize: pInteger;
  PayloadType: Byte;
begin
  PayloadType := 1;
  buffer.Append(PayloadType); // type comes before before "size" marker
  MarkPos := buffer.Size; // Position of the "size" marker in the stream
  SomeInteger := 0; // placeholder for Size
  buffer.AppendBuffer(SomeInteger, sizeof(Integer));
  Cstring := utf8string(Name);
  buffer.AppendBuffer(Cstring[low(utf8string)], length(Cstring) + 1); // string plus #0
  if Assigned(Docs) then
    for I := 0 to high(Docs) do
      if Assigned(Docs[I]) then
        buffer.Append(Docs[I]);
  pSize := @buffer.buffer[MarkPos];
  pSize^ := buffer.Size - MarkPos; // number of bytes written after "markpos"
end;

{ TgoMongoProtocol }

class constructor TgoMongoProtocol.Create;
begin
  FClientSocketManager := TgoClientSocketManager.Create(TgoSocketOptimization.Scale, TgoSocketPoolBehavior.PoolAndReuse);
end;

class destructor TgoMongoProtocol.Destroy;
begin
  FreeAndNil(FClientSocketManager);
end;

constructor TgoMongoProtocol.Create(const AHost: string; const APort: Integer; const ASettings: TgoMongoProtocolSettings);
begin
  Assert(AHost <> '');
  Assert(APort <> 0);
  inherited Create;
  fRecycleSocket:=True;
  FHost := AHost;
  FPort := APort;
  FMaxWriteBatchSize := DEF_MAX_BULK_SIZE;
  FMaxMessageSizeBytes := DEF_MAX_MSG_SIZE; // 2 x maximum message size of 16 mb
  FSettings := ASettings;
  FConnectionLock := TCriticalSection.Create;
  FRepliesLock := TCriticalSection.Create;
  FRecvBufferLock := TCriticalSection.Create;
  FCompletedReplies := TDictionary<Integer, IgoMongoReply>.Create;
  FPartialReplies := TDictionary<Integer, tStopWatch>.Create;
  fReplyEvents := TDictionary<Integer, TEvent>.Create;
  fReplyEvent := TEvent.Create(nil, True, False, ''); //Currently, only support request-response model
  SetLength(FRecvBuffer, RECV_BUFFER_SIZE);
  ProtocolDefaults;
end;

procedure TgoMongoProtocol.ProtocolDefaults; {most basic protocol - Disable compression etc }
begin
  FMaxWireVersion := 1;
  FMinWireVersion := 1;
  FMaxWriteBatchSize := DEF_MAX_BULK_SIZE;
  FMaxMessageSizeBytes := DEF_MAX_MSG_SIZE;
  fServerknowsZlib := False;
  fServerknowsSnappy := False;
  fSupportsReplication:=False;
end;

//ClearReplies: clears the reply queues
procedure TgoMongoProtocol.ClearReplies;
begin
  FRepliesLock.Acquire;
  try
    FCompletedReplies.Clear;
    FPartialReplies.Clear;
    fReplyEvents.Clear;
  finally
    FRepliesLock.Release;
  end;
end;



destructor TgoMongoProtocol.Destroy;
begin
  FConnectionLock.Acquire;
  if fRecycleSocket then
      __RecycleConnection
  else
      __DisposeConnection;
  FConnectionLock.Release;

  FCompletedReplies.Free;
  FPartialReplies.Free;
  fReplyEvents.Free;
  FRepliesLock.Free;
  FConnectionLock.Free;
  FRecvBufferLock.Free;
  fReplyEvent.Free;
  inherited;
end;




// Getter of property Connected
function TgoMongoProtocol.GetConnected: Boolean;
begin
  FConnectionLock.Acquire;
  result:=__Connected;
  FConnectionLock.Release;
end;



class procedure TgoMongoProtocol.ConnectionFailedException(aMessage: string);
begin
  if aMessage = '' then
    aMessage := 'MongoDB Connection Failed';
  raise EgoMongoDBConnectionError.Create(aMessage);
end;

// Setter of property Connected
procedure TgoMongoProtocol.SetConnected(Value: Boolean);
begin
  if (Value <> GetConnected) then
  begin
    if Value then
    begin
      if not Reconnect() then
        ConnectionFailedException;
    end
    else
    begin
      FConnectionLock.Acquire;
      __DisposeConnection; //closes the connection without recycling it
      FConnectionLock.Release;
    end;
  end;
end;

// EnsureConnected() : called from inside op_msg()
// Checks if we're connected, auto-reconnects if necessary.

function TgoMongoProtocol.EnsureConnected: Boolean;
begin
  result := GetConnected();
  if (not result) then
    result := Reconnect();
end;


// Reconnect()
// it disposes of the old connection, creates a new one and connects.
// The method is protected against recursion :
// reconnect --> Authenticate+Hello --> op_msg --> EnsureConnected --> Reconnect (recursion)

function TgoMongoProtocol.Reconnect: Boolean;
begin
  result := False;
  atomicincrement(rec__1);
  try
    if (rec__1 > 1) then
      Exit(GetConnected) //protection against recursive calls
    else
    begin
      FConnectionLock.Acquire;
      try
        result:=__Reconnect;  //call __reconnect, inside a critical section
      finally
        FConnectionLock.Release;
      end;
    end;
  finally
    atomicdecrement(rec__1);
  end;
end;


//__reconnect()
// Internal routine, must be wrapped inside fConnectionLock critical section
// Exceptions are possible if authentication fails

function TgoMongoProtocol.__Reconnect: Boolean;
begin
    __DisposeConnection(); //get rid of an old connection
    if not __RequestConnection() then  //get a new one or an existing one
      Exit(False); //failed? then shutting down ...
    result := __ConnectSocket(); //connect socket only if it was down
    if result then
    begin
      ClearReplies; //Start with an empty reply buffer
      ProtocolDefaults; //most basic protocol - Disable compression etc

      //from here on, op_msg is going to be used, which calls ensureconnected() recursively

      if not Authenticate() then // SCRAM Authenticate , Always do this, because credentials may have changed
        raise EgoMongoDBConnectionError.Create(Format(RS_MONGODB_AUTHENTICATION_ERROR, [FAuthErrorCode, FAuthErrorMessage]));

      NegotiateProtocol; // Negotiate protocol features and compression, ignore exceptions
    end;
end;



//__RequestConnection: Try to obtain a TgoSocketConnection object from the ClientSocketManager.
// Internal routine, must be wrapped inside connectionLock critical section
function TgoMongoProtocol.__RequestConnection: Boolean;
begin
  Assert(not Assigned(FConnection), 'A connection was already there!');
  result := False;
  try
    FConnection := FClientSocketManager.Request(FHost, FPort); //Request a connection from the pool
    result := Assigned(FConnection);
    if result then
    begin
      FConnection.OnConnected := SocketConnected;
      FConnection.OnDisconnected := SocketDisconnected;
      FConnection.OnRecv := SocketRecv;
    end; // ELSE we are shutting down
  except
    //Exceptions won't happen but we trap them anyway.
  end;
end;

// __DisposeConnection(): Disconnects and frees the connection. fConnection is NIL afterwards.
// Internal routine, must be wrapped inside fConnectionLock critical section

procedure TgoMongoProtocol.__DisposeConnection;
begin
  try
    if Assigned(FConnection) then
    begin
      FConnection.StopCallbacks; //its destructor does not do this unfortunately
      FConnection.Free; //also does PostDisconnect!
    end;
  except
    //Exceptions won't happen but we trap them anyway.
  end;
  FConnection := nil;
end;

// __RecycleConnection(): if the socket is connected it is returned to the client socket manager,
// otherwise it is destroyed. fConnection is NIL afterwards.
// Internal routine, must be wrapped inside fConnectionLock critical section

procedure TgoMongoProtocol.__RecycleConnection;
begin
  try
    if Assigned(FConnection) then
    begin
      if FConnection.State = TgoConnectionState.Connected then
         FClientSocketManager.Release(FConnection) //This also stops all callbacks
      else __DisposeConnection;
    end;
  except
    //Exceptions won't happen but we trap them anyway.
  end;
  FConnection := nil;
end;


// __Connected(): returns the connection status.
// Internal routine, must be wrapped inside fConnectionLock critical section

function TgoMongoProtocol.__Connected: Boolean;
begin
    if (FConnection <> nil) then
      result := (FConnection.State = TgoConnectionState.Connected) //just a flag
    else
      result := false;
end;

// __ConnectSocket:
// Internal routine, must be wrapped inside fConnectionLock critical section
// If the socket is already connected, nothing is done, otherwise it tries
// to connect the socket to the server

function TgoMongoProtocol.__ConnectSocket: Boolean;

  procedure WaitForConnected;
  var
    aNow: tStopWatch;
  begin
    aNow := ThisMoment;
    repeat
        if __connected() then exit;
    until (aNow.ElapsedMilliseconds > FSettings.ConnectionTimeout)
  end;

begin
  Assert(Assigned(FConnection), 'There is no connection object to work with.');
  result := __Connected;

  if not result then
  begin
    { Enable or disable Tls support }
    FConnection.SSL := FSettings.Secure;

    { Pass host name for Server Name Indication (SNI) for Tls }
    if FConnection.SSL then
    begin
      FConnection.OpenSSL.Host := FHost;
      FConnection.OpenSSL.Port := FPort;
    end;

    { Apply X.509 certificate }
    FConnection.Certificate := FSettings.Certificate;
    FConnection.PrivateKey := FSettings.PrivateKey;
    FConnection.Password := FSettings.PrivateKeyPassword;

    if FConnection.Connect then
      WaitForConnected;

    result := __Connected;
  end; //if
end;

function TgoMongoProtocol.saslStart(const APayload: string): IgoMongoReply;
var
  Writer: IgoBsonWriter;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('saslStart', 1);
  Writer.WriteString('$db', FSettings.AuthDatabase);
  if FSettings.AuthMechanism = TgoMongoAuthMechanism.SCRAM_SHA_1 then
    Writer.WriteString('mechanism', 'SCRAM-SHA-1')
  else
    Writer.WriteString('mechanism', 'SCRAM-SHA-256');
  Writer.WriteName('payload');
  Writer.WriteBinaryData(TgoBsonBinaryData.Create(TEncoding.Utf8.GetBytes(APayload)));
  Writer.WriteInt32('autoAuthorize', 1);
  Writer.WriteEndDocument;
  result := OpMsg(Writer.ToBson, nil, False, max(ReplyTimeout, 5000));
  //a missing response would close the socket and throw an exception.
end;

function TgoMongoProtocol.saslContinue(const AConversationId: Integer; const APayload: string): IgoMongoReply;
var
  Writer: IgoBsonWriter;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('saslContinue', 1);
  Writer.WriteInt32('conversationId', AConversationId);
  Writer.WriteString('$db', FSettings.AuthDatabase);
  Writer.WriteName('payload');
  Writer.WriteBinaryData(TgoBsonBinaryData.Create(TEncoding.Utf8.GetBytes(APayload)));
  Writer.WriteEndDocument;
  result := OpMsg(Writer.ToBson, nil, False, max(ReplyTimeout, 5000)) ;
  //a missing response would close the socket and throw an exception.
end;

//Authenticate()
//Returns true if authentication succeeded.
//Exceptions are possible if the socket breaks.

function TgoMongoProtocol.Authenticate: Boolean;
var
  Scram: TgoScram;
  ServerFirstMsg, ServerSecondMsg: string;
  ConversationDoc: TgoBsonDocument;
  PayloadBinary: TgoBsonBinaryData;
  ConversationId: Integer;
  Ok: Boolean;
  MongoReply: IgoMongoReply;
begin
  if FSettings.AuthMechanism = TgoMongoAuthMechanism.None then
    Exit(True)
  else
  begin
    { Reset auth error code }
    FAuthErrorMessage := '';
    FAuthErrorCode := 0;

    { Initialize our Scram helper }
    case FSettings.AuthMechanism of
      TgoMongoAuthMechanism.SCRAM_SHA_1:
        Scram := TgoScram.Create(TgoScramMechanism.SCRAM_SHA_1, FSettings.Username, FSettings.Password);
    else
      Scram := TgoScram.Create(TgoScramMechanism.SCRAM_SHA_256, FSettings.Username, FSettings.Password);
    end;

    try
      { Step 1 }
      Scram.CreateFirstMsg;

      { Start the initial sasl handshake }
      MongoReply := saslStart(SCRAM_GS2_HEADER + Scram.ClientFirstMsg);

      if MongoReply = nil then
        Exit(False);
      ConversationDoc := MongoReply.FirstDoc;
      if ConversationDoc.IsNil then
        Exit(False);

      Ok := ConversationDoc['ok'];
      if not Ok then
      begin
        // {
        // "ok" : 0.0,
        // "errmsg" : "Authentication failed.",
        // "code" : 18,
        // "codeName" : "AuthenticationFailed"
        // }
        FAuthErrorMessage := ConversationDoc['errmsg'];
        FAuthErrorCode := ConversationDoc['code'];
        Exit(False);
      end;

      // {
      // "conversationId" : 1,
      // "done" : false,
      // "payload" : { "$binary" : "a=b,c=d", "$type" : "00" },
      // "ok" : 1.0
      // }
      { The first message from the server to the client }
      PayloadBinary := ConversationDoc['payload'].AsBsonBinaryData;
      ServerFirstMsg := TEncoding.Utf8.GetString(PayloadBinary.AsBytes);
      ConversationId := ConversationDoc['conversationId'];

      { Process the first message from the server to the client }
      Scram.HandleServerFirstMsg(ConversationId, ServerFirstMsg);

      { Step 2 - Send the final client message }
      MongoReply := saslContinue(Scram.ConversationId, Scram.ClientFinalMsg);

      if MongoReply = nil then
        Exit(False);
      ConversationDoc := MongoReply.FirstDoc;
      if ConversationDoc.IsNil then
        Exit(False);

      Ok := ConversationDoc['ok'];
      if not Ok then
      begin
        FAuthErrorMessage := ConversationDoc['errmsg'];
        FAuthErrorCode := ConversationDoc['code'];
        Exit(False);
      end;

      { The second message from the server to the client }
      PayloadBinary := ConversationDoc['payload'].AsBsonBinaryData;
      ServerSecondMsg := TEncoding.Utf8.GetString(PayloadBinary.AsBytes);

      { Process the second message from the server to the client }
      Scram.HandleServerSecondMsg(ServerSecondMsg);

      { Verify that the actual signature matches the servers expected signature }
      if not Scram.ValidSignature then
      begin
        FAuthErrorMessage := 'Server signature does not match';
        FAuthErrorCode := -1;
        Exit(False);
      end;

      { Step 3 - Acknowledge with an empty payload }
      MongoReply := saslContinue(Scram.ConversationId, '');
      if MongoReply = nil then
        Exit(False);
      ConversationDoc := MongoReply.FirstDoc;
      if ConversationDoc.IsNil then
        Exit(False);

      Ok := ConversationDoc['ok'];
      if not Ok then
      begin
        FAuthErrorMessage := ConversationDoc['errmsg'];
        FAuthErrorCode := ConversationDoc['code'];
        Exit(False);
      end;

      result := (ConversationDoc['done'] = True);
    finally
      Scram.Free;
    end;
  end;
end;

{NegotiateProtocol()
 The "Hello" tells the server what our capabilities are and queries
 the server's capabilities. This is where we determine if the server
 supports compression etc.}

procedure TgoMongoProtocol.NegotiateProtocol;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  Doc: TgoBsonDocument;
  Compressions: tgoBsonArray;
  Value: tgobsonvalue;
  debug: string;
  I: Integer;
begin
  try
    Writer := TgoBsonWriter.Create;

    // ***************** main doc {
    Writer.WriteStartDocument;
    Writer.WriteInt32('hello', 1);
    Writer.WriteString('$db', DB_ADMIN);

    // client {
    Writer.WriteStartDocument('client');
    if FSettings.ApplicationName <> '' then
    begin
      // application {
      Writer.WriteStartDocument('application');
      Writer.WriteString('name', FSettings.ApplicationName);
      Writer.WriteEndDocument; // application}
    end;

    // driver {
    Writer.WriteStartDocument('driver');
    Writer.WriteString('name', 'Grijjy for Delphi/modified');
    Writer.WriteString('version', '2.0 beta');
    Writer.WriteEndDocument; // driver}

    // os {
    Writer.WriteStartDocument('os');
    case tosVersion.platform of
      tosVersion.tplatform.pfwindows:
        Writer.WriteString('type', 'Windows');
      tosVersion.tplatform.pfMacOS:
        Writer.WriteString('type', 'Darwin');
    else
      Writer.WriteString('type', 'Linux');
    end;
    Writer.WriteString('name', tosVersion.Name);
    case tosVersion.architecture of
      tosVersion.tarchitecture.arIntelX86:
        Writer.WriteString('architecture', 'x86');
      tosVersion.tarchitecture.arIntelX64:
        Writer.WriteString('architecture', 'x86_64');
      tosVersion.tarchitecture.arArm32:
        Writer.WriteString('architecture', 'arm');
    else
      Writer.WriteString('architecture', 'arm64');
    end;
    Writer.WriteString('version', Format('%d.%d.%d', [tosVersion.Major, tosVersion.Minor, tosVersion.Build]));
    Writer.WriteEndDocument; // os}
    Writer.WriteString('platform', 'Delphi');
    Writer.WriteEndDocument; // client}

    if (FSettings.UseSnappyCompression and Snappy_Implemented) or (FSettings.UseZlibCompression and ZLIB_Implemented) then
    begin
      Writer.WriteStartarray('compression');
      if (FSettings.UseSnappyCompression and Snappy_Implemented) then
        Writer.WriteString('snappy');
      if (FSettings.UseZlibCompression and ZLIB_Implemented) then
        Writer.WriteString('zlib');
      Writer.WriteEndArray { compression };
      Writer.WriteEndDocument; // main doc}
    end;

    Reply := OpMsg(Writer.ToBson, nil, False, max(ReplyTimeout, 5000));

    if Assigned(Reply) then
    begin
      Doc := Reply.FirstDoc;
      if not Doc.IsNil then
      begin
        debug := Doc.ToJson;


        fSupportsReplication := Doc.Contains('setName');


        if Doc.Contains('maxWireVersion') then
          FMaxWireVersion := Doc['maxWireVersion'].AsInteger;
        if Doc.Contains('minWireVersion') then
          FMinWireVersion := Doc['minWireVersion'].AsInteger;
        if Doc.Contains('MaxWriteBatchSize') then
          FMaxWriteBatchSize := max(Doc['maxWriteBatchSize'].AsInteger, DEF_MAX_BULK_SIZE);
        if Doc.Contains('maxMessageSizeBytes') then
          FMaxMessageSizeBytes := max(Doc['maxMessageSizeBytes'].AsInteger, DEF_MAX_MSG_SIZE);

        if Doc.Contains('compression') then
        begin
          Compressions := Doc['compression'].AsBsonArray;
          for I := 0 to Compressions.Count - 1 do
          begin
            Value := Compressions[I];
            if Value.IsString then
            begin
              if Value.AsString = 'snappy' then
                fServerknowsSnappy := True;
              if Value.AsString = 'zlib' then
                fServerknowsZlib := True;
            end;
          end;
        end;
      end;
    end;
  except
    // ignore exceptions
  end;
end;

class function tMsgPayload.ReadBsonDoc(TestMode: Boolean; out Bson: tBytes; buffer: Pointer; BytesAvail: Integer; var BytesRead: Integer):
  tgoPayloadDecodeResult;
var
  DocSize: Integer;
begin
  BytesRead := 0;
  SetLength(Bson, 0);
  result := tgoPayloadDecodeResult.pdEOF; // Assume not enough bytes for minimal bson document
  if BytesAvail >= EmptyDocSize then
  begin
    move(buffer^, DocSize, sizeof(Integer)); // read size of bson document (includes docsize itself)
    if (BytesAvail >= DocSize) and (DocSize >= EmptyDocSize) then // buffer is big enough?
    begin
      result := tgoPayloadDecodeResult.pdOK; // OK
      BytesRead := DocSize;
      if not TestMode then
      begin
        SetLength(Bson, DocSize);
        move(buffer^, Bson[0], DocSize);
      end;
    end
    else // we'd read beyond the end of the buffer, or docsize is invalid
    begin
      result := tgoPayloadDecodeResult.pdBufferOverrun;
    end;
  end;
end;

{ https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#command-arguments-as-payload
  Any unknown Payload Types MUST result in an error and the socket MUST be closed.
  !!! There is no ordering implied by payload types.!!!
  !!! A section with payload type 1 can be serialized before payload type 0!!! }

class function tMsgPayload.DecodeSequence(TestMode: Boolean; SeqStart: Pointer; SizeAvail: Integer; var SizeRead: Integer; var PayloadType:
  Byte; var Name: string; var data: TArray<tBytes>): tgoPayloadDecodeResult;

{ SeqStart: Start of the section, first byte that follows is the PayloadType
  SizeAvail: Available size from SeqStart to the end of the buffer
  if result=True:
  -> SizeRead:  the size of the processed section.
  -> PayloadType: the decoded payload type
  -> Name: The name of the payload, if PayloadType=1
  -> Data: An array of BSON docs.
}
var
  PayloadSize, offs, PayloadStart: Integer;
  c: ansichar;
  Cstring: utf8string;
  tempresult: tgoPayloadDecodeResult;

  function cursor: Pointer;
  begin
    result := Pointer(intptr(SeqStart) + offs);
  end;

  procedure read(var Output; bytes: Integer); // Simulate simple memory stream
  begin
    move(cursor^, Output, bytes);
    inc(offs, bytes);
  end;

  procedure peek(var Output; bytes: Integer); // Simulate simple memory stream
  var
    startoffs: Integer;
  begin
    startoffs := offs;
    read(Output, bytes);
    offs := startoffs;
  end;

  function BufLeft: Integer;
  begin
    result := SizeAvail - offs;
  end;

  function PayloadLeft: Integer;
  var
    PayloadProcessed: Integer;
  begin
    PayloadProcessed := offs - PayloadStart;
    result := PayloadSize - PayloadProcessed;
  end;

  function AppendDoc(): tgoPayloadDecodeResult;
  var
    tb: tBytes;
    bRead: Integer;
  begin
    result := ReadBsonDoc(TestMode, tb, cursor, PayloadLeft, bRead);
    if result = tgoPayloadDecodeResult.pdOK then
    begin
      if not TestMode then
      begin
        SetLength(data, length(data) + 1);
        data[high(data)] := tb;
      end;
      inc(offs, bRead); // acknowledge read
    end;
  end;

begin
  result := tgoPayloadDecodeResult.pdEOF;
  Name := '';
  offs := 0;
  SizeRead := 0;
  SetLength(data, 0);
  Cstring := '';
  PayloadSize := 0;

  // minimum size for decoding is 6 Bytes (payloadtype + empty bson doc). When we reach the end of the message
  // there may be a CRC which is only 4 bytes. Decoding stops there.

  if BufLeft >= MinSequenceSize then
  begin
    read(PayloadType, 1); // Read the payload type
    PayloadStart := offs; // to facilitate function "PayloadLeft"

    if (PayloadType in [0, 1]) then // disallow other payload types
    begin
      peek(PayloadSize, sizeof(PayloadSize)); // Peek the payload size counter

      if (BufLeft >= PayloadSize) and (PayloadSize >= 0) then // Avoid buffer overrun
      begin
        case PayloadType of

          0: // Type 0: contains ONE BSON doc
            result := AppendDoc();

          1: // Type 1: payload with string header, then zero or more BSON docs
            begin
              inc(offs, sizeof(Integer)); // jump over payload size
              // Read string header - probably just ascii, but allow utf8 anyway
              while PayloadLeft > 0 do
              begin
                read(c, 1);
                if c = #0 then
                  Break;
                SetLength(Cstring, length(Cstring) + 1); // dumb append of byte
                Cstring[length(Cstring)] := c;
              end; // while
              Name := string(Cstring);

              result := tgoPayloadDecodeResult.pdOK; // the specs say "0 or more" BSON documents, so 0 is acceptable

              // pull in as many docs as possible
              while PayloadLeft > 0 do
              begin
                tempresult := AppendDoc();
                case tempresult of
                  tgoPayloadDecodeResult.pdOK:
                    Continue; // OK, potentially more documents
                  tgoPayloadDecodeResult.pdEOF:
                    Break; // no more documents, ready
                  tgoPayloadDecodeResult.pdBufferOverrun: // Error
                    begin
                      result := tempresult; // invalidate whole result
                      Break;
                    end;
                else // can't occur
                  Break;
                end;
              end; // while
            end; // case 1
        end; // case
      end // if  bufleft OK
      else if (PayloadSize < 0) or (BufLeft < PayloadSize) then
        result := tgoPayloadDecodeResult.pdBufferOverrun;
    end // if PayloadType OK
    else
      result := tgoPayloadDecodeResult.pdInvalidPayloadType; // unknown PayloadType
  end; // if bufleft

  if result = tgoPayloadDecodeResult.pdOK then
    SizeRead := sizeof(Byte) + PayloadSize; // Should be identical with offs
end;

function TgoMongoProtocol.CanUseCompression: Boolean;
begin
  result := (FSettings.UseSnappyCompression and Snappy_Implemented and fServerknowsSnappy) or //
  (FSettings.UseZlibCompression and ZLIB_Implemented and fServerknowsZlib);
end;

procedure TgoMongoProtocol.Compress(var Packet: tBytes);
var
  h: PMsgHeader;
  q: POPCompressedHeader;
  DataSize, compressedsize: Integer;
  Output: tBytes;
begin
  if length(Packet) > 0 then
  begin
    h := @Packet[0];
    DataSize := h.DataSize;
    if fServerknowsSnappy then
    begin
      if tSnappycompressor.Compress(h.DataStart, DataSize, compressedsize, Output) then
      begin
        SetLength(Packet, sizeof(TOPCompressedHeader) + length(Output));
        q := @Packet[0];
        q.OriginalOpCode := q.Header.OpCode;
        q.Header.OpCode := OP_COMPRESSED;
        q.Header.MessageLength := length(Packet);
        q.UncompressedSize := DataSize;
        q.CompressorID := CompressorID_Snappy;
        move(Output[0], q.DataStart^, length(Output));
      end;
    end
    else if fServerknowsZlib then
    begin
      if tZlibCompressor.Compress(h.DataStart, DataSize, compressedsize, Output) then
      begin
        SetLength(Packet, sizeof(TOPCompressedHeader) + length(Output));
        q := @Packet[0];
        q.OriginalOpCode := q.Header.OpCode;
        q.Header.OpCode := OP_COMPRESSED;
        q.Header.MessageLength := length(Packet);
        q.UncompressedSize := DataSize;
        q.CompressorID := CompressorID_Zlib;
        move(Output[0], q.DataStart^, length(Output));
      end;
    end;
    //Else do nothing.
  end;
end;

function TgoMongoProtocol.LastPartialReply(const ARequestId: Integer; out ALastRecv: tStopWatch): Boolean;
begin
  FRepliesLock.Acquire;
  try
    result := FPartialReplies.TryGetValue(ARequestId, ALastRecv);
  finally
    FRepliesLock.Release;
  end;
end;

function TgoMongoProtocol.ThisMoment: tStopWatch;
begin
  result := tStopWatch.StartNew;
end;

(* OpMsg remarks:

 Normally NoResponse = false:  We DO expect a response after a request and we wait max. aTimeoutMS seconds for it.

 The exceptions are the following, but they haven't been implemented yet :

 - OP_MSG with moretocome flag set (the message is part of a stream of messages)
 - Fire-and-forget operations, such as unacknowledged writes  -->  writeconcern: {w: 0}

 Note: if you set noResponse to true and the server returns a response anyway, the response
 lands in fCompletedReplies and stays there indefinitely...
*)

function TgoMongoProtocol.OpMsg(const ParamType0: tBytes; const ParamsType1: TArray<tgoPayloadType1>; NoResponse: Boolean; aTimeoutMS:
  Integer): IgoMongoReply;
var
  MsgHeader: TOPMSGHeader;
  pHeader: POpMsgHeader;
  data: tgoByteBuffer;
  I, RequestID: Integer;
  T: tBytes;
  paramtype: Byte;
  ExpectResponse: Boolean;
begin
  if length(ParamType0) = 0 then
    raise EgoMongoDBError.Create('Mandatory document of PayloadType 0 missing in OpMsg');
  ExpectResponse := not NoResponse;

  if EnsureConnected() then //this performs autoconnect if possible
  begin
    RequestID := atomicincrement(FNextRequestId); //rolls over after 2 billion
    MsgHeader.Header.RequestID := RequestID;
    MsgHeader.Header.ResponseTo := 0;
    MsgHeader.Header.OpCode := OP_MSG;
    if NoResponse then
      MsgHeader.flagbits := [TGoMongoMsgFlag.msgfMoreToCome]
    else
      MsgHeader.flagbits := [];

    if ExpectResponse then
    begin
      fReplyEvent.ResetEvent;
      FRepliesLock.Enter;
      fReplyEvents.Add(RequestID, fReplyEvent); //register the event for this ID
      FCompletedReplies.Remove(RequestID); // ... just in case some old response with this ID was still lingering (should not happen)
      FRepliesLock.Leave;
    end;

    try
      data := tgoByteBuffer.Create;
      try
        data.AppendBuffer(MsgHeader, sizeof(MsgHeader));
        // Append section of PayloadType 0, that contains the first document.
        // Every op_msg MUST have ONE section of payload type 0.
        // this is the standard command document, like {"insert": "collection"},
        // plus write concern and other command arguments.

        paramtype := 0;
        data.Append(paramtype);
        data.Append(ParamType0);

        // Some parameters may be dis-embedded from the first document and simply appended as sections of Payload Type 1,
        // see https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#command-arguments-as-payload

        for I := 0 to high(ParamsType1) do
          ParamsType1[I].WriteTo(data);

        { TODO : optional Checksum }

        // update message length in header
        pHeader := @data.buffer[0];
        pHeader.Header.MessageLength := data.Size;
        T := data.ToBytes;

        //Compression also implies that the ANSWER will be compressed by the server !

        if CanUseCompression then
          Compress(T);

        Send(T); //-> on error: socket will be destroyed + exception

      finally
        FreeAndNil(data);
      end;

      if ExpectResponse then
        result := WaitForReply(RequestID, fReplyEvent, aTimeoutMS) //-> on timeout: socket will be destroyed + exception
      else
        result := nil;
    finally
      if ExpectResponse then
      begin
        FRepliesLock.Enter;
        fReplyEvents.Remove(RequestID);
        FRepliesLock.Leave;
      end;
    end;
  end
  else
    ConnectionFailedException; //if NOT ensureconnected()
end;

function TgoMongoProtocol.HaveReplyMsgHeader(out AMsgHeader): Boolean;
begin
  result := HaveReplyMsgHeader(AMsgHeader, FRecvBuffer, FRecvSize);
end;

function TgoMongoProtocol.HaveReplyMsgHeader(out AMsgHeader; tb: tBytes; Size: Integer): Boolean;
begin
  result := (Size >= sizeof(TMsgHeader));
  if (result) then
  begin
    move(tb[0], AMsgHeader, sizeof(TMsgHeader));
    result := TMsgHeader(AMsgHeader).ValidOpcode;
  end;
end;

procedure TgoMongoProtocol.PrepareForReuse;
begin
  ClearReplies;
end;



procedure TgoMongoProtocol.Send(const adata: tBytes); //Send() is ONLY called from OP_MSG
var
  Success: Boolean;
begin
  Success := False;
  FConnectionLock.Acquire;
  try
    if (FConnection <> nil) then
      Success := FConnection.Send(adata); //closes socket on error
  finally
    if not Success then
      __DisposeConnection;
    FConnectionLock.Release;
    if not Success then
      ConnectionFailedException;
  end;
end;

procedure TgoMongoProtocol.SocketConnected;
begin
  { Not interested (yet) }
end;

procedure TgoMongoProtocol.SocketDisconnected;
begin
  { Not interested (yet) }
end;

function TgoMongoProtocol.TryGetReply(const ARequestId: Integer; out AReply: IgoMongoReply): Boolean;
begin
  FRepliesLock.Acquire;
  try
    result := FCompletedReplies.TryGetValue(ARequestId, AReply);
  finally
    FRepliesLock.Release;
  end;
end;

function TgoMongoProtocol.WaitForReply(const ARequestId: Integer; const aReplyEvent: TEvent; aTimeoutMS: Integer = 0): IgoMongoReply;
var
  LastRecv: tStopWatch;
  TimeLeft: Int64;
begin
  result := nil;
  if (aTimeoutMS <= 0) then
    aTimeoutMS := max(FSettings.ReplyTimeout, 500);
  TimeLeft := aTimeoutMS;
  while GetConnected() do
  begin
    aReplyEvent.WaitFor(TimeLeft);

    if TryGetReply(ARequestId, result) then
      Break //Normal situation, Success! Result=Reply
    else
      TimeLeft := 0; //Timeout

    // check if a partial reply was received or updated.
    // If yes, update TimeLeft.

    if (TimeLeft <= 0) then
    begin
      // if no partial reply received, give up.
      if not LastPartialReply(ARequestId, LastRecv) then
        Break // give up (result=NIL)
      else
      begin
        //partial reply found. How long ago was it updated?
        TimeLeft := Int64(aTimeoutMS) - LastRecv.ElapsedMilliseconds;
        if TimeLeft <= 0 then //radio-silence too long?
          Break //give up (result=NIL)
        else
          TimeLeft := max(TimeLeft, 10); //just in case bandwidth is very low and data trickles in
      end;
    end;
  end; //while

  RemoveReply(ARequestId);
  if (result = nil) then
    Recover; // There could be trash in the input buffer, blocking the system
end;

procedure TgoMongoProtocol.Recover;
var
  MsgHeader: TMsgHeader;
begin
  { remove trash from the reception buffer }
  FRecvBufferLock.Enter;
  try
    if (FRecvSize > 0) then
    begin
      // if it begins with a valid response header, remove its statistics
      if HaveReplyMsgHeader(MsgHeader) then
        RemoveReply(MsgHeader.RequestID);
      // clear the buffer
      FRecvSize := 0;
    end;
  finally
    FRecvBufferLock.Leave;
  end;
end;

function TgoMongoProtocol.EnsureCapacity(CapacityNeeded: Integer): Boolean;
var
  Size: Integer;
begin
  result := True;
  Size := length(FRecvBuffer);
  if Size < CapacityNeeded then
  begin
    try
      while Size < CapacityNeeded do
        Size := Size + RECV_BUFFER_SIZE; //chunks of 128 K, avoid excessive realloc
      SetLength(FRecvBuffer, Size);
    except
      // Out Of Memory, cannot do reallocmem
      result := False;
      SetLength(FRecvBuffer, 0);
      SetLength(FRecvBuffer, RECV_BUFFER_SIZE);
      FRecvSize := 0;
    end;
  end;
end;


class function TgoMongoProtocol.IsInternalError(const errorcode: Integer): Boolean;
begin
  case errorcode of
    146, 147, 301:
      result := True;
  else
    result := False;
  end;
end;

procedure TgoMongoProtocol.SocketRecv(const ABuffer: Pointer; const ASize: Integer);
{ remove the processed bytes from the buffer }

  procedure ClearBuffer;
  begin
    FRecvSize := 0;
  end;

  procedure RemoveBytes(NumBytes: Integer);
  var
    BytesLeft: Integer;
  begin
    if (NumBytes >= FRecvSize) then
      ClearBuffer
    else
    begin
      BytesLeft := FRecvSize - NumBytes;
      if BytesLeft > 0 then // should always be true
      begin
        move(FRecvBuffer[NumBytes], FRecvBuffer[0], BytesLeft);
        FRecvSize := BytesLeft;
      end
      else
        ClearBuffer;
    end;
  end;

  procedure QueueReply(AReply: IgoMongoReply);
  var
    Waiter: TEvent;
    ID: Integer;
  begin
    ID := AReply.ResponseTo;
    Waiter := nil;
    FRepliesLock.Acquire;
    try
      FPartialReplies.Remove(ID); //no longer needed
      FCompletedReplies.AddOrSetValue(ID, AReply); //Add the completed reply to the dictionary.
      fReplyEvents.TryGetValue(ID, Waiter);
    finally
      FRepliesLock.Release;
      if Assigned(Waiter) then //if a tevent is waiting for this reply, fire it!
        Waiter.SetEvent;
    end;
  end;

  procedure ReportError(aResponseTo, aErrorcode: Integer; aErrorText, aErrorMnemonic: string);
  var
    MsgHeader: TMsgHeader;
  begin
    // Is there at least a partial reply in the input buffer, so we know the ID of the
    // request that this was a response to ?
    QueueReply(TgoMongoMsgReply.CreateFromError(aResponseTo, aErrorcode, aErrorText, aErrorMnemonic));
    ClearBuffer; //totally discard input buffer
  end;

var
  MongoReply: IgoMongoReply;
  ProcessedBytes, ID: Integer;
  MsgHeader: TMsgHeader;
  Validation: tgoReplyValidationResult;
  HaveHeader: Boolean;
begin
  ID := 0;
  try
    FRecvBufferLock.Enter;
    try
      HaveHeader := HaveReplyMsgHeader(MsgHeader);
      if HaveHeader then
        ID := MsgHeader.ResponseTo;

      if EnsureCapacity(FRecvSize + ASize) then
      begin
        { buffer the new data }
        move(ABuffer^, FRecvBuffer[FRecvSize], ASize);
        FRecvSize := FRecvSize + ASize;
      end
      else
      begin
        { If at least the header is complete, post an "out of memory" reply so op_msg can react accordingly. }
        if HaveHeader then
          ReportError(ID, 146, 'Buffer exceeded Memory Limit', 'ExceededMemoryLimit');
        ClearBuffer;
        Exit; // --> finally
      end;

      { Is there one or more valid replies pending? }
      repeat
        Validation := TgoMongoMsgReply.ValidateMessage(FRecvBuffer, FRecvSize, ProcessedBytes, MongoReply);
        HaveHeader := HaveReplyMsgHeader(MsgHeader);
        if HaveHeader then
          ID := MsgHeader.ResponseTo;

        case Validation of

          tgoReplyValidationResult.rvrOK:
            begin
              RemoveBytes(ProcessedBytes);
              QueueReply(MongoReply);
              //Continue just in case the server sent multiple replies
            end;

          tgoReplyValidationResult.rvrNoHeader: //Not enough bytes in buffer to do anything.
            Break; // --> finally

          tgoReplyValidationResult.rvrGrowing:
            begin
              // header opcode is valid but message still growing/incomplete
              // Update the partial reply timestamp
              if HaveHeader then
                UpdateReplyTimeout(ID);
              Break; // --> finally
            end;

          tgoReplyValidationResult.rvrOpcodeInvalid:
            begin
              // TRASH in buffer: whatever is at the start of the buffer is not a valid header
              // We can't post an error message. Opmsg() will timeout.
              ClearBuffer;
              Break; // --> finally
            end;

          tgoReplyValidationResult.rvrCompressorError, tgoReplyValidationResult.rvrCompressorNotSupported:
            begin
              if HaveHeader then
                ReportError(ID, 147, 'Compressor: Expansion error or compressor not supported', 'ZLibError');
              ClearBuffer;
              Break; // --> finally
            end;

          tgoReplyValidationResult.rvrDataError, tgoReplyValidationResult.rvrChecksumInvalid:
            begin
              if HaveHeader then
                ReportError(ID, 301, 'Data Corruption Detected', 'DataCorruptionDetected');
              ClearBuffer;
              Break; // --> finally
            end;
        else
          Break;
        end; // case
      until False;
    finally
      FRecvBufferLock.Leave;
    end;
  except
    //No exceptions should escape - it is a socket event handler that runs in a background thread
  end;
end;

procedure TgoMongoProtocol.UpdateReplyTimeout(const ARequestId: Integer);
begin
  FRepliesLock.Acquire;
  try
    FPartialReplies.AddOrSetValue(ARequestId, ThisMoment);
  finally
    FRepliesLock.Release;
  end;
end;

procedure TgoMongoProtocol.RemoveReply(const ARequestId: Integer);
begin
  FRepliesLock.Acquire;
  try
    FPartialReplies.Remove(ARequestId);
    FCompletedReplies.Remove(ARequestId);
  finally
    FRepliesLock.Release;
  end;
end;

function TgoMongoProtocol.getRecycleSocket: Boolean;
begin
  Result := fRecycleSocket;
end;


function TgoMongoProtocol.SupportsReplication: Boolean;
begin
  Result:=EnsureConnected() And fSupportsReplication;
end;

function TgoMongoProtocol.SupportsTransactions: Boolean;
begin
 Result:=SupportsReplication();
end;

procedure TgoMongoProtocol.setRecycleSocket(const Value: Boolean);
begin
  fRecycleSocket := Value;
end;

function TMsgHeader.Compressed: Boolean;
begin
  result := (self.OpCode = OP_COMPRESSED);
end;

function TMsgHeader.DataSize: Integer;
begin
  result := MessageLength - sizeof(self);
end;

function TMsgHeader.DataStart: Pointer;
begin
  result := Pointer(nativeuint(@self) + sizeof(self));
end;

function TOPCompressedHeader.CompressedDataSize: Integer;
begin
  result := Header.MessageLength - sizeof(self);
end;

function TOPCompressedHeader.DataStart: Pointer;
begin
  result := Pointer(nativeuint(@self) + sizeof(self));
end;

function TOPCompressedHeader.UnCompressedDataSize: Integer;
begin
  result := UncompressedSize;
end;

function TOPCompressedHeader.UnCompressedMessageSize: Integer;
begin
  result := UnCompressedDataSize + sizeof(Header);
end;

function TMsgHeader.ValidOpcode: Boolean;
begin
  { VERY basic format detection, but better than nothing }
  result := (self.OpCode = OP_MSG) or (self.OpCode = OP_COMPRESSED);
end;

{ TgoMongoMsgReply }

constructor TgoMongoMsgReply.CreateFromError(aReplyTo, aCode: Integer; aErrMsg, aCodeName: string);
begin
  inherited Create;
  FHeader.Header.ResponseTo := aReplyTo;
  FHeader.Header.OpCode := OP_MSG;
  //The other fields in fHeader are irrelevant for opmsg() and HandleCommandReply()

  FFirstDoc := TgoBsonDocument.Create;
  if aCode <> 0 then
  begin
    FFirstDoc['ok'] := 0; //indicates error
    FFirstDoc['code'] := aCode; //the error code
    if aErrMsg <> '' then
      FFirstDoc['errmsg'] := aErrMsg; //the error message
    if aCodeName <> '' then
      FFirstDoc['codeName'] := aCodeName; //the error mnemonic
  end
  else
    FFirstDoc['ok'] := 1; //indicates no error

  self.FPayload0 := self.FPayload0 + [FFirstDoc.ToBson]; //redundant
end;

// Read data from a previously validated data buffer
procedure TgoMongoMsgReply.ReadData(const ABuffer: Pointer; const ASize: Integer);
var
  I, k: Integer;
  DocBuf: TArray<tBytes>;
  data: Pointer;
  StartOfData, Avail, SizeRead, NewSize: Integer;
  PayloadType: Byte;
  seqname: string;
begin
  FFirstDoc.SetNil;
  if (ASize >= sizeof(TOPMSGHeader)) then
  begin
    move(ABuffer^, FHeader, sizeof(FHeader));
    // read the header
    StartOfData := sizeof(FHeader);

    data := Pointer(nativeuint(ABuffer) + nativeuint(StartOfData));

    Avail := FHeader.Header.MessageLength - StartOfData;
    while tMsgPayload.DecodeSequence(False, data, Avail, SizeRead, PayloadType, seqname, DocBuf) = tgoPayloadDecodeResult.pdOK do
    begin
      case PayloadType of
        0:
          begin
            k := length(FPayload0);
            NewSize := k + length(DocBuf);
            SetLength(FPayload0, NewSize);
            for I := 0 to high(DocBuf) do
              FPayload0[k + I] := DocBuf[I];
          end;

        1:
          begin
            SetLength(FPayload1, length(FPayload1) + 1);
            FPayload1[high(FPayload1)].Name := seqname;
            FPayload1[high(FPayload1)].Docs := DocBuf;
          end;
      end;

      Avail := Avail - SizeRead;
      inc(intptr(data), SizeRead);
    end;
  end;
end;

// Validate an UNCOMPRESSED op_msg packet. Create an igoMongoReply if the packet is valid.

class function TgoMongoMsgReply.ValidateOPMessage(const ABuffer: tBytes; const ASize: Integer; var aSizeRead: Integer; out AReply:
  IgoMongoReply): tgoReplyValidationResult;
var
  DocBuf: TArray<tBytes>;
  data: Pointer;
  StartOfData, Avail: Integer;
  PayloadType: Byte;
  seqname: string;
  pHeader: POpMsgHeader;
  SizeRead, Type0Docs: Integer;
  AllBytesRead, HasChecksum: Boolean;

  function ChecksumOK: Boolean;
  begin
    { TODO : Implement checksum check, there's a tCRC32 at the end of the message }
    result := True;
  end;

begin
  AReply := nil;
  SizeRead := 0;
  Type0Docs := 0;
  try
    if (ASize >= sizeof(TOPMSGHeader)) then
    begin
      pHeader := @ABuffer[0];
      if pHeader.Header.ValidOpcode then
      begin
        if ASize >= pHeader.Header.MessageLength then
        begin
          StartOfData := sizeof(TOPMSGHeader); // Data starts right after the header
          aSizeRead := StartOfData;
          data := @ABuffer[StartOfData];
          Avail := pHeader.Header.MessageLength - StartOfData;
          HasChecksum := TGoMongoMsgFlag.msgfChecksumPresent in pHeader.flagbits;

          if (not HasChecksum) or ChecksumOK() then // needs compiler switch $B-
          begin
            repeat
              case tMsgPayload.DecodeSequence(True, data, Avail, SizeRead, PayloadType, seqname, DocBuf) of
                tgoPayloadDecodeResult.pdOK:
                  begin
                    if PayloadType = 0 then
                      inc(Type0Docs);
                    Avail := Avail - SizeRead;
                    inc(intptr(data), SizeRead);
                    inc(aSizeRead, SizeRead);
                  end;

                tgoPayloadDecodeResult.pdEOF:
                  Break;

                tgoPayloadDecodeResult.pdInvalidPayloadType, tgoPayloadDecodeResult.pdBufferOverrun:
                  Exit(tgoReplyValidationResult.rvrDataError);

              end; // case
            until False;

            // packet MUST have ONE document of payload type 0
            if Type0Docs <> 1 then
              Exit(tgoReplyValidationResult.rvrDataError);

            // Does amount of data parsed match the message length of the header ?
            if (HasChecksum) then
            begin
              AllBytesRead := (aSizeRead = pHeader.Header.MessageLength - sizeof(tCRC32));
              if AllBytesRead then
                inc(aSizeRead, sizeof(tCRC32)); // jump over checksum
            end // if checksum
            else
              AllBytesRead := (aSizeRead = pHeader.Header.MessageLength);
            if AllBytesRead then
            begin
              result := tgoReplyValidationResult.rvrOK;
              AReply := TgoMongoMsgReply.Create(@ABuffer[0], aSizeRead);
            end
              // Message decodes OK. All is well.
            else
              result := tgoReplyValidationResult.rvrDataError; // Header opcode OK, message could be complete, but decoding fails
          end // if checksum OK
          else
            result := tgoReplyValidationResult.rvrChecksumInvalid; // Header opcode OK, message could be complete, CRC fails
        end // if aSize big enough for Data
        else
          result := tgoReplyValidationResult.rvrGrowing; // Header opcode OK, but message not complete yet
      end // if valid opcode
      else
        result := tgoReplyValidationResult.rvrOpcodeInvalid; // Invalid header, opcode unknown
    end // if enough bytes for a header
    else
      result := tgoReplyValidationResult.rvrNoHeader; // Buffer does not contain enough bytes for a header
  except
    result := tgoReplyValidationResult.rvrDataError; //could be OutOfMemory as well
    // no exceptions allowed to exit
  end;
end;

// Validate a OP_MSG packet, which may be either compressed or uncompressed.
// Decompress if necessary.
// Create a iGoMongoReply if the packet is valid.

class function TgoMongoMsgReply.ValidateMessage(const ABuffer: tBytes; const ASize: Integer; var aSizeRead: Integer; out AReply:
  IgoMongoReply): tgoReplyValidationResult;
var
  Source: POPCompressedHeader;
  Target: PMsgHeader;
  UnpackedMsg, Decompressed: tBytes;

begin
  AReply := nil;
  aSizeRead := 0;

  { Distinguish between compressed and uncompressed messages }

  if (ASize >= sizeof(TMsgHeader)) then
  begin
    Source := @ABuffer[0];
    if Source.Header.Compressed then // has a VALID op_compressed opcode
    begin
      if ASize < Source.Header.MessageLength then
        Exit(tgoReplyValidationResult.rvrGrowing); //Message is incomplete.

      if Source.CompressorID > CompressorID_Highest then
        Exit(tgoReplyValidationResult.rvrCompressorNotSupported);

      SetLength(UnpackedMsg, Source.UnCompressedMessageSize);

      // Prepare the "uncompressed" header
      Target := @UnpackedMsg[0];
      Target.MessageLength := Source.UnCompressedMessageSize;
      Target.RequestID := Source.Header.RequestID;
      Target.ResponseTo := Source.Header.ResponseTo;
      Target.OpCode := OP_MSG;

      case Source.CompressorID of

        0: { noop - never executed}
          begin
            if not tNoopCompressor.Expand(Source.DataStart, Source.CompressedDataSize, Source.UnCompressedDataSize, Decompressed) then
              Exit(tgoReplyValidationResult.rvrCompressorError)
            else
              move(Decompressed[0], Target.DataStart^, length(Decompressed));
          end; //case none

        CompressorID_Snappy:
          begin
            if not tSnappycompressor.Expand(Source.DataStart, Source.CompressedDataSize, Source.UnCompressedDataSize, Decompressed) then
              Exit(tgoReplyValidationResult.rvrCompressorError)
            else
              move(Decompressed[0], Target.DataStart^, length(Decompressed));
          end; // case snappy

        CompressorID_Zlib:
          begin
            if not tZlibCompressor.Expand(Source.DataStart, Source.CompressedDataSize, Source.UnCompressedDataSize, Decompressed) then
              Exit(tgoReplyValidationResult.rvrCompressorError)
            else
              move(Decompressed[0], Target.DataStart^, length(Decompressed));
          end; // case zlib
      end; //case

      result := ValidateOPMessage(UnpackedMsg, Source.UnCompressedMessageSize, aSizeRead, AReply);

      if result = tgoReplyValidationResult.rvrOK then
        aSizeRead := Source.Header.MessageLength; // Bytes to discard!
    end // if compressed
    else
      result := ValidateOPMessage(ABuffer, ASize, aSizeRead, AReply); // uncompressed or TRASH
  end
  else
    result := tgoReplyValidationResult.rvrNoHeader; // not enough bytes for a header

end;

constructor TgoMongoMsgReply.Create(const ABuffer: Pointer; const ASize: Integer);
begin
  inherited Create;
  ReadData(ABuffer, ASize);
end;

function TgoMongoMsgReply._FirstDoc: TgoBsonDocument;
begin
  if FFirstDoc.IsNil then
  begin
    if length(FPayload0) > 0 then
      FFirstDoc := TgoBsonDocument.Load(FPayload0[0]);
  end;
  result := FFirstDoc;
end;

function TgoMongoMsgReply._GetPayload0: TArray<tBytes>;
begin
  result := FPayload0;
end;

function TgoMongoMsgReply._GetPayload1: TArray<tgoPayloadType1>;
begin
  result := FPayload1;
end;

function TgoMongoMsgReply._GetResponseTo: Integer;
begin
  result := FHeader.Header.ResponseTo;
end;

{ TgoMongoProtocolSettings }

initialization

end.

