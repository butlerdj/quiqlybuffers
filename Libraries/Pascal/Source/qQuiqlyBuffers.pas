{                                                                              }
{ Quiqly Buffers 0.14                                                          }
{                                                                              }
{ Quiqly Buffers is a compact binary encoding for structured data.             }
{                                                                              }
{ The Quiqly Buffer protocol is open and free to use.                          }
{                                                                              }
{ Copyright (c) 2018-2019, Quiqly.                                             }
{                                                                              }
{ Revision history:                                                            }
{                                                                              }
{   2018/07/25  0.01  Initial version as part of MQ.                           }
{   2018/08/10  0.02  Refactored as Quiqly Buffers.                            }
{   2018/09/02  0.03  Bytes type.                                              }
{   2018/09/05  0.04  Int8, Int16, Int32 encodings.                            }
{   2018/09/18  0.05  Add buffer header (buffer size).                         }
{   2018/11/04  0.06  DateTime encoding.                                       }
{   2018/11/04  0.07  Derive data size from FieldType.                         }
{   2018/11/04  0.08  Remove DataSize field from field header.                 }
{   2018/11/05  0.09  VarInt encoding for FieldId.                             }
{   2018/11/05  0.10  Indexed GetField.                                        }
{   2018/11/05  0.11  Buffer iterator.                                         }
{   2018/11/05  0.12  Initial buffer in message buffer record.                 }
{   2019/04/03  0.13  qpMsgBufGetFieldXXXDef functions.                        }
{   2019/04/10  0.14  Word8, Word16, Word32 and Word64 encodings.              }
{                                                                              }

{ Todo: Add/Get from RTTI for record }
{       Add from another MsgBuf }

{$INCLUDE flcInclude.inc}

unit qQuiqlyBuffers;

interface

uses
  { System }
  SysUtils,

  { Fundamentals }
  flcStdTypes;



type
  EqpMessageBuffer = class(Exception);



{ Message Buffer structure }

const
  QP_MsgBuf_MaxRecordStackDepth = 32;
  QP_MsgBuf_InitialBufferSize = 1024;

type
  TqpMessageBuffer = record
    Buffer        : Pointer;  // Encoded message buffer pointer
    BufferSize    : Int32;    // Encoded message buffer size
    AllocSize     : Int32;
    InitialBuffer : array[0..QP_MsgBuf_InitialBufferSize - 1] of Byte;
    StackDepth    : Int32;
    RecordStack   : array[0..QP_MsgBuf_MaxRecordStackDepth - 1] of Word32;
  end;



{ Initialise / Finalise }

procedure qpMsgBufInit(out Buf: TqpMessageBuffer);
procedure qpMsgBufInitBuf(out Buf: TqpMessageBuffer;
          const MsgBuf: Pointer; const MsgBufSize: Int32);
procedure qpMsgBufFinalise(var Buf: TqpMessageBuffer);



{ Add field }

procedure qpMsgBufAddFieldIntZero(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
procedure qpMsgBufAddFieldInt8(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Int8);
procedure qpMsgBufAddFieldInt16(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Int16);
procedure qpMsgBufAddFieldInt32(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Int32);
procedure qpMsgBufAddFieldInt64(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Int64);

procedure qpMsgBufAddFieldInt(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Int64);

procedure qpMsgBufAddFieldWordZero(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
procedure qpMsgBufAddFieldWord8(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Word8);
procedure qpMsgBufAddFieldWord16(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Word16);
procedure qpMsgBufAddFieldWord32(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Word32);
procedure qpMsgBufAddFieldWord64(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Word64);

procedure qpMsgBufAddFieldUInt(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: UInt64);

procedure qpMsgBufAddFieldString(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: String);

procedure qpMsgBufAddFieldBoolean(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Boolean);

procedure qpMsgBufAddFieldSingle(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Single);
procedure qpMsgBufAddFieldDouble(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Double);
procedure qpMsgBufAddFieldFloat(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Double);

procedure qpMsgBufAddFieldBytesPtr(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const BytesPtr: Pointer; const BytesBufSize: Int32);
procedure qpMsgBufAddFieldBytes(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: TBytes);

procedure qpMsgBufAddFieldDateTime(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: TDateTime);
procedure qpMsgBufAddFieldDate(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: TDateTime);

procedure qpMsgBufAddFieldRecordStart(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
procedure qpMsgBufAddFieldRecordEnd(var Buf: TqpMessageBuffer);



{ Get Field by FieldId }
{ Returns True if field exists }

function  qpMsgBufGetFieldInt(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; out Value: Int64;
          const BasePos: Word32 = 0): Boolean;
function  qpMsgBufGetFieldUInt(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; out Value: UInt64;
          const BasePos: Word32 = 0): Boolean;
function  qpMsgBufGetFieldString(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; out Value: String;
          const BasePos: Word32 = 0): Boolean;
function  qpMsgBufGetFieldBoolean(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; out Value: Boolean;
          const BasePos: Word32 = 0): Boolean;
function  qpMsgBufGetFieldFloat(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; out Value: Double;
          const BasePos: Word32 = 0): Boolean;

function  qpMsgBufGetFieldBytesBufSize(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          out FieldBytesSize: Int32;
          const BasePos: Word32 = 0): Boolean;
function  qpMsgBufGetFieldBytesBuf(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; var BytesBuf; const BytesBufSize: Int32;
          out FieldBytesSize: Int32;
          const BasePos: Word32 = 0): Boolean;
function  qpMsgBufGetFieldBytes(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; out Bytes: TBytes;
          const BasePos: Word32 = 0): Boolean;

function  qpMsgBufGetFieldDateTime(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; out Value: TDateTime;
          const BasePos: Word32 = 0): Boolean;
function  qpMsgBufGetFieldRecord(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; var RecordBasePos: Word32;
          const BasePos: Word32 = 0): Boolean;



{ Get Field by FieldId }
{ Returns Default value if field does not exists }

function  qpMsgBufGetFieldIntDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: Int64;
          const BasePos: Word32 = 0): Int64;

function  qpMsgBufGetFieldUIntDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: UInt64;
          const BasePos: Word32 = 0): UInt64;

function  qpMsgBufGetFieldStringDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: String;
          const BasePos: Word32 = 0): String;

function  qpMsgBufGetFieldBooleanDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: Boolean;
          const BasePos: Word32 = 0): Boolean;

function  qpMsgBufGetFieldFloatDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: Double;
          const BasePos: Word32 = 0): Double;

function  qpMsgBufGetFieldDateTimeDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: TDateTime;
          const BasePos: Word32 = 0): TDateTime;



{ Get Field by FieldId }
{ Raises exception if field does not exists }

function  qpMsgBufRequireFieldInt(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; const BasePos: Word32 = 0): Int64;

function  qpMsgBufRequireFieldUInt(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; const BasePos: Word32 = 0): UInt64;

function  qpMsgBufRequireFieldString(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; const BasePos: Word32 = 0): String;

function  qpMsgBufRequireFieldBoolean(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; const BasePos: Word32 = 0): Boolean;

function  qpMsgBufRequireFieldFloat(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; const BasePos: Word32 = 0): Double;

function  qpMsgBufRequireFieldBytesBuf(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          var BytesBuf; const BytesBufSize: Int32;
          const BasePos: Word32 = 0): Int32;
function  qpMsgBufRequireFieldBytes(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const BasePos: Word32 = 0): TBytes;

function  qpMsgBufRequireFieldDateTime(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; const BasePos: Word32 = 0): TDateTime;

function  qpMsgBufRequireFieldRecord(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32; const BasePos: Word32 = 0): Word32;



{ Message Buffer indexed access }
{ Improves access speed in message buffers with large number of fields. }
{ qpMsgBufIndexFields is called to index fields in a message buffer. }

const
  QP_MsgBufIdx_SmallIndexLength = 256;
  QP_MsgBufIdx_MaxFieldId       = $FFFFF;

type
  TqpMessageBufferIndexType = (qpmbitSmall, qpmbitLarge);
  TqpMessageBufferIndex = record
    BasePos    : Word32;
    IndexType  : TqpMessageBufferIndexType;
    IndexSmall : array[0..QP_MsgBufIdx_SmallIndexLength - 1] of Word32;
    IndexLarge : array of Word32;
  end;

procedure qpMsgBufIndexFields(
          var MsgBufIdx: TqpMessageBufferIndex;
          const MaxFieldId: Word32;
          const Buf: TqpMessageBuffer;
          const BasePos: Word32 = 0);

function  qpMsgBufIdxGetFieldInt(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32; out Value: Int64): Boolean;

function  qpMsgBufIdxGetFieldUInt(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32; out Value: UInt64): Boolean;

function  qpMsgBufIdxGetFieldString(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32; out Value: String): Boolean;

function  qpMsgBufIdxGetFieldBoolean(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32; out Value: Boolean): Boolean;

function  qpMsgBufIdxGetFieldFloat(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32; out Value: Double): Boolean;

function  qpMsgBufIdxGetFieldBytes(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32; out Value: TBytes): Boolean;

function  qpMsgBufIdxGetFieldDateTime(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32; out Value: TDateTime): Boolean;

function  qpMsgBufIdxGetFieldRecord(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32; out RecordBasePos: Word32): Boolean;



function  qpMsgBufIdxRequireFieldInt(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32): Int64;

function  qpMsgBufIdxRequireFieldUInt(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32): UInt64;

function  qpMsgBufIdxRequireFieldString(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32): String;

function  qpMsgBufIdxRequireFieldBoolean(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32): Boolean;

function  qpMsgBufIdxRequireFieldFloat(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32): Double;

function  qpMsgBufIdxRequireFieldBytes(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32): TBytes;

function  qpMsgBufIdxRequireFieldDateTime(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32): TDateTime;

function  qpMsgBufIdxRequireFieldRecord(
          const Buf: TqpMessageBuffer;
          const BufIdx: TqpMessageBufferIndex;
          const FieldId: Word32): Word32;



{ Message Buffer iterator }

type
  TqpMessageBufferIterator = record
    BufOfs     : Word32;
    FieldType  : Byte;
    FieldId    : Word32;
    DataP      : Pointer;
    DataSize   : Word32;
    HeaderSize : Word32;
  end;

function  qpMsgBufIterateFirst(
          const Buf: TqpMessageBuffer;
          const BasePos: Word32;
          var Iterator: TqpMessageBufferIterator): Boolean;

function  qpMsgBufIterateNext(
          const Buf: TqpMessageBuffer;
          var Iterator: TqpMessageBufferIterator): Boolean;

function  qpMsgBufIteratorGetFieldInt(
          const Iterator: TqpMessageBufferIterator): Int64;

function  qpMsgBufIteratorGetFieldUInt(
          const Iterator: TqpMessageBufferIterator): UInt64;

function  qpMsgBufIteratorGetFieldString(
          const Iterator: TqpMessageBufferIterator): String;

function  qpMsgBufIteratorGetFieldBoolean(
          const Iterator: TqpMessageBufferIterator): Boolean;

function  qpMsgBufIteratorGetFieldFloat(
          const Iterator: TqpMessageBufferIterator): Double;

function  qpMsgBufIteratorGetFieldBytes(
          const Iterator: TqpMessageBufferIterator): TBytes;

function  qpMsgBufIteratorGetFieldDateTime(
          const Iterator: TqpMessageBufferIterator): TDateTime;

function  qpMsgBufIteratorGetFieldRecord(
          const Iterator: TqpMessageBufferIterator): Word32;



{ Tests }

procedure Test;



implementation



const
  SErrInvalidEncoding = 'Invalid encoding';
  SErrInvalidFieldType = 'Invalid field type';
  SErrFieldNotFound = 'Field not found';
  SErrMaxFieldIdTooHigh = 'Max FieldId too high';
  SErrTypeConversionError = 'Type conversion error';



type
  TqpMessageBufferHeader = record
    Size : Word32;
  end;
  PqpMessageBufferHeader = ^TqpMessageBufferHeader;

const
  QP_MsgBuf_HeaderSize = SizeOf(TqpMessageBufferHeader);



const
  QP_MessageBufferFieldTypeSize = SizeOf(Byte);

  // FieldType
  //
  // Data size in low nibble
  // 0    = 0 byte size, special value (0)
  // 1..8 = 1..8 bytes
  // 9    = 16 bytes
  // A    = 1 byte size
  // B    = 2 byte size
  // C    = 4 byte size
  // D    = 0 bytes, special value
  // E    = 0 bytes, special value (null)
  // F    = 0 bytes, special value (-1/1)
  //
  // Data type in high nibble
  // 0 = Word / Unsigned Int
  // 1 = Signed Int
  // 2 = Wide String (2 bytes per character)
  // 3 = Boolean
  // 4 = Floating point
  // 5 = Untyped bytes
  // 6 = Date/Time
  // E = Record
  // F = Special: end of record
  QP_MsgBuf_FieldType_Word_Zero     = $00;
  QP_MsgBuf_FieldType_Word8         = $01;
  QP_MsgBuf_FieldType_Word16        = $02;
  QP_MsgBuf_FieldType_Word32        = $04;
  QP_MsgBuf_FieldType_Word64        = $08;
  QP_MsgBuf_FieldType_Int_Zero      = $10;
  QP_MsgBuf_FieldType_Int8          = $11;
  QP_MsgBuf_FieldType_Int16         = $12;
  QP_MsgBuf_FieldType_Int32         = $14;
  QP_MsgBuf_FieldType_Int64         = $18;
  QP_MsgBuf_FieldType_WString_Empty = $20;
  QP_MsgBuf_FieldType_WString_Size1 = $2A;
  QP_MsgBuf_FieldType_WString_Size2 = $2B;
  QP_MsgBuf_FieldType_WString_Size4 = $2C;
  QP_MsgBuf_FieldType_Boolean_False = $30;
  QP_MsgBuf_FieldType_Boolean       = $31;
  QP_MsgBuf_FieldType_Boolean_True  = $3F;
  QP_MsgBuf_FieldType_Float_Zero    = $40;
  QP_MsgBuf_FieldType_Single        = $44;
  QP_MsgBuf_FieldType_Double        = $48;
  QP_MsgBuf_FieldType_Bytes_Empty   = $50;
  QP_MsgBuf_FieldType_Bytes_1       = $51;
  QP_MsgBuf_FieldType_Bytes_2       = $52;
  QP_MsgBuf_FieldType_Bytes_4       = $54;
  QP_MsgBuf_FieldType_Bytes_8       = $58;
  QP_MsgBuf_FieldType_Bytes_Size1   = $5A;
  QP_MsgBuf_FieldType_Bytes_Size2   = $5B;
  QP_MsgBuf_FieldType_Bytes_Size4   = $5C;
  QP_MsgBuf_FieldType_DateTime_Zero = $60;
  QP_MsgBuf_FieldType_Date          = $64;
  QP_MsgBuf_FieldType_DateTime      = $68;
  QP_MsgBuf_FieldType_Record        = $E4;
  QP_MsgBuf_FieldType_End           = $F0;

function qpMsgBufFieldTypeDataSize(const FieldType: Byte; out DataSizeSize: Byte): Byte;
var
  DataSizeNib : Byte;
begin
  DataSizeNib := FieldType and $0F;
  case DataSizeNib of
    $A : DataSizeSize := 1;
    $B : DataSizeSize := 2;
    $C : DataSizeSize := 4;
  else
    DataSizeSize := 0;
  end;
  case DataSizeNib of
    1..8 : Result := DataSizeNib;
    9    : Result := 16;
  else
    Result := 0;
  end;
end;



const
  qpVarIntMaxEncSize = 5;

// returns bytes needed to encode Word32 VarInt value
function qpVarIntWord32EncodedSize(const B: Word32): Integer;
begin
  if B < $80 then
    Result := 1
  else
  if B < $4000 then
    Result := 2
  else
  if B < $200000 then
    Result := 3
  else
  if B < $10000000 then
    Result := 4
  else
    Result := 5;
end;

// initialises VarInt from Word32
// assumes Buf has at least qpVarIntWord32EncodeSize bytes
function qpVarIntWord32Encode(var Buf; const B: Word32): Integer;
var
  P : PByte;
begin
  P := @Buf;
  if B < $80 then
    begin
      P^ := Byte(B);
      Result := 1;
    end
  else
  if B < $4000 then
    begin
      P^ := Byte(B and $7F) or $80; Inc(P);
      P^ := Byte(B shr 7);
      Result := 2;
    end
  else
  if B < $200000 then
    begin
      P^ := Byte(B          and $7F) or $80; Inc(P);
      P^ := Byte((B shr 7)  and $7F) or $80; Inc(P);
      P^ := Byte( B shr 14         );
      Result := 3;
    end
  else
  if B < $10000000 then
    begin
      P^ := Byte(B          and $7F) or $80; Inc(P);
      P^ := Byte((B shr 7)  and $7F) or $80; Inc(P);
      P^ := Byte((B shr 14) and $7F) or $80; Inc(P);
      P^ := Byte( B shr 21         );
      Result := 4;
    end
  else
    begin
      P^ := Byte(B          and $7F) or $80; Inc(P);
      P^ := Byte((B shr 7)  and $7F) or $80; Inc(P);
      P^ := Byte((B shr 14) and $7F) or $80; Inc(P);
      P^ := Byte((B shr 21) and $7F) or $80; Inc(P);
      P^ := Byte( B shr 28         );
      Result := 5;
    end;
end;

// returns size of VarInt encoded in buffer
// buffer may be larger than the VarInt encoded in it
function qpVarIntEncodedSize(const Buf; const BufSize: Integer): Integer;
var
  L, I : Integer;
  P : PByte;
  F : Boolean;
  C : Byte;
begin
  L := BufSize;
  if L <= 0 then
    raise EqpMessageBuffer.Create(SErrInvalidEncoding);
  I := 0;
  P := @Buf;
  F := False;
  repeat
    C := P^;
    Inc(I);
    if C and $80 = 0 then
      F := True
    else
      begin
        if (I >= qpVarIntMaxEncSize) or (I >= L) then
          raise EqpMessageBuffer.Create(SErrInvalidEncoding);
        Inc(P);
      end;
  until F;
  Result := I;
end;

// returns encoded VarInt buffer as Word32
// assumes valid encoding in buffer
function qpVarIntToWord32(const Buf; const EncSize: Integer): Word32;
var
  P : PByte;
begin
  Assert(EncSize > 0);
  P := @Buf;
  case EncSize of
    1 : Result := P^;
    2 : begin
          Result :=            Word32(P^) and $7F; Inc(P);
          Result := Result or (Word32(P^) shl 7);
        end;
    3 : begin
          Result :=             Word32(P^) and $7F; Inc(P);
          Result := Result or ((Word32(P^) and $7F) shl 7); Inc(P);
          Result := Result or  (Word32(P^) shl 14);
        end;
    4 : begin
          Result :=             Word32(P^) and $7F; Inc(P);
          Result := Result or ((Word32(P^) and $7F) shl 7);  Inc(P);
          Result := Result or ((Word32(P^) and $7F) shl 14); Inc(P);
          Result := Result or  (Word32(P^) shl 21);
        end;
    5 : begin
          Result :=             Word32(P^) and $7F; Inc(P);
          Result := Result or ((Word32(P^) and $7F) shl 7);  Inc(P);
          Result := Result or ((Word32(P^) and $7F) shl 14); Inc(P);
          Result := Result or ((Word32(P^) and $7F) shl 21); Inc(P);
          if P^ and $F0 <> 0 then
            raise EqpMessageBuffer.Create(SErrInvalidEncoding);
          Result := Result or  (Word32(P^) shl 28);
        end;
  else
    raise EqpMessageBuffer.Create(SErrInvalidEncoding);
  end;
end;



procedure qpMsgBufInit(out Buf: TqpMessageBuffer);
begin
  Buf.Buffer := @Buf.InitialBuffer;
  Buf.BufferSize := 0;
  Buf.AllocSize := 0;
  Buf.StackDepth := 0;
end;

procedure qpMsgBufInitBuf(out Buf: TqpMessageBuffer;
    const MsgBuf: Pointer; const MsgBufSize: Int32);
begin
  if MsgBufSize < 0 then
    raise EqpMessageBuffer.Create('Invalid buffer size');
  if MsgBufSize > 0 then
    begin
      if MsgBufSize < QP_MsgBuf_HeaderSize then
        raise EqpMessageBuffer.Create('Invalid buffer size');
      if PqpMessageBufferHeader(MsgBuf)^.Size <> Word32(MsgBufSize) then
        raise EqpMessageBuffer.Create('Invalid buffer size');
    end;
  Buf.Buffer := MsgBuf;
  Buf.BufferSize := MsgBufSize;
  Buf.AllocSize := -1;
  Buf.StackDepth := 0;
end;

procedure qpMsgBufFinalise(var Buf: TqpMessageBuffer);
begin
  if Buf.AllocSize > 0 then
    begin
      Buf.AllocSize := 0;
      Buf.BufferSize := 0;
      FreeMem(Buf.Buffer);
      Buf.Buffer := nil;
    end;
end;



procedure qpMsgBufEnsureAlloc(var Msg: TqpMessageBuffer; const Alloc: Int32);
var
  OldAlloc : Int32;
  NewAlloc : Int32;
begin
  OldAlloc := Msg.AllocSize;
  if OldAlloc < 0 then
    raise EqpMessageBuffer.Create('Buffer not resizable');
  if Alloc <= QP_MsgBuf_InitialBufferSize then
    exit;
  if OldAlloc >= Alloc then
    exit;
  if OldAlloc = 0 then
    begin
      NewAlloc := QP_MsgBuf_InitialBufferSize * 4;
      if NewAlloc < Alloc then
        NewAlloc := Alloc;
      GetMem(Msg.Buffer, NewAlloc);
      Move(Msg.InitialBuffer, Msg.Buffer^, Msg.BufferSize);
    end
  else
    begin
      NewAlloc := OldAlloc * 4;
      if NewAlloc < Alloc then
        NewAlloc := Alloc;
      ReallocMem(Msg.Buffer, NewAlloc);
    end;
  Msg.AllocSize := NewAlloc;
end;

procedure qpMsgBufEnsureDataAvail(var Msg: TqpMessageBuffer; const Size: Int32);
begin
  qpMsgBufEnsureAlloc(Msg, Msg.BufferSize + Size);
end;



procedure qpMsgBufAddFieldBuf(var Msg: TqpMessageBuffer;
          const FieldId: Word32; const FieldType: Byte;
          const ValueSizePtr: Pointer; const ValueSizeSize: Int32;
          const ValuePtr: Pointer; const ValueSize: Int32);
var
  BufSize : Int32;
  FieldIdSize : Int32;
  P, PBuf : PByte;
  FieldSize : Int32;
begin
  Assert(ValueSizeSize >= 0);
  Assert(ValueSize >= 0);
  BufSize := Msg.BufferSize;
  if BufSize = 0 then
    begin
      qpMsgBufEnsureDataAvail(Msg, QP_MsgBuf_HeaderSize);
      Msg.BufferSize := QP_MsgBuf_HeaderSize;
      BufSize := QP_MsgBuf_HeaderSize;
    end;
  FieldIdSize := qpVarIntWord32EncodedSize(FieldId);
  FieldSize := QP_MessageBufferFieldTypeSize + FieldIdSize + ValueSizeSize + ValueSize;
  qpMsgBufEnsureDataAvail(Msg, FieldSize);
  PBuf := Msg.Buffer;
  P := PBuf;
  Inc(P, BufSize);
  P^ := FieldType;
  Inc(P, QP_MessageBufferFieldTypeSize);
  qpVarIntWord32Encode(P^, FieldId);
  Inc(P, FieldIdSize);
  if ValueSizeSize > 0 then
    begin
      Move(ValueSizePtr^, P^, ValueSizeSize);
      Inc(P, ValueSizeSize);
    end;
  if ValueSize > 0 then
    Move(ValuePtr^, P^, ValueSize);
  Inc(BufSize, FieldSize);
  Msg.BufferSize := BufSize;
  PqpMessageBufferHeader(PBuf)^.Size := BufSize;
end;

procedure qpMsgBufAddFieldIntZero(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Int_Zero,
      nil, 0, nil, 0);
end;

procedure qpMsgBufAddFieldInt8(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Int8);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Int8,
      nil, 0, @Value, SizeOf(Int8));
end;

procedure qpMsgBufAddFieldInt16(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Int16);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Int16,
      nil, 0, @Value, SizeOf(Int16));
end;

procedure qpMsgBufAddFieldInt32(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Int32);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Int32,
      nil, 0, @Value, SizeOf(Int32));
end;

procedure qpMsgBufAddFieldInt64(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Int64);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Int64,
      nil, 0, @Value, SizeOf(Int64));
end;

procedure qpMsgBufAddFieldInt(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Int64);
begin
  if Value = 0 then
    qpMsgBufAddFieldIntZero(Buf, FieldId)
  else
  if (Value >= MinInt8) and (Value <= MaxInt8) then
    qpMsgBufAddFieldInt8(Buf, FieldId, Int8(Value))
  else
  if (Value >= MinInt16) and (Value <= MaxInt16) then
    qpMsgBufAddFieldInt16(Buf, FieldId, Int16(Value))
  else
  if (Value >= MinInt32) and (Value <= MaxInt32) then
    qpMsgBufAddFieldInt32(Buf, FieldId, Int32(Value))
  else
    qpMsgBufAddFieldInt64(Buf, FieldId, Value);
end;

procedure qpMsgBufAddFieldWordZero(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Word_Zero,
      nil, 0, nil, 0);
end;

procedure qpMsgBufAddFieldWord8(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Word8);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Word8,
      nil, 0, @Value, SizeOf(Word8));
end;

procedure qpMsgBufAddFieldWord16(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Word16);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Word16,
      nil, 0, @Value, SizeOf(Word16));
end;

procedure qpMsgBufAddFieldWord32(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Word32);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Word32,
      nil, 0, @Value, SizeOf(Word32));
end;

procedure qpMsgBufAddFieldWord64(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Word64);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Word64,
      nil, 0, @Value, SizeOf(Word64));
end;

procedure qpMsgBufAddFieldUInt(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: UInt64);
begin
  if Value = 0 then
    qpMsgBufAddFieldWordZero(Buf, FieldId)
  else
  if Value <= MaxByte then
    qpMsgBufAddFieldWord8(Buf, FieldId, Byte(Value))
  else
  if Value <= MaxWord16 then
    qpMsgBufAddFieldWord16(Buf, FieldId, Word16(Value))
  else
  if Value <= MaxWord32 then
    qpMsgBufAddFieldWord32(Buf, FieldId, Word32(Value))
  else
    qpMsgBufAddFieldWord64(Buf, FieldId, Value);
end;

procedure qpMsgBufAddFieldWString_Empty(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_WString_Empty,
      nil, 0, nil, 0);
end;

procedure qpMsgBufAddFieldWString_Size1(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: String);
var
  Size : Byte;
begin
  Size := Length(Value) * SizeOf(Char);
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_WString_Size1,
      @Size, 1, PChar(Value), Size);
end;

procedure qpMsgBufAddFieldWString_Size2(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: String);
var
  Size : Word16;
begin
  Size := Length(Value) * SizeOf(Char);
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_WString_Size2,
      @Size, 2, PChar(Value), Size);
end;

procedure qpMsgBufAddFieldWString_Size4(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: String);
var
  Size : Word32;
begin
  Size := Length(Value) * SizeOf(Char);
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_WString_Size4,
      @Size, 4, PChar(Value), Size);
end;

procedure qpMsgBufAddFieldString(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: String);
var
  Size : Word32;
begin
  Size := Length(Value) * SizeOf(Char);
  if Size = 0 then
    qpMsgBufAddFieldWString_Empty(Buf, FieldId)
  else
  if Size <= MaxByte then
    qpMsgBufAddFieldWString_Size1(Buf, FieldId, Value)
  else
  if Size <= MaxWord16 then
    qpMsgBufAddFieldWString_Size2(Buf, FieldId, Value)
  else
    qpMsgBufAddFieldWString_Size4(Buf, FieldId, Value);
end;

procedure qpMsgBufAddFieldBoolean_False(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Boolean_False,
      nil, 0, nil, 0);
end;

procedure qpMsgBufAddFieldBoolean_True(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Boolean_True,
      nil, 0, nil, 0);
end;

procedure qpMsgBufAddFieldBoolean(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Boolean);
begin
  if Value then
    qpMsgBufAddFieldBoolean_True(Buf, FieldId)
  else
    qpMsgBufAddFieldBoolean_False(Buf, FieldId);
end;

procedure qpMsgBufAddFieldFloat_Zero(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Float_Zero,
      nil, 0, nil, 0);
end;

procedure qpMsgBufAddFieldSingle(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Single);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Single,
      nil, 0, @Value, SizeOf(Single));
end;

procedure qpMsgBufAddFieldDouble(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Double);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Double,
      nil, 0, @Value, SizeOf(Double));
end;

procedure qpMsgBufAddFieldFloat(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: Double);
begin
  if Value = 0.0 then
    qpMsgBufAddFieldFloat_Zero(Buf, FieldId)
  else
    qpMsgBufAddFieldDouble(Buf, FieldId, Value);
end;

procedure qpMsgBufAddFieldBytes_Empty(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Bytes_Empty,
      nil, 0, nil, 0);
end;

procedure qpMsgBufAddFieldBytesPtr_Size1(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const BytesPtr: Pointer; const BytesBufSize: Byte);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Bytes_Size1,
      @BytesBufSize, 1, BytesPtr, BytesBufSize);
end;

procedure qpMsgBufAddFieldBytesPtr_Size2(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const BytesPtr: Pointer; const BytesBufSize: Word16);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Bytes_Size2,
      @BytesBufSize, 2, BytesPtr, BytesBufSize);
end;

procedure qpMsgBufAddFieldBytesPtr_Size4(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const BytesPtr: Pointer; const BytesBufSize: Word32);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Bytes_Size4,
      @BytesBufSize, 4, BytesPtr, BytesBufSize);
end;

procedure qpMsgBufAddFieldBytesPtr(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const BytesPtr: Pointer; const BytesBufSize: Int32);
begin
  if BytesBufSize <= 0 then
    qpMsgBufAddFieldBytes_Empty(Buf, FieldId)
  else
  if BytesBufSize <= MaxByte then
    qpMsgBufAddFieldBytesPtr_Size1(Buf, FieldId, BytesPtr, BytesBufSize)
  else
  if BytesBufSize <= MaxWord16 then
    qpMsgBufAddFieldBytesPtr_Size2(Buf, FieldId, BytesPtr, BytesBufSize)
  else
    qpMsgBufAddFieldBytesPtr_Size4(Buf, FieldId, BytesPtr, BytesBufSize);
end;

procedure qpMsgBufAddFieldBytes(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: TBytes);
begin
  qpMsgBufAddFieldBytesPtr(Buf, FieldId, Pointer(Value), Length(Value));
end;

procedure qpMsgBufAddFieldDateTime(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: TDateTime);
begin
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_DateTime,
      nil, 0, @Value, SizeOf(TDateTime));
end;

procedure qpMsgBufAddFieldDate(var Buf: TqpMessageBuffer;
          const FieldId: Word32; const Value: TDateTime);
var
  ValDate : Int32;
begin
  ValDate := Trunc(Value);
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Date,
      nil, 0, @ValDate, SizeOf(Int32));
end;

procedure qpMsgBufAddFieldRecordStart(var Buf: TqpMessageBuffer;
          const FieldId: Word32);
var
  RecLen : Word32;
  StackDepth : Int32;
begin
  RecLen := 0;
  qpMsgBufAddFieldBuf(Buf, FieldId, QP_MsgBuf_FieldType_Record,
      nil, 0, @RecLen, SizeOf(Word32));
  StackDepth := Buf.StackDepth;
  Buf.RecordStack[StackDepth] := Buf.BufferSize;
  Buf.StackDepth := StackDepth + 1;
end;

procedure qpMsgBufAddFieldRecordEnd(var Buf: TqpMessageBuffer);
var
  StackDepth : Int32;
  RecStartOfs : Word32;
  RecStartDataP : PByte;
begin
  StackDepth := Buf.StackDepth;
  if StackDepth <= 0 then
    raise EqpMessageBuffer.Create('Record end without start');
  qpMsgBufAddFieldBuf(Buf, 0, QP_MsgBuf_FieldType_End,
      nil, 0, nil, 0);
  Dec(StackDepth);
  RecStartOfs := Buf.RecordStack[StackDepth];
  RecStartDataP := Buf.Buffer;
  Inc(RecStartDataP, RecStartOfs - SizeOf(Word32));
  PWord32(RecStartDataP)^ := Word32(Buf.BufferSize) - RecStartOfs;
  Buf.StackDepth := StackDepth;
end;



procedure qpMsgBufDecodeFieldHeader(
          const Buf; const BufSize: Int32;
          out FieldType: Byte;
          out FieldIdP: Pointer; out FieldIdSize: Int32;
          out DataP: Pointer; out DataSize: Word32;
          out HeaderSize: Int32);
var
  BufLeft : Int32;
  BufP : PByte;
  DataSizeSize : Byte;
begin
  BufLeft := BufSize;
  Dec(BufLeft, QP_MessageBufferFieldTypeSize);
  if BufLeft < 0 then
    raise EqpMessageBuffer.Create(SErrInvalidEncoding);
  BufP := @Buf;
  FieldType := BufP^;
  Inc(BufP, QP_MessageBufferFieldTypeSize);
  FieldIdSize := qpVarIntEncodedSize(BufP^, BufLeft);
  FieldIdP := BufP;
  Dec(BufLeft, FieldIdSize);
  Inc(BufP, FieldIdSize);
  DataSize := qpMsgBufFieldTypeDataSize(FieldType, DataSizeSize);
  if DataSizeSize = 0 then
    begin
      HeaderSize := BufSize - BufLeft;
      if DataSize > 0 then
        begin
          Dec(BufLeft, DataSize);
          if BufLeft < 0 then
            raise EqpMessageBuffer.Create(SErrInvalidEncoding);
          DataP := BufP;
        end
      else
        DataP := nil;
    end
  else
    begin
      Dec(BufLeft, DataSizeSize);
      if BufLeft < 0 then
        raise EqpMessageBuffer.Create(SErrInvalidEncoding);
      HeaderSize := BufSize - BufLeft;
      case DataSizeSize of
        1 : DataSize := PByte(BufP)^;
        2 : DataSize := PWord16(BufP)^;
        4 : DataSize := PWord32(BufP)^;
      else
        raise EqpMessageBuffer.Create(SErrInvalidEncoding);
      end;
      Inc(BufP, DataSizeSize);
      DataP := BufP;
      Dec(BufLeft, DataSize);
      if BufLeft < 0 then
        raise EqpMessageBuffer.Create(SErrInvalidEncoding);
    end;
end;



procedure qpMsgBufFieldToInt(const FieldType: Byte; const DataP: Pointer; out Value: Int64);
begin
  case FieldType of
    QP_MsgBuf_FieldType_Word_Zero,
    QP_MsgBuf_FieldType_Bytes_Empty,
    QP_MsgBuf_FieldType_DateTime_Zero,
    QP_MsgBuf_FieldType_Int_Zero       : Value := 0;
    QP_MsgBuf_FieldType_Bytes_1,
    QP_MsgBuf_FieldType_Int8           : Value := PInt8(DataP)^;
    QP_MsgBuf_FieldType_Bytes_2,
    QP_MsgBuf_FieldType_Int16          : Value := PInt16(DataP)^;
    QP_MsgBuf_FieldType_Bytes_4,
    QP_MsgBuf_FieldType_Date,
    QP_MsgBuf_FieldType_Int32          : Value := PInt32(DataP)^;
    QP_MsgBuf_FieldType_Bytes_8,
    QP_MsgBuf_FieldType_Int64          : Value := PInt64(DataP)^;
    QP_MsgBuf_FieldType_Word8          : Value := PWord8(DataP)^;
    QP_MsgBuf_FieldType_Word16         : Value := PWord16(DataP)^;
    QP_MsgBuf_FieldType_Word32         : Value := PWord32(DataP)^;
    QP_MsgBuf_FieldType_Word64         : if PWord64(DataP)^ > MaxInt64 then
                                           raise EqpMessageBuffer.Create(SErrTypeConversionError)
                                         else
                                           Value := PWord64(DataP)^;
    QP_MsgBuf_FieldType_Float_Zero     : Value := 0;
    QP_MsgBuf_FieldType_Boolean_False  : Value := 0;
    QP_MsgBuf_FieldType_Boolean        : if PBoolean(DataP)^ then Value := 1 else Value := 0;
    QP_MsgBuf_FieldType_Boolean_True   : Value := 1;
  else
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  end;
end;

procedure qpMsgBufFieldToUInt(const FieldType: Byte; const DataP: Pointer; out Value: UInt64);
begin
  case FieldType of
    QP_MsgBuf_FieldType_Word_Zero,
    QP_MsgBuf_FieldType_Bytes_Empty,
    QP_MsgBuf_FieldType_DateTime_Zero,
    QP_MsgBuf_FieldType_Int_Zero       : Value := 0;
    QP_MsgBuf_FieldType_Int8           : if PInt8(DataP)^ < 0 then
                                           raise EqpMessageBuffer.Create(SErrTypeConversionError)
                                         else
                                           Value := PInt8(DataP)^;
    QP_MsgBuf_FieldType_Int16          : if PInt16(DataP)^ < 0 then
                                           raise EqpMessageBuffer.Create(SErrTypeConversionError)
                                         else
                                           Value := PInt16(DataP)^;
    QP_MsgBuf_FieldType_Date,
    QP_MsgBuf_FieldType_Int32          : if PInt32(DataP)^ < 0 then
                                           raise EqpMessageBuffer.Create(SErrTypeConversionError)
                                         else
                                           Value := PInt32(DataP)^;
    QP_MsgBuf_FieldType_Int64          : if PInt64(DataP)^ < 0 then
                                           raise EqpMessageBuffer.Create(SErrTypeConversionError)
                                         else
                                           Value := PInt64(DataP)^;
    QP_MsgBuf_FieldType_Float_Zero     : Value := 0;
    QP_MsgBuf_FieldType_Boolean_False  : Value := 0;
    QP_MsgBuf_FieldType_Boolean        : if PBoolean(DataP)^ then Value := 1 else Value := 0;
    QP_MsgBuf_FieldType_Boolean_True   : Value := 1;
    QP_MsgBuf_FieldType_Bytes_1,
    QP_MsgBuf_FieldType_Word8          : Value := PWord8(DataP)^;
    QP_MsgBuf_FieldType_Bytes_2,
    QP_MsgBuf_FieldType_Word16         : Value := PWord16(DataP)^;
    QP_MsgBuf_FieldType_Bytes_4,
    QP_MsgBuf_FieldType_Word32         : Value := PWord32(DataP)^;
    QP_MsgBuf_FieldType_Bytes_8,
    QP_MsgBuf_FieldType_Word64         : Value := PWord64(DataP)^;
  else
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  end;
end;

procedure qpMsgBufFieldToString(const FieldType: Byte;
          const DataP: Pointer; const DataSize: Word32; out Value: String);
var
  Len : Word32;
begin
  case FieldType of
    QP_MsgBuf_FieldType_Int_Zero,
    QP_MsgBuf_FieldType_Float_Zero,
    QP_MsgBuf_FieldType_Word_Zero     : Value := '0';
    QP_MsgBuf_FieldType_Int8          : Value := IntToStr(PInt8(DataP)^);
    QP_MsgBuf_FieldType_Int16         : Value := IntToStr(PInt16(DataP)^);
    QP_MsgBuf_FieldType_Int32         : Value := IntToStr(PInt32(DataP)^);
    QP_MsgBuf_FieldType_Int64         : Value := IntToStr(PInt64(DataP)^);
    QP_MsgBuf_FieldType_Boolean_False : Value := 'false';
    QP_MsgBuf_FieldType_Boolean       : if PBoolean(DataP)^ then Value := 'true' else Value := 'false';
    QP_MsgBuf_FieldType_Boolean_True  : Value := 'true';
    QP_MsgBuf_FieldType_Single        : Value := FloatToStr(PSingle(DataP)^);
    QP_MsgBuf_FieldType_Double        : Value := FloatToStr(PDouble(DataP)^);
    QP_MsgBuf_FieldType_Bytes_Empty   : Value := '';
    QP_MsgBuf_FieldType_Bytes_1       : Value := WideChar(PByte(DataP)^);
    QP_MsgBuf_FieldType_Bytes_2       : Value := WideChar(PWord16(DataP)^);
    QP_MsgBuf_FieldType_Word8         : Value := IntToStr(PWord8(DataP)^);
    QP_MsgBuf_FieldType_Word16        : Value := IntToStr(PWord16(DataP)^);
    QP_MsgBuf_FieldType_Word32        : Value := IntToStr(PWord32(DataP)^);
    QP_MsgBuf_FieldType_Word64        : if PWord64(DataP)^ > MaxInt64 then
                                          raise EqpMessageBuffer.Create(SErrTypeConversionError)
                                        else
                                          Value := IntToStr(PWord64(DataP)^);
    QP_MsgBuf_FieldType_WString_Empty : Value := '';
    QP_MsgBuf_FieldType_WString_Size1,
    QP_MsgBuf_FieldType_WString_Size2,
    QP_MsgBuf_FieldType_WString_Size4 :
      begin
        if DataSize mod 2 = 1 then
          raise EqpMessageBuffer.Create(SErrInvalidEncoding);
        Len := DataSize div 2;
        SetLength(Value, Len);
        if Len > 0 then
          Move(DataP^, PChar(Value)^, DataSize);
      end;
  else
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  end;
end;

procedure qpMsgBufFieldToBoolean(const FieldType: Byte; const DataP: Pointer; out Value: Boolean);
begin
  case FieldType of
    QP_MsgBuf_FieldType_Int_Zero      : Value := False;
    QP_MsgBuf_FieldType_Int8          : Value := PInt8(DataP)^ <> 0;
    QP_MsgBuf_FieldType_Int16         : Value := PInt16(DataP)^ <> 0;
    QP_MsgBuf_FieldType_Int32         : Value := PInt32(DataP)^ <> 0;
    QP_MsgBuf_FieldType_Int64         : Value := PInt64(DataP)^ <> 0;
    QP_MsgBuf_FieldType_Boolean_False : Value := False;
    QP_MsgBuf_FieldType_Boolean       : Value := PBoolean(DataP)^;
    QP_MsgBuf_FieldType_Boolean_True  : Value := True;
    QP_MsgBuf_FieldType_Float_Zero    : Value := False;
    QP_MsgBuf_FieldType_Single        : Value := PSingle(DataP)^ <> 0.0;
    QP_MsgBuf_FieldType_Double        : Value := PDouble(DataP)^ <> 0.0;
    QP_MsgBuf_FieldType_Word_Zero     : Value := False;
    QP_MsgBuf_FieldType_Bytes_Empty   : Value := False;
    QP_MsgBuf_FieldType_Bytes_1,
    QP_MsgBuf_FieldType_Word8         : Value := PWord8(DataP)^ <> 0;
    QP_MsgBuf_FieldType_Bytes_2,
    QP_MsgBuf_FieldType_Word16        : Value := PWord16(DataP)^ <> 0;
    QP_MsgBuf_FieldType_Bytes_4,
    QP_MsgBuf_FieldType_Word32        : Value := PWord32(DataP)^ <> 0;
    QP_MsgBuf_FieldType_Bytes_8,
    QP_MsgBuf_FieldType_Word64        : Value := PWord64(DataP)^ <> 0;
  else
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  end;
end;

procedure qpMsgBufFieldToFloat(const FieldType: Byte; const DataP: Pointer; out Value: Double);
begin
  case FieldType of
    QP_MsgBuf_FieldType_Int_Zero      : Value := 0.0;
    QP_MsgBuf_FieldType_Int8          : Value := PInt8(DataP)^;
    QP_MsgBuf_FieldType_Int16         : Value := PInt16(DataP)^;
    QP_MsgBuf_FieldType_Date,
    QP_MsgBuf_FieldType_Int32         : Value := PInt32(DataP)^;
    QP_MsgBuf_FieldType_Int64         : Value := PInt64(DataP)^;
    QP_MsgBuf_FieldType_Boolean_False : Value := 0.0;
    QP_MsgBuf_FieldType_Boolean       : if PBoolean(DataP)^ then Value := 1.0 else Value := 0.0;
    QP_MsgBuf_FieldType_Boolean_True  : Value := 1.0;
    QP_MsgBuf_FieldType_Float_Zero    : Value := 0.0;
    QP_MsgBuf_FieldType_Bytes_4,
    QP_MsgBuf_FieldType_Single        : Value := PSingle(DataP)^;
    QP_MsgBuf_FieldType_Bytes_8,
    QP_MsgBuf_FieldType_Double        : Value := PDouble(DataP)^;
    QP_MsgBuf_FieldType_Bytes_Empty   : Value := 0.0;
    QP_MsgBuf_FieldType_DateTime_Zero : Value := 0.0;
    QP_MsgBuf_FieldType_DateTime      : Value := PDateTime(DataP)^;
    QP_MsgBuf_FieldType_Word_Zero     : Value := 0.0;
    QP_MsgBuf_FieldType_Word8         : Value := PWord8(DataP)^;
    QP_MsgBuf_FieldType_Word16        : Value := PWord16(DataP)^;
    QP_MsgBuf_FieldType_Word32        : Value := PWord32(DataP)^;
    QP_MsgBuf_FieldType_Word64        : Value := PWord64(DataP)^;
  else
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  end;
end;

procedure qpMsgBufFieldToBytes(const FieldType: Byte; const DataP: Pointer;
          const DataSize: Word32; out Value: TBytes);
begin
  case FieldType of
    QP_MsgBuf_FieldType_Bytes_Empty,
    QP_MsgBuf_FieldType_Bytes_1,
    QP_MsgBuf_FieldType_Bytes_2,
    QP_MsgBuf_FieldType_Bytes_4,
    QP_MsgBuf_FieldType_Bytes_8,
    QP_MsgBuf_FieldType_Bytes_Size1,
    QP_MsgBuf_FieldType_Bytes_Size2,
    QP_MsgBuf_FieldType_Bytes_Size4 : ;
  else
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  end;
  SetLength(Value, DataSize);
  if DataSize > 0 then
    Move(DataP^, Pointer(Value)^, DataSize);
end;

procedure qpMsgBufFieldToDateTime(const FieldType: Byte; const DataP: Pointer; out Value: TDateTime);
begin
  case FieldType of
    QP_MsgBuf_FieldType_Bytes_Empty,
    QP_MsgBuf_FieldType_Float_Zero   : Value := 0.0;
    QP_MsgBuf_FieldType_Single       : Value := PSingle(DataP)^;
    QP_MsgBuf_FieldType_Double       : Value := PDouble(DataP)^;
    QP_MsgBuf_FieldType_Bytes_2,
    QP_MsgBuf_FieldType_Int16        : Value := PInt16(DataP)^;
    QP_MsgBuf_FieldType_Bytes_4,
    QP_MsgBuf_FieldType_Int32,
    QP_MsgBuf_FieldType_Date         : Value := PInt32(DataP)^;
    QP_MsgBuf_FieldType_Bytes_8,
    QP_MsgBuf_FieldType_DateTime     : Value := PDateTime(DataP)^;
  else
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  end;
end;



function qpMsgBufGetField(const Buf: TqpMessageBuffer;
         const FieldId: Word32;
         out BaseOfs: Word32; out FieldType: Byte;
         out FieldDataP: Pointer; out FieldDataSize: Word32;
         const BasePos: Word32): Boolean;
var
  BufOfs : Word32;
  BufSize : Word32;
  BufP : PByte;
  FieldTypeV : Byte;
  FieldIdP : Pointer;
  FieldIdSize : Int32;
  FieldIdV : Word32;
  DataP : Pointer;
  DataSize : Word32;
  HeaderSize : Int32;
  FieldSize : Int32;
  RecSize : Int32;
begin
  BufOfs := BasePos;
  if BufOfs <= 0 then
    BufOfs := QP_MsgBuf_HeaderSize;
  BufSize := Buf.BufferSize;
  BufP := Buf.Buffer;
  Inc(BufP, BufOfs);
  while BufOfs < BufSize do
    begin
      qpMsgBufDecodeFieldHeader(BufP^, BufSize - BufOfs,
          FieldTypeV, FieldIdP, FieldIdSize, DataP, DataSize, HeaderSize);
      FieldSize := HeaderSize + Int32(DataSize);
      Inc(BufP, FieldSize);
      Inc(BufOfs, FieldSize);
      if FieldTypeV = QP_MsgBuf_FieldType_End then
        begin
          Result := False;
          exit;
        end;
      FieldIdV := qpVarIntToWord32(FieldIdP^, FieldIdSize);
      if FieldIdV = FieldId then
        begin
          BaseOfs := BufOfs - BasePos;
          FieldType := FieldTypeV;
          FieldDataP := DataP;
          FieldDataSize := DataSize;
          Result := True;
          exit;
        end;
      if FieldTypeV = QP_MsgBuf_FieldType_Record then
        begin
          RecSize := PWord32(DataP)^;
          Inc(BufP, RecSize);
          Inc(BufOfs, RecSize);
        end;
    end;
  Result := False;
end;

function qpMsgBufGetFieldInt(const Buf: TqpMessageBuffer;
         const FieldId: Word32; out Value: Int64;
         const BasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufGetField(Buf, FieldId, Ofs, FieldType, DataP, DataSize, BasePos) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToInt(FieldType, DataP, Value);
  Result := True;
end;

function qpMsgBufGetFieldUInt(const Buf: TqpMessageBuffer;
         const FieldId: Word32; out Value: UInt64;
         const BasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufGetField(Buf, FieldId, Ofs, FieldType, DataP, DataSize, BasePos) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToUInt(FieldType, DataP, Value);
  Result := True;
end;

function qpMsgBufGetFieldString(const Buf: TqpMessageBuffer;
         const FieldId: Word32; out Value: String;
         const BasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufGetField(Buf, FieldId, Ofs, FieldType, DataP, DataSize, BasePos) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToString(FieldType, DataP, DataSize, Value);
  Result := True;
end;

function qpMsgBufGetFieldBoolean(const Buf: TqpMessageBuffer;
         const FieldId: Word32; out Value: Boolean;
         const BasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufGetField(Buf, FieldId, Ofs, FieldType, DataP, DataSize, BasePos) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToBoolean(FieldType, DataP, Value);
  Result := True;
end;

function qpMsgBufGetFieldFloat(const Buf: TqpMessageBuffer;
         const FieldId: Word32; out Value: Double;
         const BasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufGetField(Buf, FieldId, Ofs, FieldType, DataP, DataSize, BasePos) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToFloat(FieldType, DataP, Value);
  Result := True;
end;

function qpMsgBufGetFieldBytesBufSize(
         const Buf: TqpMessageBuffer;
         const FieldId: Word32;
         out FieldBytesSize: Int32;
         const BasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufGetField(Buf, FieldId, Ofs, FieldType, DataP, DataSize, BasePos) then
    begin
      Result := False;
      exit;
    end;
  case FieldType of
    QP_MsgBuf_FieldType_Bytes_Empty,
    QP_MsgBuf_FieldType_Bytes_1,
    QP_MsgBuf_FieldType_Bytes_Size1,
    QP_MsgBuf_FieldType_Bytes_Size2,
    QP_MsgBuf_FieldType_Bytes_Size4 : FieldBytesSize := DataSize;
  else
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  end;
  Result := True;
end;

function qpMsgBufGetFieldBytesBuf(
         const Buf: TqpMessageBuffer;
         const FieldId: Word32; var BytesBuf; const BytesBufSize: Int32;
         out FieldBytesSize: Int32;
         const BasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
  Size : Int32;
begin
  if not qpMsgBufGetField(Buf, FieldId, Ofs, FieldType, DataP, DataSize, BasePos) then
    begin
      Result := False;
      exit;
    end;
  case FieldType of
    QP_MsgBuf_FieldType_Bytes_Empty,
    QP_MsgBuf_FieldType_Bytes_1,
    QP_MsgBuf_FieldType_Bytes_Size1,
    QP_MsgBuf_FieldType_Bytes_Size2,
    QP_MsgBuf_FieldType_Bytes_Size4 : FieldBytesSize := DataSize;
  else
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  end;
  Size := DataSize;
  if Size > BytesBufSize then
    Size := BytesBufSize;
  if Size > 0 then
    Move(DataP^, BytesBuf, Size);
  Result := True;
end;

function qpMsgBufGetFieldBytes(
         const Buf: TqpMessageBuffer;
         const FieldId: Word32; out Bytes: TBytes;
         const BasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufGetField(Buf, FieldId, Ofs, FieldType, DataP, DataSize, BasePos) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToBytes(FieldType, DataP, DataSize, Bytes);
  Result := True;
end;

function qpMsgBufGetFieldDateTime(const Buf: TqpMessageBuffer;
         const FieldId: Word32; out Value: TDateTime;
         const BasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufGetField(Buf, FieldId, Ofs, FieldType, DataP, DataSize, BasePos) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToDateTime(FieldType, DataP, Value);
  Result := True;
end;

function qpMsgBufGetFieldRecord(const Buf: TqpMessageBuffer;
         const FieldId: Word32; var RecordBasePos: Word32;
         const BasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufGetField(Buf, FieldId, Ofs, FieldType, DataP, DataSize, BasePos) then
    begin
      Result := False;
      exit;
    end;
  if FieldType <> QP_MsgBuf_FieldType_Record then
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  RecordBasePos := BasePos + Ofs;
  Result := True;
end;



function  qpMsgBufGetFieldIntDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: Int64;
          const BasePos: Word32): Int64;
begin
  if not qpMsgBufGetFieldInt(Buf, FieldId, Result, BasePos) then
    Result := Default;
end;

function  qpMsgBufGetFieldUIntDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: UInt64;
          const BasePos: Word32): UInt64;
begin
  if not qpMsgBufGetFieldUInt(Buf, FieldId, Result, BasePos) then
    Result := Default;
end;

function  qpMsgBufGetFieldStringDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: String;
          const BasePos: Word32): String;
begin
  if not qpMsgBufGetFieldString(Buf, FieldId, Result, BasePos) then
    Result := Default;
end;

function  qpMsgBufGetFieldBooleanDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: Boolean;
          const BasePos: Word32): Boolean;
begin
  if not qpMsgBufGetFieldBoolean(Buf, FieldId, Result, BasePos) then
    Result := Default;
end;

function  qpMsgBufGetFieldFloatDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: Double;
          const BasePos: Word32): Double;
begin
  if not qpMsgBufGetFieldFloat(Buf, FieldId, Result, BasePos) then
    Result := Default;
end;

function  qpMsgBufGetFieldDateTimeDef(
          const Buf: TqpMessageBuffer;
          const FieldId: Word32;
          const Default: TDateTime;
          const BasePos: Word32): TDateTime;
begin
  if not qpMsgBufGetFieldDateTime(Buf, FieldId, Result, BasePos) then
    Result := Default;
end;



function qpMsgBufRequireFieldInt(const Buf: TqpMessageBuffer;
         const FieldId: Word32; const BasePos: Word32): Int64;
begin
  if not qpMsgBufGetFieldInt(Buf, FieldId, Result, BasePos) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufRequireFieldUInt(const Buf: TqpMessageBuffer;
         const FieldId: Word32; const BasePos: Word32): UInt64;
begin
  if not qpMsgBufGetFieldUInt(Buf, FieldId, Result, BasePos) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufRequireFieldString(const Buf: TqpMessageBuffer;
         const FieldId: Word32; const BasePos: Word32): String;
begin
  if not qpMsgBufGetFieldString(Buf, FieldId, Result, BasePos) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufRequireFieldBoolean(const Buf: TqpMessageBuffer;
         const FieldId: Word32; const BasePos: Word32): Boolean;
begin
  if not qpMsgBufGetFieldBoolean(Buf, FieldId, Result, BasePos) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufRequireFieldFloat(const Buf: TqpMessageBuffer;
         const FieldId: Word32; const BasePos: Word32): Double;
begin
  if not qpMsgBufGetFieldFloat(Buf, FieldId, Result, BasePos) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufRequireFieldBytesBuf(const Buf: TqpMessageBuffer;
         const FieldId: Word32;
         var BytesBuf; const BytesBufSize: Int32;
         const BasePos: Word32): Int32;
begin
  if not qpMsgBufGetFieldBytesBuf(Buf, FieldId, BytesBuf, BytesBufSize,
      Result, BasePos) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufRequireFieldBytes(const Buf: TqpMessageBuffer;
         const FieldId: Word32;
         const BasePos: Word32): TBytes;
begin
  if not qpMsgBufGetFieldBytes(Buf, FieldId, Result, BasePos) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufRequireFieldDateTime(const Buf: TqpMessageBuffer;
         const FieldId: Word32; const BasePos: Word32): TDateTime;
begin
  if not qpMsgBufGetFieldDateTime(Buf, FieldId, Result, BasePos) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufRequireFieldRecord(const Buf: TqpMessageBuffer;
         const FieldId: Word32; const BasePos: Word32): Word32;
begin
  if not qpMsgBufGetFieldRecord(Buf, FieldId, Result, BasePos) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;



procedure qpMsgBufIndexFields(
          var MsgBufIdx: TqpMessageBufferIndex;
          const MaxFieldId: Word32;
          const Buf: TqpMessageBuffer;
          const BasePos: Word32);
var
  IdxLen : Int32;
  BufOfs : Word32;
  BufSize : Word32;
  BufP : PByte;
  FieldType : Byte;
  FieldIdP : Pointer;
  FieldIdSize : Int32;
  FieldId : Word32;
  DataP : Pointer;
  DataSize : Word32;
  HeaderSize : Int32;
  FieldSize : Int32;
  RecSize : Int32;
begin
  MsgBufIdx.BasePos := BasePos;
  if MaxFieldId < QP_MsgBufIdx_SmallIndexLength then
    begin
      MsgBufIdx.IndexType := qpmbitSmall;
      FillChar(MsgBufIdx.IndexSmall[0], SizeOf(MsgBufIdx.IndexSmall), 0);
    end
  else
  if MaxFieldId > QP_MsgBufIdx_MaxFieldId then
    raise EqpMessageBuffer.Create(SErrMaxFieldIdTooHigh)
  else
    begin
      MsgBufIdx.IndexType := qpmbitLarge;
      IdxLen := MaxFieldId + 1;
      SetLength(MsgBufIdx.IndexLarge, IdxLen);
      FillChar(MsgBufIdx.IndexLarge[0], IdxLen * SizeOf(Word32), 0);
    end;
  BufOfs := BasePos;
  if BufOfs <= 0 then
    BufOfs := QP_MsgBuf_HeaderSize;
  BufSize := Buf.BufferSize;
  BufP := Buf.Buffer;
  Inc(BufP, BufOfs);
  while BufOfs < BufSize do
    begin
      qpMsgBufDecodeFieldHeader(BufP^, BufSize - BufOfs,
          FieldType, FieldIdP, FieldIdSize, DataP, DataSize, HeaderSize);
      if FieldType = QP_MsgBuf_FieldType_End then
        exit;
      FieldId := qpVarIntToWord32(FieldIdP^, FieldIdSize);
      if FieldId <= MaxFieldId then
        if MsgBufIdx.IndexType = qpmbitSmall then
          MsgBufIdx.IndexSmall[FieldId] := BufOfs
        else
          MsgBufIdx.IndexLarge[FieldId] := BufOfs;
      FieldSize := HeaderSize + Int32(DataSize);
      Inc(BufP, FieldSize);
      Inc(BufOfs, FieldSize);
      if FieldType = QP_MsgBuf_FieldType_Record then
        begin
          RecSize := PWord32(DataP)^;
          Inc(BufP, RecSize);
          Inc(BufOfs, RecSize);
        end;
    end;
end;

function qpMsgBufIdxGetField(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32;
         out BaseOfs: Word32; out FieldType: Byte;
         out FieldDataP: Pointer; out FieldDataSize: Word32): Boolean;
var
  FieldOfs: Word32;
  BufP : PByte;
  FieldIdP : Pointer;
  FieldIdSize : Int32;
  HeaderSize : Int32;
begin
  if BufIdx.IndexType = qpmbitSmall then
    begin
      if FieldId >= QP_MsgBufIdx_SmallIndexLength then
        begin
          Result := False;
          exit;
        end;
      FieldOfs := BufIdx.IndexSmall[FieldId];
    end
  else
    begin
      if FieldId >= Word32(Length(BufIdx.IndexLarge)) then
        begin
          Result := False;
          exit;
        end;
      FieldOfs := BufIdx.IndexLarge[FieldId];
    end;
  if FieldOfs = 0 then
    begin
      Result := False;
      exit;
    end;
  BufP := Buf.Buffer;
  Inc(BufP, FieldOfs);
  qpMsgBufDecodeFieldHeader(BufP^, Buf.BufferSize - Int32(FieldOfs),
      FieldType, FieldIdP, FieldIdSize, FieldDataP, FieldDataSize, HeaderSize);
  BaseOfs := FieldOfs + Word32(HeaderSize) + FieldDataSize - BufIdx.BasePos;
  Result := True;
end;

function qpMsgBufIdxGetFieldInt(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32; out Value: Int64): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufIdxGetField(Buf, BufIdx, FieldId, Ofs, FieldType, DataP, DataSize) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToInt(FieldType, DataP, Value);
  Result := True;
end;

function qpMsgBufIdxGetFieldUInt(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32; out Value: UInt64): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufIdxGetField(Buf, BufIdx, FieldId, Ofs, FieldType, DataP, DataSize) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToUInt(FieldType, DataP, Value);
  Result := True;
end;

function qpMsgBufIdxGetFieldString(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32; out Value: String): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufIdxGetField(Buf, BufIdx, FieldId, Ofs, FieldType, DataP, DataSize) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToString(FieldType, DataP, DataSize, Value);
  Result := True;
end;

function qpMsgBufIdxGetFieldBoolean(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32; out Value: Boolean): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufIdxGetField(Buf, BufIdx, FieldId, Ofs, FieldType, DataP, DataSize) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToBoolean(FieldType, DataP, Value);
  Result := True;
end;

function qpMsgBufIdxGetFieldFloat(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32; out Value: Double): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufIdxGetField(Buf, BufIdx, FieldId, Ofs, FieldType, DataP, DataSize) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToFloat(FieldType, DataP, Value);
  Result := True;
end;

function qpMsgBufIdxGetFieldBytes(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32; out Value: TBytes): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufIdxGetField(Buf, BufIdx, FieldId, Ofs, FieldType, DataP, DataSize) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToBytes(FieldType, DataP, DataSize, Value);
  Result := True;
end;

function qpMsgBufIdxGetFieldDateTime(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32; out Value: TDateTime): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufIdxGetField(Buf, BufIdx, FieldId, Ofs, FieldType, DataP, DataSize) then
    begin
      Result := False;
      exit;
    end;
  qpMsgBufFieldToDateTime(FieldType, DataP, Value);
  Result := True;
end;

function qpMsgBufIdxGetFieldRecord(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32; out RecordBasePos: Word32): Boolean;
var
  Ofs : Word32;
  FieldType : Byte;
  DataP : Pointer;
  DataSize : Word32;
begin
  if not qpMsgBufIdxGetField(Buf, BufIdx, FieldId, Ofs, FieldType, DataP, DataSize) then
    begin
      Result := False;
      exit;
    end;
  RecordBasePos := BufIdx.BasePos + Ofs;
  Result := True;
end;

function qpMsgBufIdxRequireFieldInt(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32): Int64;
begin
  if not qpMsgBufIdxGetFieldInt(Buf, BufIdx, FieldId, Result) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufIdxRequireFieldUInt(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32): UInt64;
begin
  if not qpMsgBufIdxGetFieldUInt(Buf, BufIdx, FieldId, Result) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufIdxRequireFieldString(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32): String;
begin
  if not qpMsgBufIdxGetFieldString(Buf, BufIdx, FieldId, Result) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufIdxRequireFieldBoolean(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32): Boolean;
begin
  if not qpMsgBufIdxGetFieldBoolean(Buf, BufIdx, FieldId, Result) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufIdxRequireFieldFloat(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32): Double;
begin
  if not qpMsgBufIdxGetFieldFloat(Buf, BufIdx, FieldId, Result) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufIdxRequireFieldBytes(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32): TBytes;
begin
  if not qpMsgBufIdxGetFieldBytes(Buf, BufIdx, FieldId, Result) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufIdxRequireFieldDateTime(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32): TDateTime;
begin
  if not qpMsgBufIdxGetFieldDateTime(Buf, BufIdx, FieldId, Result) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;

function qpMsgBufIdxRequireFieldRecord(
         const Buf: TqpMessageBuffer;
         const BufIdx: TqpMessageBufferIndex;
         const FieldId: Word32): Word32;
begin
  if not qpMsgBufIdxGetFieldRecord(Buf, BufIdx, FieldId, Result) then
    raise EqpMessageBuffer.Create(SErrFieldNotFound);
end;



function qpMsgBufIterateSetNext(
         const Buf: TqpMessageBuffer;
         const IterateOfs: Word32;
         var Iterator: TqpMessageBufferIterator): Boolean;
var
  BufOfs : Word32;
  BufSize : Word32;
  BufP : PByte;
  FieldType : Byte;
  FieldIdP : Pointer;
  FieldIdSize : Int32;
  DataP : Pointer;
  DataSize : Word32;
  HeaderSize : Int32;
begin
  BufOfs := IterateOfs;
  BufSize := Buf.BufferSize;
  if BufOfs >= BufSize then
    begin
      Result := False;
      exit;
    end;
  BufP := Buf.Buffer;
  Inc(BufP, BufOfs);
  qpMsgBufDecodeFieldHeader(BufP^, BufSize - BufOfs,
      FieldType, FieldIdP, FieldIdSize, DataP, DataSize, HeaderSize);
  if FieldType = QP_MsgBuf_FieldType_End then
    begin
      Result := False;
      exit;
    end;
  Iterator.BufOfs := BufOfs;
  Iterator.FieldType := FieldType;
  Iterator.FieldId := qpVarIntToWord32(FieldIdP^, FieldIdSize);
  Iterator.DataP := DataP;
  Iterator.DataSize := DataSize;
  Iterator.HeaderSize := HeaderSize;
  Result := True;
end;

function qpMsgBufIterateFirst(
         const Buf: TqpMessageBuffer;
         const BasePos: Word32;
         var Iterator: TqpMessageBufferIterator): Boolean;
var
  BufOfs : Word32;
begin
  BufOfs := BasePos;
  if BufOfs <= 0 then
    BufOfs := QP_MsgBuf_HeaderSize;
  Result := qpMsgBufIterateSetNext(Buf, BufOfs, Iterator);
end;

function qpMsgBufIterateNext(
         const Buf: TqpMessageBuffer;
         var Iterator: TqpMessageBufferIterator): Boolean;
var
  BufOfs : Word32;
  RecSize : Word32;
begin
  BufOfs := Iterator.BufOfs + Iterator.HeaderSize + Iterator.DataSize;
  if Iterator.FieldType = QP_MsgBuf_FieldType_Record then
    begin
      RecSize := PWord32(Iterator.DataP)^;
      Inc(BufOfs, RecSize);
    end;
  Result := qpMsgBufIterateSetNext(Buf, BufOfs, Iterator);
end;

function qpMsgBufIteratorGetFieldInt(
         const Iterator: TqpMessageBufferIterator): Int64;
begin
  qpMsgBufFieldToInt(Iterator.FieldType, Iterator.DataP, Result);
end;

function qpMsgBufIteratorGetFieldUInt(
         const Iterator: TqpMessageBufferIterator): UInt64;
begin
  qpMsgBufFieldToUInt(Iterator.FieldType, Iterator.DataP, Result);
end;

function qpMsgBufIteratorGetFieldString(
         const Iterator: TqpMessageBufferIterator): String;
begin
  qpMsgBufFieldToString(Iterator.FieldType, Iterator.DataP, Iterator.DataSize, Result);
end;

function qpMsgBufIteratorGetFieldBoolean(
         const Iterator: TqpMessageBufferIterator): Boolean;
begin
  qpMsgBufFieldToBoolean(Iterator.FieldType, Iterator.DataP, Result);
end;

function qpMsgBufIteratorGetFieldFloat(
         const Iterator: TqpMessageBufferIterator): Double;
begin
  qpMsgBufFieldToFloat(Iterator.FieldType, Iterator.DataP, Result);
end;

function qpMsgBufIteratorGetFieldBytes(
         const Iterator: TqpMessageBufferIterator): TBytes;
begin
  qpMsgBufFieldToBytes(Iterator.FieldType, Iterator.DataP, Iterator.DataSize, Result);
end;

function qpMsgBufIteratorGetFieldDateTime(
         const Iterator: TqpMessageBufferIterator): TDateTime;
begin
  qpMsgBufFieldToDateTime(Iterator.FieldType, Iterator.DataP, Result);
end;

function qpMsgBufIteratorGetFieldRecord(
         const Iterator: TqpMessageBufferIterator): Word32;
begin
  if Iterator.FieldType <> QP_MsgBuf_FieldType_Record then
    raise EqpMessageBuffer.Create(SErrInvalidFieldType);
  Result := Iterator.BufOfs + Iterator.HeaderSize + Iterator.DataSize;
end;



procedure Test;
var
  M : TqpMessageBuffer;
  ValI : Int64;
  ValU : UInt64;
  ValS : String;
  ValB : Boolean;
  ValR : Word32;
  ValF : Double;
  ValBy : TBytes;
  Size : Int32;
  MI : TqpMessageBufferIndex;
  It : TqpMessageBufferIterator;
  It2 : TqpMessageBufferIterator;
begin
  { Initialise }

  qpMsgBufInit(M);



  { Add }

  qpMsgBufAddFieldInt(M, 1, 1000);
  qpMsgBufAddFieldString(M, 2, 'ABC');
  qpMsgBufAddFieldBoolean(M, 3, True);
  qpMsgBufAddFieldFloat(M, 100, 1.5);
  qpMsgBufAddFieldRecordStart(M, 4);
  qpMsgBufAddFieldInt(M, 1, 2);
  qpMsgBufAddFieldInt64(M, 98, 3);
  qpMsgBufAddFieldInt32(M, 99, 4);
  qpMsgBufAddFieldRecordEnd(M);
  qpMsgBufAddFieldInt(M, 99, 5);
  ValI := 111;
  qpMsgBufAddFieldBytesPtr(M, 110, @ValI, SizeOf(ValI));
  SetLength(ValBy, 2);
  ValBy[0] := 201;
  ValBy[1] := 202;
  qpMsgBufAddFieldBytes(M, 111, ValBy);
  qpMsgBufAddFieldUInt(M, 112, $12345678);



  { GetField }

  Assert(not qpMsgBufGetFieldInt(M, 0, ValI));

  Assert(qpMsgBufGetFieldInt(M, 1, ValI));
  Assert(ValI = 1000);

  Assert(qpMsgBufGetFieldString(M, 2, ValS));
  Assert(ValS = 'ABC');

  Assert(qpMsgBufGetFieldBoolean(M, 3, ValB));
  Assert(ValB);

  Assert(qpMsgBufGetFieldFloat(M, 100, ValF));
  Assert(ValF = 1.5);

  Assert(not qpMsgBufGetFieldInt(M, 98, ValI));

  Assert(qpMsgBufGetFieldRecord(M, 4, ValR));
  Assert(ValR > 0);

  Assert(qpMsgBufGetFieldInt(M, 1, ValI, ValR));
  Assert(ValI = 2);

  Assert(qpMsgBufGetFieldInt(M, 98, ValI, ValR));
  Assert(ValI = 3);

  Assert(qpMsgBufGetFieldInt(M, 99, ValI, ValR));
  Assert(ValI = 4);

  Assert(qpMsgBufGetFieldInt(M, 99, ValI));
  Assert(ValI = 5);

  Assert(qpMsgBufGetFieldBytesBufSize(M, 110, Size));
  Assert(Size = SizeOf(ValI));
  Assert(qpMsgBufGetFieldBytesBuf(M, 110, ValI, SizeOf(ValI), Size));
  Assert(Size = SizeOf(ValI));
  Assert(ValI = 111);

  Assert(qpMsgBufGetFieldBytesBufSize(M, 111, Size));
  Assert(Size = 2);
  ValBy := nil;
  Assert(qpMsgBufGetFieldBytes(M, 111, ValBy));
  Assert(Length(ValBy) = 2);
  Assert(ValBy[0] = 201);
  Assert(ValBy[1] = 202);

  Assert(qpMsgBufGetFieldUInt(M, 112, ValU));
  Assert(ValU = $12345678);



  { RequireField }

  Assert(qpMsgBufRequireFieldInt(M, 1) = 1000);
  Assert(qpMsgBufRequireFieldString(M, 2) = 'ABC');
  Assert(qpMsgBufRequireFieldBoolean(M, 3) = True);
  Assert(qpMsgBufRequireFieldFloat(M, 100) = 1.5);
  Assert(qpMsgBufRequireFieldInt(M, 99) = 5);
  Assert(qpMsgBufRequireFieldBytesBuf(M, 110, ValI, SizeOf(ValI)) = SizeOf(ValI));
  Assert(ValI = 111);
  Assert(qpMsgBufRequireFieldUInt(M, 112) = $12345678);



  { Index }

  qpMsgBufIndexFields(MI, 200, M, 0);

  Assert(not qpMsgBufIdxGetFieldInt(M, MI, 0, ValI));

  Assert(qpMsgBufIdxGetFieldInt(M, MI, 1, ValI));
  Assert(ValI = 1000);

  Assert(qpMsgBufIdxGetFieldString(M, MI, 2, ValS));
  Assert(ValS = 'ABC');

  Assert(qpMsgBufIdxGetFieldBoolean(M, MI, 3, ValB));
  Assert(ValB);

  Assert(qpMsgBufIdxGetFieldFloat(M, MI, 100, ValF));
  Assert(ValF = 1.5);

  Assert(not qpMsgBufIdxGetFieldInt(M, MI, 98, ValI));

  Assert(qpMsgBufIdxGetFieldRecord(M, MI, 4, ValR));
  Assert(ValR > 0);

  Assert(qpMsgBufGetFieldInt(M, 1, ValI, ValR));
  Assert(ValI = 2);

  Assert(qpMsgBufGetFieldInt(M, 98, ValI, ValR));
  Assert(ValI = 3);

  Assert(qpMsgBufGetFieldInt(M, 99, ValI, ValR));
  Assert(ValI = 4);

  Assert(qpMsgBufIdxGetFieldInt(M, MI, 99, ValI));
  Assert(ValI = 5);

  Assert(qpMsgBufIdxGetFieldUInt(M, MI, 112, ValU));
  Assert(ValU = $12345678);



  { Iterate }

  Assert(qpMsgBufIterateFirst(M, 0, It));
  Assert(It.FieldType = QP_MsgBuf_FieldType_Int16);
  Assert(It.FieldId = 1);
  Assert(qpMsgBufIteratorGetFieldInt(It) = 1000);

  Assert(qpMsgBufIterateNext(M, It));
  Assert(It.FieldType = QP_MsgBuf_FieldType_WString_Size1);
  Assert(It.FieldId = 2);
  Assert(qpMsgBufIteratorGetFieldString(It) = 'ABC');

  Assert(qpMsgBufIterateNext(M, It));
  Assert(It.FieldType = QP_MsgBuf_FieldType_Boolean_True);
  Assert(It.FieldId = 3);
  Assert(qpMsgBufIteratorGetFieldBoolean(It));

  Assert(qpMsgBufIterateNext(M, It));
  Assert(It.FieldType = QP_MsgBuf_FieldType_Double);
  Assert(It.FieldId = 100);
  Assert(qpMsgBufIteratorGetFieldFloat(It) = 1.5);

  Assert(qpMsgBufIterateNext(M, It));
  Assert(It.FieldType = QP_MsgBuf_FieldType_Record);
  Assert(It.FieldId = 4);
  ValR := qpMsgBufIteratorGetFieldRecord(It);

  Assert(qpMsgBufGetFieldInt(M, 1, ValI, ValR));
  Assert(ValI = 2);

  Assert(qpMsgBufIterateFirst(M, ValR, It2));
  Assert(It2.FieldType = QP_MsgBuf_FieldType_Int8);
  Assert(It2.FieldId = 1);
  Assert(qpMsgBufIteratorGetFieldInt(It2) = 2);

  Assert(qpMsgBufIterateNext(M, It2));
  Assert(It2.FieldType = QP_MsgBuf_FieldType_Int64);
  Assert(It2.FieldId = 98);
  Assert(qpMsgBufIteratorGetFieldInt(It2) = 3);

  Assert(qpMsgBufIterateNext(M, It2));
  Assert(It2.FieldType = QP_MsgBuf_FieldType_Int32);
  Assert(It2.FieldId = 99);
  Assert(qpMsgBufIteratorGetFieldInt(It2) = 4);

  Assert(not qpMsgBufIterateNext(M, It2));

  Assert(qpMsgBufIterateNext(M, It));
  Assert(It.FieldType = QP_MsgBuf_FieldType_Int8);
  Assert(It.FieldId = 99);
  Assert(qpMsgBufIteratorGetFieldInt(It) = 5);

  Assert(qpMsgBufIterateNext(M, It));
  Assert(It.FieldType = QP_MsgBuf_FieldType_Bytes_Size1);
  Assert(It.FieldId = 110);
  ValBy := qpMsgBufIteratorGetFieldBytes(It);
  Assert(Length(ValBy) = SizeOf(ValI));
  Move(ValBy[0], ValI, SizeOf(ValI));
  Assert(ValI = 111);

  Assert(qpMsgBufIterateNext(M, It));
  Assert(It.FieldId = 111);
  Assert(It.FieldType = QP_MsgBuf_FieldType_Bytes_Size1);
  ValBy := qpMsgBufIteratorGetFieldBytes(It);
  Assert(Length(ValBy) = 2);
  Assert(ValBy[0] = 201);
  Assert(ValBy[1] = 202);

  Assert(qpMsgBufIterateNext(M, It));
  Assert(It.FieldType = QP_MsgBuf_FieldType_Word32);
  Assert(It.FieldId = 112);
  Assert(qpMsgBufIteratorGetFieldUInt(It) = $12345678);

  Assert(not qpMsgBufIterateNext(M, It));



  { Finalise }

  qpMsgBufFinalise(M);
end;



end.

