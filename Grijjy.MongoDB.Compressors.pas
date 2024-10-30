unit Grijjy.MongoDB.Compressors;

interface

uses system.SysUtils, system.ZLib, Snappy;

function Snappy_Implemented: Boolean;

const
  ZLIB_Implemented = True;

type
  tNoopCompressor = class
    class function Compress(adata: Pointer; aUncompressedSize: Integer; out aCompressedSize: Integer; out Output: tBytes): Boolean;
    class function Expand(adata: Pointer; aCompressedSize, aUncompressedSize:
      Integer; out Output: tBytes): Boolean;
  end;

  tZlibCompressor = class
    class function Compress(adata: Pointer; aUncompressedSize: Integer; out aCompressedSize: Integer; out Output: tBytes): Boolean;
    class function Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; out OutputBuffer: tBytes): Boolean;
  end;

  // currently support Snappy only under Windows
  tSnappyCompressor = class
    class function Compress(adata: Pointer; aUncompressedSize: Integer; out aCompressedSize: Integer; out Output: tBytes): Boolean;
    class function Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; out Output: tBytes): Boolean;
  end;

implementation

{$HINTS OFF}

function Snappy_Implemented: Boolean;
begin
  result := Snappy.Snappy_Implemented;
end;

class function tSnappyCompressor.Compress(adata: Pointer; aUncompressedSize: Integer; out aCompressedSize: Integer; out Output: tBytes):
  Boolean;
var
  OutSize: Integer;
  OutBuffer: Pointer;
var
  OutLen: NativeUInt;
  Status: Snappy_Status;
begin
  result := false;
  setlength(Output, 0);
  aCompressedSize := 0;
  if Snappy_Implemented then
  begin
    if aUncompressedSize <= 0 then
      Exit
    else
    begin
      OutLen := snappy_max_compressed_length(aUncompressedSize); //worst-case
      setlength(Output, OutLen);
      Status := snappy_compress(adata, aUncompressedSize, @Output[0], OutLen);
      if Status = Snappy_OK then
      begin
        result := True;
        aCompressedSize := OutLen;
        setlength(Output, aCompressedSize); // truncate
      end;
    end;
  end;
end;

class function tSnappyCompressor.Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; out Output: tBytes): Boolean;
var
  ExpandedSize: NativeUInt;
  Status: Snappy_Status;
begin
  result := false;
  setlength(Output, 0);
  if Snappy_Implemented then
  begin
    if aCompressedSize <= 0 then
      Exit
    else
    begin
      Status := snappy_uncompressed_length(adata, aCompressedSize, ExpandedSize);
      result := (Status = Snappy_OK);
      if result then
      begin
        result := (ExpandedSize = aUncompressedSize);
        if result then
        begin
          setlength(Output, ExpandedSize);
          Status := snappy_uncompress(adata, aCompressedSize, @Output[0], ExpandedSize);
          result := (Status = Snappy_OK);
        end ;
      end;
    end;
  end;
end;

class function tNoopCompressor.Compress(adata: Pointer; aUncompressedSize: Integer; out aCompressedSize: Integer; out Output: tBytes):
  Boolean;
begin
  aCompressedSize := aUncompressedSize;
  setlength(Output, aUncompressedSize);
  Move(adata^, Output[0], aUncompressedSize);
  result := True;
end;

class function tNoopCompressor.Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; out Output: tBytes): Boolean;
begin
  setlength(Output, aCompressedSize);
  if aCompressedSize > 0 then
    Move(adata^, Output[0], aCompressedSize);
  result := True;
end;

class function tZlibCompressor.Compress(adata: Pointer; aUncompressedSize: Integer; out aCompressedSize: Integer; out Output: tBytes):
  Boolean;
var
  OutSize: Integer;
  tempBuffer: Pointer;
begin
  result := false;
  setlength(Output, 0);
  aCompressedSize := 0;
  if aUncompressedSize <= 0 then
    Exit
  else
  begin
    zcompress(adata, aUncompressedSize, tempBuffer, aCompressedSize, clDefault);
    if aCompressedSize > 0 then
    begin
      result := True;
      setlength(Output, aCompressedSize);
      Move(tempBuffer^, Output[0], aCompressedSize);
      FreeMem(tempBuffer);
    end;
  end;
end;

class function tZlibCompressor.Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; out OutputBuffer: tBytes): Boolean;
var
  ExpandedSize: Integer;
  tempBuffer: Pointer;
begin
  result := false;
  setlength(OutputBuffer, 0);
  if aCompressedSize <= 0 then
    Exit(false)
  else
  begin
    ZDecompress(adata, aCompressedSize, tempBuffer, ExpandedSize, 0);
    if ExpandedSize = aUncompressedSize then
    begin
      result := True;
      setlength(OutputBuffer, aUncompressedSize);
      Move(tempBuffer^, OutputBuffer[0], aUncompressedSize);
      FreeMem(tempBuffer);
    end;
  end;
end;

end.

