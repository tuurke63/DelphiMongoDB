unit Grijjy.System.Logging;

interface

type
  TgoLog = (ToFile, ToConsole, ToDefault);
  TgoLogs = set of TgoLog;

  TgoLogging = class
  public
    constructor Create(aMode: TgoLogs; aName: String);
    procedure Send(aMessage: String);
  end;

implementation
uses
  Winapi.Windows;

constructor TgoLogging.Create(aMode: TgoLogs; aName: String);
begin
end;

procedure TgoLogging.Send(aMessage: String);
begin
{$IFDEF MSWINDOWS}
  OutputDebugString(PChar(aMessage));
{$ENDIF}
end;

end.