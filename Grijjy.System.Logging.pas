unit Grijjy.System.Logging;

interface
uses
  system.sysutils,
  System.Classes;

  {re-implementation of Grijjy.System.Logging which is unfortunately
   missing in the GrijjyFoundation repository }

type
  TgoLog = (ToFile, ToConsole, ToDefault);
  TgoLogs = set of TgoLog;

  igoLogging = interface
    ['{8D5118AE-10F8-4A4D-917F-A813D09CCB15}']
    procedure Send(const aMessage: string);
  end;

  TgoLogging = class(tinterfacedobject, igoLogging)
  protected
    fname:String;
  public
    class function NowStr:String; static;
    constructor Create(aMode: TgoLogs; aName: string);
    procedure Send(const aMessage: string);
  end;

implementation

{$IFDEF MSWINDOWS}
uses Winapi.Windows;
{$ENDIF}

constructor TgoLogging.Create(aMode: TgoLogs; aName: string);
begin
  inherited Create;
  fname:=aname;
  //dummy
end;

class function TgoLogging.NowStr: String;
var tdt:tdatetime;
begin
   tdt:=Now;
   DateTimeToString(Result, 'hh:nn:ss.zzz', tdt);
end;

procedure TgoLogging.Send(const aMessage: string);
begin
{$IFDEF MSWINDOWS}
  OutputDebugString(PChar(nowstr+' Grijjy.'+fname + ' -->'+amessage));
{$ENDIF}
end;

end.

