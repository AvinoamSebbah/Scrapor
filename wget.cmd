@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "out="
set "url="
set "expect_out="

:parse
if "%~1"=="" goto run
set "arg=%~1"
if defined expect_out (
  set "out=%~1"
  set "expect_out="
) else if /I "!arg!"=="--output-document" (
  set "expect_out=1"
) else if /I "!arg:~0,18!"=="--output-document=" (
  set "out=!arg:~18!"
) else if /I "!arg!"=="-O" (
  set "expect_out=1"
) else (
  set "url=%~1"
)
shift
goto parse

:run
if "!url:~0,1!"=="'" set "url=!url:~1!"
if "!url:~-1!"=="'" set "url=!url:~0,-1!"

if not defined out (
  echo wget.cmd: missing --output-document argument 1>&2
  exit /b 2
)

if not defined url (
  echo wget.cmd: missing URL argument 1>&2
  exit /b 2
)

curl.exe -L --fail --silent --show-error --output "!out!" "!url!"
exit /b %ERRORLEVEL%
