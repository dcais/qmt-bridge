@echo off
setlocal
if not "%~2"=="" goto usage
if "%~1"=="staging" goto start
if "%~1"=="prd" goto start
goto usage

:start
pushd "%~dp0"
if errorlevel 1 exit /b 1
if not exist ".venv\Scripts\python.exe" (
    echo ERROR: Missing .venv\Scripts\python.exe. Set up the project virtual environment first.
    popd
    exit /b 1
)
if not exist "service\ipc_bridge\.env.%~1" (
    echo ERROR: Missing service\ipc_bridge\.env.%~1. Copy and configure its .example template first.
    popd
    exit /b 1
)
".venv\Scripts\python.exe" -m service.ipc_bridge --env-file "service\ipc_bridge\.env.%~1"
set "bridge_exit_code=%errorlevel%"
popd
exit /b %bridge_exit_code%

:usage
echo Usage: start_ipc_bridge staging ^| prd
exit /b 2
