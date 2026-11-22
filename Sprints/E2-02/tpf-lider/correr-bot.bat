@echo off
rem Arranca o lider. Uso: correr-bot.bat [chunkMax]
rem Sem chunkMax, usa 4096 para /rajada 500 0 produzir varios chunks por tamanho.
setlocal EnableExtensions
cd /d "%~dp0"

set "GUILD=1550233395143901226"
set "CHUNKMAX=%~1"
if "%CHUNKMAX%"=="" set "CHUNKMAX=4096"

echo [tpf] pasta     = %CD%
echo [tpf] guild     = %GUILD%
echo [tpf] chunkMax  = %CHUNKMAX%
mvn -q compile exec:java -Dtpf.guild=%GUILD% -Dtpf.chunkMax=%CHUNKMAX%

set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" echo [tpf] o lider terminou com codigo %RC%
endlocal & exit /b %RC%
