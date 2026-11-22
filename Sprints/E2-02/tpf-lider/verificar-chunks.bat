@echo off
rem Verifica os criterios de chunk do E2-02 sobre a pasta de dados do lider.
rem Uso: verificar-chunks.bat [pasta-de-dados] [PAD_BYTES]
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

set "DADOS=%~1"
if "%DADOS%"=="" set "DADOS=dados-lider"
set "PAD=%~2"
if "%PAD%"=="" set "PAD=4096"

set "LOG=%DADOS%\registos.log"
set "EV=%DADOS%\eventos-lider.txt"
set "SEL=%DADOS%\selados"
set "FALHAS=0"

if not exist "%EV%" goto :semficheiros
if not exist "%LOG%" goto :semficheiros
if not exist "%SEL%\" goto :semficheiros
goto :inicio

:semficheiros
echo ERRO: faltam ficheiros em "%DADOS%" ^(registos.log, eventos-lider.txt ou selados\^)
set "FALHAS=1"
goto :fim

:inicio
echo == 1. um SELADO por chunk, intervalos contiguos, bytes multiplo de %PAD% ==
set "N_EV=0"
set "ESPERADO=1"
for /f "tokens=3 delims=;" %%d in ('findstr /c:";SELADO;" "%EV%"') do (
    set /a N_EV+=1
    for /f "tokens=2,3 delims= " %%a in ("%%d") do (
        set "LSN=%%a"
        set "BYTES=%%b"
    )
    set "LSN=!LSN:lsn=!"
    set "BYTES=!BYTES:bytes=!"
    for /f "tokens=1,2 delims=-" %%x in ("!LSN!") do (
        set "DE=%%x"
        set "ATE=%%y"
    )
    if not "!DE!"=="!ESPERADO!" call :erro "intervalo nao contiguo: esperava !ESPERADO!, veio !DE!"
    set /a "RESTO=!BYTES! %% %PAD%"
    if not "!RESTO!"=="0" call :erro "bytes=!BYTES! nao e multiplo de %PAD%"
    set /a "ESPERADO=!ATE!+1"
)
echo SELADO=!N_EV!

echo == 2. um .bin e um .hdr por chunk ==
set "N_BIN=0"
if exist "%SEL%\*.bin" for %%f in ("%SEL%\*.bin") do set /a N_BIN+=1
set "N_HDR=0"
if exist "%SEL%\*.hdr" for %%f in ("%SEL%\*.hdr") do set /a N_HDR+=1
echo .bin=!N_BIN!  .hdr=!N_HDR!
if not "!N_EV!"=="!N_BIN!" call :erro "numero de .bin diferente do numero de SELADO"
if not "!N_BIN!"=="!N_HDR!" call :erro "numero de .hdr diferente do numero de .bin"

echo == 3. nome do .bin = SHA-256 do conteudo ==
if exist "%SEL%\*.bin" for %%f in ("%SEL%\*.bin") do (
    set "NOME=%%~nf"
    set "HASH="
    for /f "skip=1 delims=" %%h in ('certutil -hashfile "%%f" SHA256') do (
        if not defined HASH set "HASH=%%h"
    )
    set "HASH=!HASH: =!"
    if /i not "!NOME!"=="!HASH!" call :erro "%%~nxf: nome diferente do SHA-256 do conteudo"
)

echo == 4. .hdr = keyVersion;salt;nonce ==
if exist "%SEL%\*.hdr" for %%f in ("%SEL%\*.hdr") do (
    for /f "tokens=1,2,3,* delims=;" %%a in ('type "%%f"') do (
        if "%%a"=="" call :erro "%%~nxf: keyVersion vazio"
        if "%%b"=="" call :erro "%%~nxf: salt vazio"
        if "%%c"=="" call :erro "%%~nxf: nonce vazio"
        if not "%%d"=="" call :erro "%%~nxf: mais de 3 campos"
    )
)

echo == 5. nenhum texto registado em claro ==
findstr /m /c:"rajada " "%SEL%\*.bin" >nul 2>&1
if not errorlevel 1 (call :erro "ha texto 'rajada ' dentro de selados") else echo ok

echo == 6. registos.log sem saltos e coberto pelos SELADO ==
set "ULTIMO=0"
for /f "tokens=1 delims=;" %%l in ('type "%LOG%"') do (
    set /a "ESPERA=!ULTIMO!+1"
    if not "%%l"=="!ESPERA!" call :erro "LSN fora de sequencia: esperava !ESPERA!, veio %%l"
    set "ULTIMO=%%l"
)
echo ultimo LSN do log = !ULTIMO!
set /a "COBERTO=!ULTIMO!+1"
if not "!ESPERADO!"=="!COBERTO!" call :erro "os SELADO nao cobrem 1..!ULTIMO!"

:fim
echo.
if "!FALHAS!"=="0" (
    echo RESULTADO: OK - 0 falhas
) else (
    echo RESULTADO: !FALHAS! falha^(s^)
)
if "!FALHAS!"=="0" (endlocal & exit /b 0)
endlocal & exit /b 1

:erro
echo ERRO: %~1
set /a FALHAS+=1
goto :eof
