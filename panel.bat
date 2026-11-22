@echo off
where pyw >nul 2>nul
if errorlevel 1 (
    py -B "%~dp0server.py" --panel %*
) else (
    start "" pyw -B "%~dp0server.py" --panel %*
)
