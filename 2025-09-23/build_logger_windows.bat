@echo off
REM Build standalone CO2Logger.exe with PyInstaller
REM Usage: build_logger_windows.bat

where pyinstaller >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
  echo PyInstaller not found. Install with: py -m pip install pyinstaller
  exit /b 1
)

set APP_NAME=CO2Logger
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist %APP_NAME%.spec del %APP_NAME%.spec

REM On Windows, --add-data uses ';' separator
pyinstaller ^
  --noconsole ^
  --onefile ^
  --name %APP_NAME% ^
  --hidden-import matplotlib.backends.backend_tkagg ^
  --add-data "ports_config.json;." ^
  multichannel_usb_logger.py

echo Build complete: dist\%APP_NAME%.exe

