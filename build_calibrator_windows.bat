@echo off
REM Build standalone MHZ19C-Cal.exe with PyInstaller
REM Usage: build_calibrator_windows.bat

where pyinstaller >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
  echo PyInstaller not found. Install with: py -m pip install pyinstaller
  exit /b 1
)

set APP_NAME=MHZ19C-Cal
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist %APP_NAME%.spec del %APP_NAME%.spec

pyinstaller ^
  --noconsole ^
  --onefile ^
  --name %APP_NAME% ^
  --hidden-import matplotlib.backends.backend_tkagg ^
  mhz19c_calibrate.py

echo Build complete: dist\%APP_NAME%.exe

