@echo off
title FastTube - Build Script
color 0A
echo.
echo  ============================================
echo          FastTube - Windows .EXE Build
echo  ============================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found. Install from python.org
    pause
    exit /b 1
)
echo  [OK] Python found.

:: Install / upgrade yt-dlp
echo  [INFO] Installing/upgrading yt-dlp...
python -m pip install -q --upgrade yt-dlp
echo  [OK] yt-dlp ready.

:: Install PyInstaller
echo  [INFO] Installing/upgrading PyInstaller...
python -m pip install -q --upgrade pyinstaller
echo  [OK] PyInstaller ready.

echo.
echo  [INFO] Building FastTube.exe (this may take 1-2 minutes)...
echo.

python -m PyInstaller ^
    --onefile ^
    --windowed ^
    --name FastTube ^
    --icon assets\icon.ico ^
    --add-data "downloads;downloads" ^
    fasttube.py

if errorlevel 1 (
    echo.
    echo  [ERROR] Build failed! Check output above.
    pause
    exit /b 1
)

echo.
echo  ============================================
echo   Build successful!
echo   File: dist\FastTube.exe
echo   NOTE: FFmpeg must be installed on target PC
echo  ============================================
echo.
pause