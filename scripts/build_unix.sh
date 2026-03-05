#!/bin/bash
echo ""
echo "============================================"
echo "      FastTube - macOS/Linux Build"
echo "============================================"
echo ""

# Check Python3
if ! command -v python3 &>/dev/null; then
    echo "[ERROR] Python3 not found."
    exit 1
fi
echo "[OK] Python3: $(python3 --version)"

# Install/upgrade yt-dlp
echo "[INFO] Installing/upgrading yt-dlp..."
pip3 install -q --upgrade yt-dlp
echo "[OK] yt-dlp ready."

# Install PyInstaller
echo "[INFO] Installing/upgrading PyInstaller..."
pip3 install -q --upgrade pyinstaller
echo "[OK] PyInstaller ready."

echo ""
echo "[INFO] Building FastTube binary..."
echo ""

python3 -m PyInstaller \
    --onefile \
    --windowed \
    --name FastTube \
    fasttube.py

if [ $? -ne 0 ]; then
    echo ""
    echo "[ERROR] Build failed!"
    exit 1
fi

echo ""
echo "============================================"
echo " Build successful!"
echo " File: dist/FastTube"
echo " NOTE: FFmpeg must be installed on target machine"
echo "============================================"