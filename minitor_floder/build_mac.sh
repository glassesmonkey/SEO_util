#!/bin/bash
echo "Cleaning previous builds..."
rm -rf build dist
echo "Installing requirements..."
pip install -r requirements.txt
echo "Building executable..."
pyinstaller --clean game_monitor.spec
echo "Build complete!"