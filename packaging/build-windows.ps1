$ErrorActionPreference = "Stop"

# Build standalone Windows executable with PyInstaller
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

python -m venv .venv-build
. .\.venv-build\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install .[dev]

# Clean previous dist/build
Remove-Item -Recurse -Force dist, build -ErrorAction SilentlyContinue

# On Windows, use ';' as the add-data separator
pyinstaller `
  --name Calishot-Web `
  --onefile `
  --noconfirm `
  --clean `
  --add-data "calishot_web\\templates;calishot_web\\templates" `
  --add-data "calishot_web\\static;calishot_web\\static" `
  --add-data "data;data" `
  -p . `
  calishot_web\cli.py

Write-Host "`nBuilt app at: dist\\Calishot-Web.exe"
