# Cross-platform installer launcher (Windows)
# This script intentionally does NOT copy any repo data/ or books/ files.
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
python "$scriptDir\install.py" @args
