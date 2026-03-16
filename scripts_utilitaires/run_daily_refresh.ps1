$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot
$env:PYTHONPATH = $projectRoot

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if ($pythonCmd) {
    & $pythonCmd.Source "$projectRoot\main.py"
    exit $LASTEXITCODE
}

$pyCmd = Get-Command py -ErrorAction SilentlyContinue
if ($pyCmd) {
    & $pyCmd.Source -3 "$projectRoot\main.py"
    exit $LASTEXITCODE
}

throw "Python est introuvable. Installe Python ou ajoute-le au PATH avant d'automatiser ce script."
