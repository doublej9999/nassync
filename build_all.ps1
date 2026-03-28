param(
    [string]$Python = "python",
    [string]$LinuxImage = "python:3.11-bullseye",
    [switch]$SkipWindows,
    [switch]$SkipLinux
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($SkipWindows -and $SkipLinux) {
    throw "Cannot skip both Windows and Linux builds. Keep at least one target platform."
}

$ProjectRoot = $PSScriptRoot
Set-Location $ProjectRoot

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "[STEP] $Message" -ForegroundColor Cyan
}

function Assert-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Command not found: $Name"
    }
}

function Run-External {
    param(
        [string]$Description,
        [string]$FilePath,
        [string[]]$Arguments
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

function Build-Windows {
    Write-Step "Build Windows executable"
    Assert-Command $Python

    $venvPython = Join-Path $ProjectRoot ".venv-build-win\Scripts\python.exe"
    if (-not (Test-Path $venvPython)) {
        Write-Step "Create Windows build virtualenv .venv-build-win"
        Run-External "Create virtualenv" $Python @("-m", "venv", ".venv-build-win")
    }

    Write-Step "Install Windows build dependencies"
    Run-External "Install pip" $venvPython @("-m", "pip", "install", "--upgrade", "pip")
    Run-External "Install build deps" $venvPython @("-m", "pip", "install", "--upgrade", "pyinstaller", "psycopg2-binary", "watchdog")

    if (Test-Path "build/windows") {
        Remove-Item -Recurse -Force "build/windows"
    }
    if (Test-Path "dist/windows") {
        Remove-Item -Recurse -Force "dist/windows"
    }

    Write-Step "Run PyInstaller for Windows"
    Run-External "PyInstaller Windows build" $venvPython @("-m", "PyInstaller", "nassync.spec", "--noconfirm", "--clean", "--workpath", "build/windows", "--distpath", "dist/windows")

    if (Test-Path "config.example.json") {
        Copy-Item "config.example.json" "dist/windows/config.json" -Force
    }
}

function Build-Linux {
    Write-Step "Build Linux executable with Docker"
    Assert-Command "docker"

    Run-External "Docker availability check" "docker" @("version")

    if (Test-Path "build/linux") {
        Remove-Item -Recurse -Force "build/linux"
    }
    if (Test-Path "dist/linux") {
        Remove-Item -Recurse -Force "dist/linux"
    }

    $dockerWorkdir = "/work"
    $mountPath = "${ProjectRoot}:$dockerWorkdir"
    $linuxBuildCommand = @'
set -euo pipefail
python -m pip install --upgrade pip
pip install --upgrade pyinstaller psycopg2-binary watchdog
pyinstaller nassync.spec --noconfirm --clean --workpath build/linux --distpath dist/linux
if [ -f config.example.json ]; then
  cp -f config.example.json dist/linux/config.json
fi
'@
    # 避免 Windows CRLF 传入 bash 导致 `pipefail\r` 报错
    $linuxBuildCommand = $linuxBuildCommand -replace "`r", ""

    Run-External "Docker Linux build" "docker" @("run", "--rm", "-v", $mountPath, "-w", $dockerWorkdir, $LinuxImage, "bash", "-lc", $linuxBuildCommand)
}

if (-not $SkipWindows) {
    Build-Windows
}

if (-not $SkipLinux) {
    Build-Linux
}

Write-Host ""
Write-Host "Build finished." -ForegroundColor Green
if (-not $SkipWindows) {
    Write-Host "Windows output: $ProjectRoot\dist\windows"
}
if (-not $SkipLinux) {
    Write-Host "Linux output: $ProjectRoot\dist\linux"
}
