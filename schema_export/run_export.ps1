# PowerShell script to run schema export

# Check if Python is installed
if (Get-Command python -ErrorAction SilentlyContinue) {
    $pythonCmd = "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    $pythonCmd = "python3"
} else {
    Write-Error "Python is not installed or not in PATH. Please install Python 3.6+ and try again."
    exit 1
}

# Check if required Python packages are installed
Write-Host "Checking required packages..."
$packages = @("psycopg2", "pyyaml")
foreach ($package in $packages) {
    $checkPackage = & $pythonCmd -c "import $package" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Installing $package..."
        & $pythonCmd -m pip install $package
    }
}

# Load environment variables from .env file if it exists
if (Test-Path "../.env") {
    Write-Host "Loading environment variables from .env file..."
    Get-Content "../.env" | ForEach-Object {
        if (![string]::IsNullOrWhiteSpace($_) -and !$_.StartsWith("#")) {
            $key, $value = $_ -split '=', 2
            [Environment]::SetEnvironmentVariable($key, $value)
        }
    }
}

# Run the export script
Write-Host "Running schema export..."
& $pythonCmd export_schemas.py

Write-Host "Schema export completed!" 