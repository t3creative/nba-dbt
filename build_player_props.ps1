# PowerShell script for building player props models in the correct order

# Run staging models first
Write-Host "Building staging models..." -ForegroundColor Cyan
dbt run --select stg__player_props stg__player_index stg__teams

# Run intermediate models in sequence
Write-Host "Building normalized props model..." -ForegroundColor Cyan
dbt run --select int__player_props_normalized

Write-Host "Building market analysis model..." -ForegroundColor Cyan
dbt run --select int__player_prop_market_analysis

Write-Host "Build complete!" -ForegroundColor Green 