Write-Host "1. IMPORT CHECK"
python -c "import core.run_bot_preflight"
python -c "import core.run_bot_ws"

Write-Host "2. PREFLIGHT CHECK"
python -m core.run_bot_preflight --preflight-only

Write-Host "3. ENGINE STRUCTURE CHECK"
Get-ChildItem -Recurse -Filter *.py | Select-String "run_bot_preflight"
Get-ChildItem -Recurse -Filter *.py | Select-String "run_bot_ws"

Write-Host "VERIFY COMPLETE"