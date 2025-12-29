Write-Host "=============================="
Write-Host " IoT Platform Health Check"
Write-Host "==============================`n"

$errors = 0

function Check {
    param (
        [string]$Name,
        [scriptblock]$Test
    )
    try {
        & $Test
        Write-Host "[OK]   $Name" -ForegroundColor Green
    }
    catch {
        Write-Host "[FAIL] $Name" -ForegroundColor Red
        Write-Host "       $($_.Exception.Message)" -ForegroundColor DarkRed
        $script:errors++
    }
}

# -------------------------------
# 1. Gateway / iot-controller
# -------------------------------
Check "Gateway → iot-controller (/metrics)" {
    $r = Invoke-WebRequest -UseBasicParsing http://localhost:8000/metrics -TimeoutSec 5
    if ($r.StatusCode -ne 200) { throw "HTTP $($r.StatusCode)" }
}

# -------------------------------
# 2. iot-controller ingest
# -------------------------------
Check "iot-controller ingest (/ingest)" {
    $body = @{
        device_id    = 9001
        device_type  = "crane"
        location     = "yard-A"
        load_weight  = 25
        status       = "operating"
        temperature  = 30
        timestamp    = (Get-Date).ToUniversalTime().ToString("o")
    } | ConvertTo-Json

    $r = Invoke-RestMethod `
        -Uri http://localhost:8000/ingest `
        -Method POST `
        -ContentType "application/json" `
        -Body $body

    if (-not $r.inserted_id) { throw "No inserted_id returned" }
}

# -------------------------------
# 3. RabbitMQ management UI
# -------------------------------
Check "RabbitMQ management UI" {
    $r = Invoke-WebRequest -UseBasicParsing http://localhost:15672 -TimeoutSec 5
    if ($r.StatusCode -notin 200,302) { throw "HTTP $($r.StatusCode)" }
}

# -------------------------------
# 4. rule-engine metrics
# -------------------------------
Check "rule-engine (/metrics)" {
    $r = Invoke-WebRequest -UseBasicParsing http://localhost:9101/metrics -TimeoutSec 5
    if ($r.StatusCode -ne 200) { throw "HTTP $($r.StatusCode)" }
}

# -------------------------------
# 5. MongoDB (alerts collection)
# -------------------------------
Check "MongoDB alerts collection accessible" {
    $out = docker exec iot-mongo mongosh iot_port --quiet --eval "db.alerts.countDocuments({})"
    if ($out -notmatch "^\d+") { throw "Mongo query failed" }
}

# -------------------------------
# 6. Prometheus API
# -------------------------------
Check "Prometheus API" {
    $r = Invoke-RestMethod http://localhost:9090/api/v1/status/runtimeinfo
    if ($r.status -ne "success") { throw "Prometheus status != success" }
}

# -------------------------------
# 7. Grafana UI
# -------------------------------
Check "Grafana UI" {
    $r = Invoke-WebRequest -UseBasicParsing http://localhost:3000 -TimeoutSec 5
    if ($r.StatusCode -notin 200,302) { throw "HTTP $($r.StatusCode)" }
}

# -------------------------------
# 8. Elasticsearch
# -------------------------------
Check "Elasticsearch" {
    $r = Invoke-RestMethod http://localhost:9200
    if (-not $r.cluster_name) { throw "No cluster_name" }
}

# -------------------------------
# 9. Kibana
# -------------------------------
Check "Kibana UI" {
    $r = Invoke-WebRequest -UseBasicParsing http://localhost:5601 -TimeoutSec 5
    if ($r.StatusCode -notin 200,302) { throw "HTTP $($r.StatusCode)" }
}

Write-Host "`n=============================="
if ($errors -eq 0) {
    Write-Host " ALL CHECKS PASSED ✔" -ForegroundColor Green
} else {
    Write-Host " FAILED CHECKS: $errors ✖" -ForegroundColor Red
}
Write-Host "=============================="


# запускать: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#.\healthcheck.ps1

