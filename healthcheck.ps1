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

function CheckPrometheusUp {
    param (
        [string]$Job
    )

    $query = [uri]::EscapeDataString("up{job=`"$Job`"}")
    $r = Invoke-RestMethod "http://localhost:9090/api/v1/query?query=$query" -TimeoutSec 5
    if ($r.status -ne "success") { throw "Prometheus query failed for job=$Job" }

    $results = @($r.data.result)
    if ($results.Count -eq 0) { throw "No Prometheus targets for job=$Job" }

    foreach ($item in $results) {
        if ($item.value[1] -ne "1") {
            throw "Target $($item.metric.instance) for job=$Job is down"
        }
    }
}

# -------------------------------
# 1. Gateway / iot-controller
# -------------------------------
Check "Gateway → iot-controller (/health)" {
    $r = Invoke-WebRequest -UseBasicParsing http://localhost:8000/health -TimeoutSec 5
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
# 4. rule-engine metrics через Prometheus
# -------------------------------
Check "rule-engine (/metrics via Prometheus)" {
    CheckPrometheusUp "rule-engine"
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
# 6.1. Prometheus alert rules
# -------------------------------
Check "Prometheus alert rules loaded" {
    $r = Invoke-RestMethod http://localhost:9090/api/v1/rules
    if ($r.status -ne "success") { throw "Prometheus rules API failed" }

    $ruleNames = @()
    foreach ($group in @($r.data.groups)) {
        foreach ($rule in @($group.rules)) {
            $ruleNames += $rule.name
        }
    }

    if ($ruleNames -notcontains "ServiceDown") {
        throw "ServiceDown alert rule is not loaded"
    }
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

