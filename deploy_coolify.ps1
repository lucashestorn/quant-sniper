<#
.SYNOPSIS
    Script de provisionamento e deploy automatizado do Quant Terminal Sniper no Coolify (https://coolify.qaexperia.com).
.DESCRIPTION
    Lê as credenciais locais em $env:LOCALAPPDATA\QaExperia\AgentSecrets\ (ou parâmetro -WriteToken),
    cria/atualiza a aplicação no servidor Coolify (yeqx6ovbr6udvm6rwbf7bsxj) e dispara o deploy.
#>
param(
    [string]$CoolifyUrl = "https://coolify.qaexperia.com",
    [string]$ServerUuid = "yeqx6ovbr6udvm6rwbf7bsxj",
    [string]$ProjectUuid = "rjv5nomnjwlrstsrwf6wm8sy",
    [string]$EnvironmentName = "production",
    [string]$AppUuid = "",
    [string]$WriteToken = ""
)

$ErrorActionPreference = "Stop"

$secretsDir = "$env:LOCALAPPDATA\QaExperia\AgentSecrets"
$inspectPath = Join-Path $secretsDir "coolify-inspect.xml"
$deployPath  = Join-Path $secretsDir "coolify-deploy.xml"

$tokenRead = if (Test-Path $inspectPath) { (Import-Clixml $inspectPath).GetNetworkCredential().Password } else { "" }
$tokenDeploy = if ($WriteToken) { $WriteToken } elseif (Test-Path $deployPath) { (Import-Clixml $deployPath).GetNetworkCredential().Password } else { "" }

if (-not $tokenDeploy) {
    throw "Token de deploy/write não encontrado em $deployPath nem informado via -WriteToken."
}

# Se já temos um AppUuid criado no Coolify, dispara o deploy diretamente (compatível com escopo 'deploy')
if ($AppUuid) {
    Write-Host "Disparando deploy da aplicação UUID=$AppUuid no Coolify..." -ForegroundColor Cyan
    $res = Invoke-RestMethod -Uri "$CoolifyUrl/api/v1/deploy?uuid=$AppUuid&force=true" `
        -Method Get `
        -Headers @{ Authorization = "Bearer $tokenDeploy"; Accept = "application/json" }
    $res | ConvertTo-Json -Depth 5
    exit 0
}

Write-Host "Construindo Dockerfile auto-contido (Single-File Build) para envio via API Coolify..." -ForegroundColor Cyan
$reqTxt = [Convert]::ToBase64String([IO.File]::ReadAllBytes("$PSScriptRoot\requirements.txt"))
$initPy = [Convert]::ToBase64String([IO.File]::ReadAllBytes("$PSScriptRoot\app\__init__.py"))
$engPy  = [Convert]::ToBase64String([IO.File]::ReadAllBytes("$PSScriptRoot\app\engine_polars.py"))
$execPy = [Convert]::ToBase64String([IO.File]::ReadAllBytes("$PSScriptRoot\app\executor_ccxt.py"))
$mainPy = [Convert]::ToBase64String([IO.File]::ReadAllBytes("$PSScriptRoot\app\main.py"))

$inlineDockerfile = @"
FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TZ=UTC POLARS_MAX_THREADS=4
WORKDIR /code
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /code/app && \
    echo '$reqTxt' | base64 -d > /code/requirements.txt && \
    echo '$initPy' | base64 -d > /code/app/__init__.py && \
    echo '$engPy'  | base64 -d > /code/app/engine_polars.py && \
    echo '$execPy' | base64 -d > /code/app/executor_ccxt.py && \
    echo '$mainPy' | base64 -d > /code/app/main.py && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /code/requirements.txt
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 CMD curl -f http://localhost:8000/api/status || exit 1
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
"@

$payload = @{
    project_uuid     = $ProjectUuid
    server_uuid      = $ServerUuid
    environment_name = $EnvironmentName
    dockerfile       = $inlineDockerfile
    name             = "quant-terminal-sniper"
    description      = "Microsserviço 24/7 FastAPI + Polars + CCXT Sniper"
    ports_exposes    = "8000"
    instant_deploy   = $true
} | ConvertTo-Json -Depth 4

Write-Host "Criando aplicação 'quant-terminal-sniper' em $CoolifyUrl ..." -ForegroundColor Cyan
$created = Invoke-RestMethod -Uri "$CoolifyUrl/api/v1/applications/dockerfile" `
    -Method Post `
    -Headers @{ Authorization = "Bearer $tokenDeploy"; Accept = "application/json" } `
    -ContentType "application/json" `
    -Body $payload

Write-Host "Aplicação criada e deploy iniciado com sucesso!" -ForegroundColor Green
$created | ConvertTo-Json -Depth 5
