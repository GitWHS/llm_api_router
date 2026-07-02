﻿# ─────────────────────────────────────────────────────────────────────────────
# LLM API Router 一键启动（前后端）
#   用法: powershell -ExecutionPolicy Bypass -File start.ps1
#   - 启动 uvicorn 后端（同时托管 /ui 前端）
#   - 等待 /healthz 就绪后自动打开浏览器到管理面 /ui/
#   - Ctrl+C 停止
# ─────────────────────────────────────────────────────────────────────────────
$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

# 1. 定位 Python
$py = (Get-Command py -ErrorAction SilentlyContinue)
if ($py) { $python = "py"; $pyArgs = @("-3") }
else {
  $pcmd = (Get-Command python -ErrorAction SilentlyContinue)
  if (-not $pcmd) { Write-Host "[ERROR] 找不到 python/py，请先安装 Python 3.10+" -ForegroundColor Red; exit 1 }
  $python = "python"; $pyArgs = @()
}

# 2. 依赖检查（缺失则尝试安装）
& $python @pyArgs -c "import fastapi, uvicorn, httpx, yaml, ruamel.yaml" 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Host "[setup] 安装依赖 (fastapi/uvicorn/httpx/pyyaml/ruamel.yaml)..." -ForegroundColor Cyan
  & $python @pyArgs -m pip install -e . ruamel.yaml -q
  if ($LASTEXITCODE -ne 0) { Write-Host "[ERROR] 依赖安装失败" -ForegroundColor Red; exit 1 }
}

# 3. 配置存在性检查
if (-not (Test-Path "$here\config.yaml")) {
  Write-Host "[WARN] 缺少 config.yaml，从示例复制" -ForegroundColor Yellow
  Copy-Item "$here\config.example.yaml" "$here\config.yaml"
}
if (-not (Test-Path "$here\keys.yaml")) {
  Write-Host "[ERROR] 缺少 keys.yaml，请先从 keys.example.yaml 复制并填入真实 key" -ForegroundColor Red
  exit 1
}

# 4. 校验配置
& $python @pyArgs -m llm_api_router.cli validate
if ($LASTEXITCODE -ne 0) { Write-Host "[ERROR] 配置校验未通过，已中止" -ForegroundColor Red; exit 1 }

# 5. 读取监听地址
$hostport = & $python @pyArgs -c "from llm_api_router.config import load_config; from pathlib import Path; c=load_config(Path('config.yaml')); print(f'{c.host}:{c.port}')"
$url = "http://$hostport"

# 6. 打印服务信息横幅（端点 / vk / 模型 / 池 / 接入示例）
$env:PYTHONIOENCODING = "utf-8"
& $python @pyArgs -m llm_api_router.cli info

# 7. 后台等待就绪 → 开浏览器到管理面
Start-Job -ScriptBlock {
  param($u)
  for ($i=0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 500
    try { if ((Invoke-WebRequest -Uri "$u/healthz" -TimeoutSec 2 -UseBasicParsing).StatusCode -eq 200) {
      Start-Process "$u/ui/"; break } } catch {}
  }
} -ArgumentList $url | Out-Null

Write-Host ""
Write-Host "[start] 后端启动中… 就绪后自动打开管理面 $url/ui/   (Ctrl+C 停止)" -ForegroundColor Green
Write-Host ""

# 8. 前台运行后端（Ctrl+C 停止）。serve 启动时也会再打印一次横幅。
& $python @pyArgs -m llm_api_router.cli serve
