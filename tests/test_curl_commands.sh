#!/usr/bin/env bash
set -euo pipefail
BASE="http://localhost:5000/api/v1"
curl -s "$BASE/health" | python3 -m json.tool
curl -s "$BASE/sensors" | python3 -m json.tool
curl -s "$BASE/sensors/temperature/latest" | python3 -m json.tool
curl -s "$BASE/sensors/temperature/stats?days=90" | python3 -m json.tool
curl -s "$BASE/anomalies?sensor=temperature&limit=5" | python3 -m json.tool
curl -s -X POST "$BASE/readings" -H "Content-Type: application/json" -d '{"sensor":"temperature","value":36.7,"unit":"C","source":"curl-test"}' | python3 -m json.tool
