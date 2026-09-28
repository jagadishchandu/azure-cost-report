<#
.SYNOPSIS
    Connects to Azure via device authentication, inventories every resource you have
    access to across all subscriptions, pulls the last N months of cost data via direct
    Azure REST API calls (no Az.Resources / Az.CostManagement dependency), and generates
    an interactive HTML report (charts + searchable/sortable table).

.PARAMETER MonthsBack
    How many months of cost history to pull. Default 6.

.PARAMETER OutputPath
    Where to write the HTML report. Defaults to the current folder with a timestamp.

.EXAMPLE
    .\Generate-AzureCostReport.ps1
    .\Generate-AzureCostReport.ps1 -MonthsBack 3 -OutputPath "C:\Reports\cost.html"
#>

param(
    [int]$MonthsBack = 6,
    [string]$OutputPath = "$PSScriptRoot\AzureCostReport_$(Get-Date -Format 'yyyyMMdd_HHmmss').html"
)

$ErrorActionPreference = "Stop"
$ManagementEndpoint = "https://management.azure.com"

# =====================================================
# 1. Module check (only Az.Accounts needed now - everything
#    else goes straight to the ARM / Cost Management REST APIs)
# =====================================================

if (-not (Get-Module -ListAvailable -Name Az.Accounts)) {
    Write-Host "Installing module Az.Accounts ..." -ForegroundColor Yellow
    Install-Module -Name Az.Accounts -Scope CurrentUser -Force -AllowClobber -Repository PSGallery
}
Import-Module Az.Accounts -ErrorAction Stop

# =====================================================
# 2. Connect (device authentication) + token helper
# =====================================================

Write-Host "`nConnecting to Azure (device authentication)..." -ForegroundColor Cyan
Connect-AzAccount -UseDeviceAuthentication | Out-Null

function Get-BearerToken {
    $tokenObj = Get-AzAccessToken -ResourceUrl $ManagementEndpoint

    # Az.Accounts 2.x returns a plain string; 3.x+ returns a SecureString
    if ($tokenObj.Token -is [System.Security.SecureString]) {
        return [System.Net.NetworkCredential]::new('', $tokenObj.Token).Password
    }
    return $tokenObj.Token
}

function Invoke-ArmRest {
    param(
        [string]$Uri,
        [string]$Method = "GET",
        [object]$Body = $null,
        [int]$MaxRetries = 5
    )

    $attempt = 0

    while ($true) {
        $attempt++
        $token = Get-BearerToken
        $headers = @{ Authorization = "Bearer $token" }

        try {
            if ($Body) {
                $jsonBody = $Body | ConvertTo-Json -Depth 10
                return Invoke-RestMethod -Uri $Uri -Method $Method -Headers $headers -ContentType "application/json" -Body $jsonBody
            }
            else {
                return Invoke-RestMethod -Uri $Uri -Method $Method -Headers $headers
            }
        }
        catch {
            $resp = $_.Exception.Response
            $status = if ($resp) { [int]$resp.StatusCode } else { 0 }

            # 429 = throttled. Respect Retry-After if present, else back off.
            if ($status -eq 429 -and $attempt -le $MaxRetries) {
                $retryAfter = 10
                try {
                    if ($resp.Headers -and $resp.Headers["Retry-After"]) {
                        $retryAfter = [int]$resp.Headers["Retry-After"]
                    }
                } catch {}

                Write-Host "  Throttled (429). Waiting $retryAfter s before retry $attempt/$MaxRetries ..." -ForegroundColor Yellow
                Start-Sleep -Seconds $retryAfter
                continue
            }

            throw
        }
    }
}

function Invoke-ArmRestPaged {
    param([string]$Uri)

    $results = New-Object System.Collections.Generic.List[object]
    $nextUri = $Uri

    while ($nextUri) {
        $page = Invoke-ArmRest -Uri $nextUri -Method GET
        if ($page.value) { $results.AddRange($page.value) }
        $nextUri = $page.nextLink
    }

    return $results
}

# =====================================================
# 3. Subscriptions (via REST)
# =====================================================

Write-Host "Fetching subscriptions..." -ForegroundColor Cyan

$subsResponse = Invoke-ArmRestPaged -Uri "$ManagementEndpoint/subscriptions?api-version=2020-01-01"

$subscriptions = $subsResponse | Where-Object { $_.state -eq "Enabled" } | ForEach-Object {
    [PSCustomObject]@{
        Id   = $_.subscriptionId
        Name = $_.displayName
    }
}

Write-Host "Found $($subscriptions.Count) enabled subscription(s) you have access to.`n" -ForegroundColor Cyan

if (-not $subscriptions -or $subscriptions.Count -eq 0) {
    Write-Warning "No subscriptions found for this account. Exiting."
    return
}

# =====================================================
# 4. Inventory resources + pull cost data per subscription (via REST)
# =====================================================

$allResources = New-Object System.Collections.Generic.List[object]
$allCostRows  = New-Object System.Collections.Generic.List[object]

$startDate = (Get-Date).AddMonths(-$MonthsBack).Date
$endDate   = (Get-Date).Date

foreach ($sub in $subscriptions) {

    Write-Host "Processing subscription: $($sub.Name) ($($sub.Id))" -ForegroundColor Green

    # ---- Resource inventory ----
    try {
        $resUri = "$ManagementEndpoint/subscriptions/$($sub.Id)/resources?api-version=2021-04-01"
        $resources = Invoke-ArmRestPaged -Uri $resUri

        foreach ($r in $resources) {
            $rg = ($r.id -split '/')[4]

            $allResources.Add([PSCustomObject]@{
                Subscription   = $sub.Name
                SubscriptionId = $sub.Id
                ResourceGroup  = $rg
                Name           = $r.name
                Type           = $r.type
                Location       = $r.location
                Id             = $r.id
            })
        }
        Write-Host "  Resources found: $($resources.Count)" -ForegroundColor DarkGray
    }
    catch {
        Write-Warning "  Could not list resources: $($_.Exception.Message)"
    }

    # ---- Cost data (monthly granularity, grouped by service) ----
    try {
        $costUri = "$ManagementEndpoint/subscriptions/$($sub.Id)/providers/Microsoft.CostManagement/query?api-version=2023-11-01"

        $costBody = @{
            type      = "ActualCost"
            timeframe = "Custom"
            timePeriod = @{
                from = $startDate.ToString("yyyy-MM-dd")
                to   = $endDate.ToString("yyyy-MM-dd")
            }
            dataset = @{
                granularity = "Monthly"
                aggregation = @{
                    totalCost = @{
                        name     = "Cost"
                        function = "Sum"
                    }
                }
                grouping = @(
                    @{ type = "Dimension"; name = "ServiceName" }
                )
            }
        }

        $result = Invoke-ArmRest -Uri $costUri -Method POST -Body $costBody

        if ($result.properties.columns -and $result.properties.rows) {
            $colNames = $result.properties.columns | ForEach-Object { $_.name }

            foreach ($row in $result.properties.rows) {
                $rowObj = [ordered]@{ Subscription = $sub.Name }
                for ($i = 0; $i -lt $colNames.Count; $i++) {
                    $rowObj[$colNames[$i]] = $row[$i]
                }
                $allCostRows.Add([PSCustomObject]$rowObj)
            }
            Write-Host "  Cost rows retrieved: $($result.properties.rows.Count)" -ForegroundColor DarkGray
        }
        else {
            Write-Host "  No cost data returned (subscription may have zero spend in range)." -ForegroundColor DarkGray
        }
    }
    catch {
        Write-Warning "  Could not retrieve cost data (likely missing Cost Management Reader role): $($_.Exception.Message)"
    }
}

# =====================================================
# 5. Normalize cost rows into a common shape
# =====================================================

function Get-MonthKey {
    param($value)

    $s = "$value"

    if ($s -match '^\d{8}$') {
        return [datetime]::ParseExact($s, "yyyyMMdd", $null).ToString("yyyy-MM")
    }
    elseif ($s -match '^\d{6}$') {
        return [datetime]::ParseExact($s, "yyyyMM", $null).ToString("yyyy-MM")
    }
    else {
        try { return ([datetime]$s).ToString("yyyy-MM") }
        catch { return $s }
    }
}

$normalizedCosts = foreach ($row in $allCostRows) {
    $props = $row.PSObject.Properties.Name

    $costProp    = $props | Where-Object { $_ -eq "Cost" } | Select-Object -First 1
    $serviceProp = $props | Where-Object { $_ -match "ServiceName" } | Select-Object -First 1
    $dateProp    = $props | Where-Object { $_ -match "UsageDate|BillingMonth|Date" } | Select-Object -First 1

    [PSCustomObject]@{
        Subscription = $row.Subscription
        Month        = if ($dateProp) { Get-MonthKey $row.$dateProp } else { "Unknown" }
        Service      = if ($serviceProp) { $row.$serviceProp } else { "Unknown" }
        Cost         = if ($costProp) { [math]::Round([double]$row.$costProp, 2) } else { 0 }
    }
}

$totalCost = [math]::Round((($normalizedCosts | Measure-Object -Property Cost -Sum).Sum), 2)

$costByMonth = $normalizedCosts |
    Group-Object Month |
    Sort-Object Name |
    ForEach-Object { [PSCustomObject]@{ Month = $_.Name; Cost = [math]::Round((($_.Group | Measure-Object -Property Cost -Sum).Sum), 2) } }

$costByService = $normalizedCosts |
    Group-Object Service |
    ForEach-Object { [PSCustomObject]@{ Service = $_.Name; Cost = [math]::Round((($_.Group | Measure-Object -Property Cost -Sum).Sum), 2) } } |
    Sort-Object Cost -Descending

$costBySubscription = $normalizedCosts |
    Group-Object Subscription |
    ForEach-Object { [PSCustomObject]@{ Subscription = $_.Name; Cost = [math]::Round((($_.Group | Measure-Object -Property Cost -Sum).Sum), 2) } } |
    Sort-Object Cost -Descending

# =====================================================
# 6. Build JSON payloads for the HTML report
# =====================================================

$resourceJson      = $allResources       | ConvertTo-Json -Depth 5 -Compress
$costByMonthJson    = $costByMonth        | ConvertTo-Json -Depth 5 -Compress
$costByServiceJson  = $costByService      | ConvertTo-Json -Depth 5 -Compress
$costBySubJson      = $costBySubscription | ConvertTo-Json -Depth 5 -Compress

if ($allResources.Count -eq 1)       { $resourceJson     = "[$resourceJson]" }
if ($costByMonth.Count -eq 1)        { $costByMonthJson   = "[$costByMonthJson]" }
if ($costByService.Count -eq 1)      { $costByServiceJson = "[$costByServiceJson]" }
if ($costBySubscription.Count -eq 1) { $costBySubJson     = "[$costBySubJson]" }

$reportGenerated = Get-Date -Format "yyyy-MM-dd HH:mm"
$rangeLabel = "$($startDate.ToString('yyyy-MM-dd')) to $($endDate.ToString('yyyy-MM-dd'))"

# =====================================================
# 7. HTML template
# =====================================================

$htmlTemplate = @'
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Azure Cost & Resource Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  :root {
    --bg: #0f1420;
    --card: #161d2e;
    --border: #263049;
    --text: #e7ecf5;
    --muted: #93a1bd;
    --accent: #4da3ff;
    --accent2: #7ee8a4;
    --accent3: #ffb454;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: 'Segoe UI', Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
  }
  header {
    padding: 24px 32px;
    border-bottom: 1px solid var(--border);
    background: linear-gradient(135deg, #131a2b, #0f1420);
  }
  header h1 { margin: 0 0 4px 0; font-size: 22px; }
  header p { margin: 0; color: var(--muted); font-size: 13px; }

  .container { padding: 24px 32px; }

  .stat-row {
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    margin-bottom: 24px;
  }
  .stat-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px 20px;
    min-width: 180px;
    flex: 1;
  }
  .stat-card .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .05em; }
  .stat-card .value { font-size: 26px; font-weight: 600; margin-top: 6px; }

  .chart-row {
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    margin-bottom: 24px;
  }
  .chart-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px;
    flex: 1;
    min-width: 320px;
  }
  .chart-card h3 { margin: 0 0 12px 0; font-size: 14px; color: var(--muted); font-weight: 600; }
  .chart-card canvas { max-height: 320px; }

  .table-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px;
  }
  .table-card h3 { margin: 0 0 12px 0; font-size: 14px; color: var(--muted); font-weight: 600; }

  .controls {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    margin-bottom: 12px;
  }
  .controls input, .controls select {
    background: #0f1420;
    border: 1px solid var(--border);
    color: var(--text);
    padding: 8px 10px;
    border-radius: 6px;
    font-size: 13px;
  }
  .controls input { flex: 1; min-width: 220px; }

  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border); }
  th { cursor: pointer; color: var(--muted); user-select: none; position: sticky; top: 0; background: var(--card); }
  th:hover { color: var(--text); }
  tr:hover td { background: #1c2438; }

  .table-scroll { max-height: 480px; overflow-y: auto; }

  .badge {
    display: inline-block;
    background: #1d2942;
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 2px 10px;
    font-size: 11px;
    color: var(--accent);
  }

  footer { padding: 16px 32px; color: var(--muted); font-size: 12px; }
</style>
</head>
<body>

<header>
  <h1>Azure Cost &amp; Resource Report</h1>
  <p>Generated __REPORT_GENERATED__ &middot; Cost window: __RANGE_LABEL__</p>
</header>

<div class="container">

  <div class="stat-row">
    <div class="stat-card">
      <div class="label">Total resources</div>
      <div class="value" id="statResources">-</div>
    </div>
    <div class="stat-card">
      <div class="label">Subscriptions</div>
      <div class="value" id="statSubs">-</div>
    </div>
    <div class="stat-card">
      <div class="label">Total cost (window)</div>
      <div class="value" id="statCost">-</div>
    </div>
    <div class="stat-card">
      <div class="label">Top service by cost</div>
      <div class="value" id="statTopService" style="font-size:16px;">-</div>
    </div>
  </div>

  <div class="chart-row">
    <div class="chart-card">
      <h3>Monthly cost trend</h3>
      <canvas id="monthChart"></canvas>
    </div>
    <div class="chart-card">
      <h3>Cost by service (top 10)</h3>
      <canvas id="serviceChart"></canvas>
    </div>
    <div class="chart-card">
      <h3>Cost by subscription</h3>
      <canvas id="subChart"></canvas>
    </div>
  </div>

  <div class="table-card">
    <h3>Resource inventory <span class="badge" id="resourceCountBadge"></span></h3>
    <div class="controls">
      <input type="text" id="searchBox" placeholder="Search by name, type, resource group, location...">
      <select id="subFilter"><option value="">All subscriptions</option></select>
      <select id="typeFilter"><option value="">All resource types</option></select>
    </div>
    <div class="table-scroll">
      <table id="resourceTable">
        <thead>
          <tr>
            <th data-key="Subscription">Subscription</th>
            <th data-key="ResourceGroup">Resource Group</th>
            <th data-key="Name">Name</th>
            <th data-key="Type">Type</th>
            <th data-key="Location">Location</th>
          </tr>
        </thead>
        <tbody id="resourceBody"></tbody>
      </table>
    </div>
  </div>

</div>

<footer>Data pulled directly from Azure Resource Manager and Cost Management REST APIs for the account used to sign in. Costs are actual costs (not amortized), monthly granularity.</footer>

<script>
const resourceData      = __RESOURCE_JSON__;
const costByMonth        = __COST_BY_MONTH_JSON__;
const costByService      = __COST_BY_SERVICE_JSON__;
const costBySubscription = __COST_BY_SUB_JSON__;

function fmtCurrency(n) {
  return '$' + Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

// ---- Stat cards ----
document.getElementById('statResources').textContent = resourceData.length.toLocaleString();
document.getElementById('statSubs').textContent = new Set(resourceData.map(r => r.Subscription)).size;
const totalCost = costByService.reduce((a, b) => a + b.Cost, 0);
document.getElementById('statCost').textContent = fmtCurrency(totalCost);
document.getElementById('statTopService').textContent = costByService.length ? costByService[0].Service : 'N/A';
document.getElementById('resourceCountBadge').textContent = resourceData.length + ' resources';

// ---- Charts ----
const palette = ['#4da3ff', '#7ee8a4', '#ffb454', '#ff6b6b', '#c792ea', '#5ad8e6', '#f78fb3', '#a0e7a0', '#f4a261', '#9d8df1'];

new Chart(document.getElementById('monthChart'), {
  type: 'line',
  data: {
    labels: costByMonth.map(m => m.Month),
    datasets: [{
      label: 'Cost',
      data: costByMonth.map(m => m.Cost),
      borderColor: '#4da3ff',
      backgroundColor: 'rgba(77,163,255,0.15)',
      fill: true,
      tension: 0.3
    }]
  },
  options: {
    plugins: { legend: { display: false } },
    scales: {
      x: { ticks: { color: '#93a1bd' }, grid: { color: '#263049' } },
      y: { ticks: { color: '#93a1bd', callback: v => fmtCurrency(v) }, grid: { color: '#263049' } }
    }
  }
});

const topServices = costByService.slice(0, 10);
new Chart(document.getElementById('serviceChart'), {
  type: 'doughnut',
  data: {
    labels: topServices.map(s => s.Service),
    datasets: [{ data: topServices.map(s => s.Cost), backgroundColor: palette }]
  },
  options: {
    plugins: { legend: { position: 'bottom', labels: { color: '#e7ecf5', boxWidth: 12, font: { size: 10 } } } }
  }
});

new Chart(document.getElementById('subChart'), {
  type: 'bar',
  data: {
    labels: costBySubscription.map(s => s.Subscription),
    datasets: [{ label: 'Cost', data: costBySubscription.map(s => s.Cost), backgroundColor: '#7ee8a4' }]
  },
  options: {
    indexAxis: 'y',
    plugins: { legend: { display: false } },
    scales: {
      x: { ticks: { color: '#93a1bd', callback: v => fmtCurrency(v) }, grid: { color: '#263049' } },
      y: { ticks: { color: '#93a1bd' }, grid: { display: false } }
    }
  }
});

// ---- Resource table ----
const subFilter = document.getElementById('subFilter');
const typeFilter = document.getElementById('typeFilter');
const searchBox = document.getElementById('searchBox');
const tbody = document.getElementById('resourceBody');

[...new Set(resourceData.map(r => r.Subscription))].sort().forEach(s => {
  const opt = document.createElement('option'); opt.value = s; opt.textContent = s;
  subFilter.appendChild(opt);
});
[...new Set(resourceData.map(r => r.Type))].sort().forEach(t => {
  const opt = document.createElement('option'); opt.value = t; opt.textContent = t;
  typeFilter.appendChild(opt);
});

let sortKey = 'Name';
let sortAsc = true;

function renderTable() {
  const search = searchBox.value.toLowerCase();
  const sub = subFilter.value;
  const type = typeFilter.value;

  let rows = resourceData.filter(r => {
    if (sub && r.Subscription !== sub) return false;
    if (type && r.Type !== type) return false;
    if (search) {
      const hay = (r.Name + ' ' + r.Type + ' ' + r.ResourceGroup + ' ' + r.Location).toLowerCase();
      if (!hay.includes(search)) return false;
    }
    return true;
  });

  rows.sort((a, b) => {
    const av = (a[sortKey] || '').toString().toLowerCase();
    const bv = (b[sortKey] || '').toString().toLowerCase();
    if (av < bv) return sortAsc ? -1 : 1;
    if (av > bv) return sortAsc ? 1 : -1;
    return 0;
  });

  tbody.innerHTML = rows.map(r => `
    <tr>
      <td>${r.Subscription}</td>
      <td>${r.ResourceGroup || ''}</td>
      <td>${r.Name}</td>
      <td>${r.Type}</td>
      <td>${r.Location || ''}</td>
    </tr>
  `).join('');

  document.getElementById('resourceCountBadge').textContent = rows.length + ' resources';
}

document.querySelectorAll('#resourceTable th').forEach(th => {
  th.addEventListener('click', () => {
    const key = th.getAttribute('data-key');
    if (sortKey === key) { sortAsc = !sortAsc; } else { sortKey = key; sortAsc = true; }
    renderTable();
  });
});

searchBox.addEventListener('input', renderTable);
subFilter.addEventListener('change', renderTable);
typeFilter.addEventListener('change', renderTable);

renderTable();
</script>

</body>
</html>
'@

$html = $htmlTemplate.Replace('__REPORT_GENERATED__', $reportGenerated)
$html = $html.Replace('__RANGE_LABEL__', $rangeLabel)
$html = $html.Replace('__RESOURCE_JSON__', $resourceJson)
$html = $html.Replace('__COST_BY_MONTH_JSON__', $costByMonthJson)
$html = $html.Replace('__COST_BY_SERVICE_JSON__', $costByServiceJson)
$html = $html.Replace('__COST_BY_SUB_JSON__', $costBySubJson)

$html | Out-File -FilePath $OutputPath -Encoding utf8

Write-Host "`nReport written to: $OutputPath" -ForegroundColor Cyan
Write-Host "Total resources: $($allResources.Count) | Total cost ($MonthsBack months): $totalCost" -ForegroundColor Cyan

Start-Process $OutputPath
