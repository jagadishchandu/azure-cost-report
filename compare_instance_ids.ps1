<#
    Compares an Instance_ID column from an Excel file against a
    provider_id column in a CSV file (substring match, since provider_id
    looks like "aws:///us-east-1a/i-0ab005a8999fe9e7f").

    Requires the ImportExcel module to read .xlsx directly:
        Install-Module -Name ImportExcel -Scope CurrentUser
#>

Add-Type -AssemblyName System.Windows.Forms

function Select-File {
    param(
        [string]$Title,
        [string]$Filter
    )
    $dialog = New-Object System.Windows.Forms.OpenFileDialog
    $dialog.Title = $Title
    $dialog.Filter = $Filter
    $dialog.Multiselect = $false
    if ($dialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {
        return $dialog.FileName
    }
    return $null
}

# --- Select files ---
$excelPath = Select-File -Title "Select Excel file" -Filter "Excel Files (*.xlsx;*.xls)|*.xlsx;*.xls"
if (-not $excelPath) { Write-Host "No Excel file selected. Exiting."; exit }

$csvPath = Select-File -Title "Select CSV file" -Filter "CSV Files (*.csv)|*.csv"
if (-not $csvPath) { Write-Host "No CSV file selected. Exiting."; exit }

Write-Host "Excel file: $excelPath"
Write-Host "CSV file:   $csvPath"

# --- Ensure ImportExcel module is available ---
if (-not (Get-Module -ListAvailable -Name ImportExcel)) {
    Write-Host "ImportExcel module not found. Installing for current user..."
    Install-Module -Name ImportExcel -Scope CurrentUser -Force -AllowClobber
}
Import-Module ImportExcel

# --- Load data ---
$excelData = Import-Excel -Path $excelPath
$csvData = Import-Csv -Path $csvPath

if (-not ($excelData | Get-Member -Name "Instance_id")) {
    Write-Host "Column 'Instance_id' not found in Excel file. Available columns:"
    $excelData | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name
    exit
}

if (-not ($csvData | Get-Member -Name "provider_id")) {
    Write-Host "Column 'provider_id' not found in CSV file. Available columns:"
    $csvData | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name
    exit
}

# --- Compare ---
$results = @()

foreach ($row in $excelData) {
    $instanceId = $row.Instance_id

    $match = $null
    if (-not [string]::IsNullOrWhiteSpace($instanceId)) {
        $match = $csvData | Where-Object { $_.provider_id -like "*$instanceId*" } | Select-Object -First 1
    }

    # Clone the full Excel row, then append match-result columns.
    $resultRow = $row.PSObject.Copy()
    $resultRow | Add-Member -MemberType NoteProperty -Name "Found_In_CSV" -Value ([bool]$match)
    $resultRow | Add-Member -MemberType NoteProperty -Name "Matched_provider_id" -Value $(if ($match) { $match.provider_id } else { "" })

    $results += $resultRow
}

# --- Output ---
$outputPath = Join-Path (Split-Path $excelPath) "instance_id_comparison_result.csv"
$results | Export-Csv -Path $outputPath -NoTypeInformation

$foundCount = ($results | Where-Object { $_.Found_In_CSV }).Count
$notFoundCount = ($results | Where-Object { -not $_.Found_In_CSV }).Count

Write-Host ""
Write-Host "Comparison complete."
Write-Host "  Matched:     $foundCount"
Write-Host "  Not matched: $notFoundCount"
Write-Host "  Results written to: $outputPath"

$results | Format-Table -AutoSize
