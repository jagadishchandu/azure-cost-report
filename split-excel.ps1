<#
.SYNOPSIS
    Splits every worksheet in an Excel workbook into two output workbooks,
    each containing ALL the same worksheet names, with the data rows of
    each sheet divided top-half / bottom-half (row order preserved).

    If -SourcePath is omitted, a file picker dialog opens to select the
    source Excel file. Output files are named automatically from the
    source file name with "_1" / "_2" suffixes, saved alongside it.

.PARAMETER SourcePath
    Path to the source .xlsx file. If omitted, a file picker dialog opens.

.PARAMETER HasHeaderRow
    Whether row 1 of each worksheet is a header row that should be repeated
    in both output files. Defaults to $true.

.EXAMPLE
    .\split-excel.ps1
    (opens a file picker, then writes report_1.xlsx / report_2.xlsx next to report.xlsx)

.EXAMPLE
    .\split-excel.ps1 -SourcePath "C:\data\report.xlsx"
#>

param(
    [string]$SourcePath,

    [bool]$HasHeaderRow = $true
)

Add-Type -AssemblyName System.Windows.Forms

if (-not $SourcePath) {
    $dialog = New-Object System.Windows.Forms.OpenFileDialog
    $dialog.Filter = "Excel Files (*.xlsx;*.xlsm)|*.xlsx;*.xlsm|All Files (*.*)|*.*"
    $dialog.Title = "Select Excel file to split"

    if ($dialog.ShowDialog() -ne [System.Windows.Forms.DialogResult]::OK) {
        Write-Host "No file selected. Exiting."
        exit
    }

    $SourcePath = $dialog.FileName
}

$SourcePath = (Resolve-Path $SourcePath).Path

$dir      = Split-Path $SourcePath -Parent
$baseName = [System.IO.Path]::GetFileNameWithoutExtension($SourcePath)
$ext      = [System.IO.Path]::GetExtension($SourcePath)

$OutputPath1 = Join-Path $dir "$baseName`_1$ext"
$OutputPath2 = Join-Path $dir "$baseName`_2$ext"

Write-Host "Source: $SourcePath"
Write-Host "Output 1: $OutputPath1"
Write-Host "Output 2: $OutputPath2"

function Release-Com($obj) {
    if ($obj) {
        [System.Runtime.Interopservices.Marshal]::ReleaseComObject($obj) | Out-Null
    }
}

function Prepare-OutputWorkbook($excel, [string[]]$sheetNames) {
    $wb = $excel.Workbooks.Add()

    while ($wb.Worksheets.Count -lt $sheetNames.Count) {
        $wb.Worksheets.Add() | Out-Null
    }
    while ($wb.Worksheets.Count -gt $sheetNames.Count) {
        $wb.Worksheets.Item($wb.Worksheets.Count).Delete()
    }

    # Two-pass rename to temp names first, avoids collisions with target names
    for ($i = 1; $i -le $wb.Worksheets.Count; $i++) {
        $wb.Worksheets.Item($i).Name = "__tmp_$i"
    }
    for ($i = 1; $i -le $sheetNames.Count; $i++) {
        $wb.Worksheets.Item($i).Name = $sheetNames[$i - 1]
    }

    return $wb
}

function Get-UsedValues($worksheet) {
    $used = $worksheet.UsedRange
    $rowCount = [int]$used.Rows.Count
    $colCount = [int]$used.Columns.Count
    $raw = $used.Value2

    # Value2 returns a scalar (not an array) when the used range is a single cell
    if ($rowCount -eq 1 -and $colCount -eq 1) {
        $arr = New-Object 'object[,]' 1, 1
        $arr[0, 0] = $raw
        $raw = $arr
    }

    return @{ Values = $raw; RowCount = $rowCount; ColCount = $colCount }
}

function Build-Block($values, [int[]]$rowIndices, [int]$colCount, [bool]$includeHeader) {
    $totalRows = $rowIndices.Count + [int]$includeHeader
    $out = New-Object 'object[,]' $totalRows, $colCount

    $destRow = 0
    if ($includeHeader) {
        for ($c = 1; $c -le $colCount; $c++) {
            $out[0, $c - 1] = $values[1, $c]
        }
        $destRow = 1
    }

    for ($r = 0; $r -lt $rowIndices.Count; $r++) {
        $srcRow = $rowIndices[$r]
        for ($c = 1; $c -le $colCount; $c++) {
            $out[$destRow, $c - 1] = $values[$srcRow, $c]
        }
        $destRow++
    }

    return $out
}

$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false

$srcWb = $null
$wb1 = $null
$wb2 = $null

try {
    $srcWb = $excel.Workbooks.Open($SourcePath)

    $sheetNames = @()
    foreach ($ws in $srcWb.Worksheets) {
        $sheetNames += $ws.Name
    }

    $wb1 = Prepare-OutputWorkbook -excel $excel -sheetNames $sheetNames
    $wb2 = Prepare-OutputWorkbook -excel $excel -sheetNames $sheetNames

    for ($i = 1; $i -le $srcWb.Worksheets.Count; $i++) {
        $srcWs = $srcWb.Worksheets.Item($i)
        $name = $srcWs.Name

        Write-Host "Processing worksheet '$name'..."

        $info = Get-UsedValues $srcWs
        $values = $info.Values
        $rowCount = $info.RowCount
        $colCount = $info.ColCount

        $headerOffset = [int]$HasHeaderRow
        $dataRowCount = [Math]::Max(0, $rowCount - $headerOffset)

        $half = [Math]::Ceiling($dataRowCount / 2)

        $firstRows  = @()
        $secondRows = @()
        if ($dataRowCount -gt 0) {
            $firstRows  = ($headerOffset + 1)..($headerOffset + $half)
            if (($headerOffset + $half + 1) -le $rowCount) {
                $secondRows = ($headerOffset + $half + 1)..$rowCount
            }
        }

        $destWs1 = $wb1.Worksheets.Item($name)
        $destWs2 = $wb2.Worksheets.Item($name)

        if ($firstRows.Count -gt 0 -or $HasHeaderRow) {
            $block1 = Build-Block -values $values -rowIndices $firstRows -colCount $colCount -includeHeader $HasHeaderRow
            $destWs1.Range($destWs1.Cells(1,1), $destWs1.Cells($block1.GetLength(0), $colCount)).Value2 = $block1
        }

        if ($secondRows.Count -gt 0 -or $HasHeaderRow) {
            $block2 = Build-Block -values $values -rowIndices $secondRows -colCount $colCount -includeHeader $HasHeaderRow
            $destWs2.Range($destWs2.Cells(1,1), $destWs2.Cells($block2.GetLength(0), $colCount)).Value2 = $block2
        }

        Write-Host "  Total data rows: $dataRowCount -> Part1: $($firstRows.Count), Part2: $($secondRows.Count)"
    }

    if (Test-Path $OutputPath1) { Remove-Item $OutputPath1 -Force }
    if (Test-Path $OutputPath2) { Remove-Item $OutputPath2 -Force }

    $wb1.SaveAs($OutputPath1)
    $wb2.SaveAs($OutputPath2)

    Write-Host "Saved: $OutputPath1"
    Write-Host "Saved: $OutputPath2"
}
finally {
    if ($srcWb) { $srcWb.Close($false) }
    if ($wb1) { $wb1.Close($false) }
    if ($wb2) { $wb2.Close($false) }

    Release-Com $srcWb
    Release-Com $wb1
    Release-Com $wb2

    $excel.Quit()
    Release-Com $excel

    [System.GC]::Collect()
    [System.GC]::WaitForPendingFinalizers()
}
