<#
.SYNOPSIS
    Fetches all snapshots (name + resource ID) across one or more subscription/resource-group pairs.

.EXAMPLE
    .\get-snapshots.ps1 -SubscriptionIds "sub-id-1","sub-id-2" -ResourceGroups "rg-1","rg-2"

    Pairs by position: sub-id-1/rg-1, sub-id-2/rg-2. Arrays must be the same length.

.EXAMPLE
    .\get-snapshots.ps1 -SubscriptionIds "sub-id-1","sub-id-2" -ResourceGroups "rg-1","rg-2" -OutCsv snapshots.csv
#>

param(
    [Parameter(Mandatory=$true)]
    [string[]]$SubscriptionIds,

    [Parameter(Mandatory=$true)]
    [string[]]$ResourceGroups,

    [string]$OutCsv
)

if ($SubscriptionIds.Count -ne $ResourceGroups.Count) {
    throw "SubscriptionIds count ($($SubscriptionIds.Count)) must match ResourceGroups count ($($ResourceGroups.Count)) -- each subscription pairs with the resource group at the same index."
}

$results = @()

for ($i = 0; $i -lt $SubscriptionIds.Count; $i++) {
    $sub = $SubscriptionIds[$i]
    $rg = $ResourceGroups[$i]

    Write-Host "Switching context to subscription '$sub'..."
    try {
        Set-AzContext -Subscription $sub -ErrorAction Stop | Out-Null
    } catch {
        Write-Warning "Failed to set context for subscription '$sub': $_"
        continue
    }

    Write-Host "Fetching snapshots in resource group '$rg'..."
    try {
        $snapshots = Get-AzSnapshot -ResourceGroupName $rg -ErrorAction Stop
    } catch {
        Write-Warning "Failed to fetch snapshots for '$sub' / '$rg': $_"
        continue
    }

    foreach ($snap in $snapshots) {
        $results += [PSCustomObject]@{
            Subscription  = $sub
            ResourceGroup = $rg
            Name          = $snap.Name
            Id            = $snap.Id
        }
    }
}

$results | Format-Table -AutoSize

if ($OutCsv) {
    $results | Export-Csv -Path $OutCsv -NoTypeInformation
    Write-Host "Exported to $OutCsv"
}
