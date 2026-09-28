param(
    [Parameter(Mandatory=$true)]
    [string]$IpFile
)

function ToUInt32($ipString) {
    $bytes = [System.Net.IPAddress]::Parse($ipString).GetAddressBytes()
    return ([uint32]$bytes[0] -shl 24) -bor ([uint32]$bytes[1] -shl 16) -bor ([uint32]$bytes[2] -shl 8) -bor [uint32]$bytes[3]
}

$ips = Get-Content $IpFile | Where-Object { $_.Trim() -ne '' }
$values = $ips | ForEach-Object { ToUInt32 $_.Trim() }

$min = ($values | Measure-Object -Minimum).Minimum
$max = ($values | Measure-Object -Maximum).Maximum

$diff = $min -bxor $max

$prefix = 32
$temp = $diff
while ($temp -ne 0) {
    $prefix--
    $temp = $temp -shr 1
}

if ($prefix -eq 0) {
    $network = 0
} elseif ($prefix -eq 32) {
    $network = $min
} else {
    $shift = 32 - $prefix
    $network = $min -band (0xFFFFFFFF -shl $shift)
}

$b1 = ($network -shr 24) -band 255
$b2 = ($network -shr 16) -band 255
$b3 = ($network -shr 8) -band 255
$b4 = $network -band 255

Write-Output "Sampled IPs: $($ips.Count)"
Write-Output "Smallest covering CIDR: $b1.$b2.$b3.$b4/$prefix"
Write-Output "(This is the minimal block containing all sampled node IPs -- not necessarily the true provisioned subnet size. Confirm with Azure if you need the authoritative value.)"
