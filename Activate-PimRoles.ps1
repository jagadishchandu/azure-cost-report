<#
.SYNOPSIS
    Activates all of the current user's eligible Microsoft Entra ID (Azure AD) PIM directory roles
    with a single justification message.

.DESCRIPTION
    Uses the Microsoft Graph PowerShell SDK to:
      1. Sign in interactively (so any Conditional Access / MFA challenge is satisfied).
      2. Look up your eligible PIM directory role assignments.
      3. Skip any role/scope that is already active.
      4. Read each role's activation policy to find the maximum duration it allows
         (unless -DurationHours is supplied, in which case that value is used, capped
         to the role's own maximum).
      5. Submit a self-activation request for each remaining role with the same
         justification text.

.PARAMETER Justification
    The justification message submitted for every role activation.

.PARAMETER DurationHours
    Optional. Requests this many hours instead of each role's policy maximum.
    Automatically capped down if it exceeds what a given role's policy allows.

.PARAMETER AuthMethod
    'GraphInteractive' (default) signs in directly via Connect-MgGraph using the
    "Microsoft Graph PowerShell" app - most tenants already have admin consent for
    the PIM scopes on this app.
    'AzAccount' reuses an existing Connect-AzAccount session and mints a Graph token
    from it via the "Azure PowerShell" app instead. Use this only if that app has
    been admin-consented for the required Graph scopes in your tenant; otherwise
    you'll see 403 / Authorization_RequestDenied errors even though sign-in succeeds.

.EXAMPLE
    .\Activate-PimRoles.ps1 -Justification "Performing scheduled tenant maintenance"

.EXAMPLE
    .\Activate-PimRoles.ps1 -Justification "On-call incident response" -DurationHours 4

.EXAMPLE
    .\Activate-PimRoles.ps1 -Justification "Test run" -WhatIf

.EXAMPLE
    .\Activate-PimRoles.ps1 -Justification "Test run" -AuthMethod AzAccount

.NOTES
    Prerequisites:
      - Az.Accounts module (for Connect-AzAccount) plus Microsoft.Graph.Authentication /
        Users / Identity.Governance modules (auto-installed if missing).
      - Your account needs the eligible PIM assignments already configured in Entra ID.
      - Auth is done once via Connect-AzAccount; a Graph access token is minted from that
        session and handed to Connect-MgGraph, so there's no separate Graph sign-in prompt.
      - The signed-in identity/app must be permitted to call:
        RoleManagement.Read.Directory, RoleEligibilitySchedule.Read.Directory,
        RoleAssignmentSchedule.ReadWrite.Directory, User.Read on Microsoft Graph.
        If your tenant has never consented the "Azure PowerShell" app for these Graph scopes,
        token acquisition will succeed but Graph calls will fail with a 403 - ask your
        Global/Privileged Role Administrator to grant consent once.
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)]
    [string]$Justification,

    [Parameter()]
    [double]$DurationHours,

    [Parameter()]
    [ValidateSet('GraphInteractive', 'AzAccount')]
    [string]$AuthMethod = 'GraphInteractive'
)

$ErrorActionPreference = 'Stop'

function Get-MaxDurationHours {
    param([string]$IsoDuration)
    $m = [regex]::Match($IsoDuration, 'PT(?:(\d+)H)?(?:(\d+)M)?')
    if (-not $m.Success) { return $null }
    $hours = 0
    if ($m.Groups[1].Success) { $hours += [int]$m.Groups[1].Value }
    if ($m.Groups[2].Success) { $hours += [double]$m.Groups[2].Value / 60 }
    return $hours
}

# 1. Ensure required modules are present
$requiredModules = @('Microsoft.Graph.Authentication', 'Microsoft.Graph.Users', 'Microsoft.Graph.Identity.Governance')
if ($AuthMethod -eq 'AzAccount') { $requiredModules = @('Az.Accounts') + $requiredModules }
foreach ($mod in $requiredModules) {
    if (-not (Get-Module -ListAvailable -Name $mod)) {
        Write-Host "Installing module $mod ..." -ForegroundColor Yellow
        Install-Module -Name $mod -Scope CurrentUser -Force -AllowClobber
    }
    Import-Module $mod -ErrorAction Stop
}

# 2. Sign in - either straight to Graph, or by reusing an existing Az session
if ($AuthMethod -eq 'AzAccount') {
    if (-not (Get-AzContext)) {
        Connect-AzAccount | Out-Null
    }
    $azContext = Get-AzContext
    if (-not $azContext) { throw "Failed to connect via Connect-AzAccount." }

    $tokenResult = Get-AzAccessToken -ResourceUrl "https://graph.microsoft.com"
    $graphToken = $tokenResult.Token
    if ($graphToken -is [System.Security.SecureString]) {
        $graphTokenSecure = $graphToken
    } else {
        $graphTokenSecure = ConvertTo-SecureString $graphToken -AsPlainText -Force
    }

    Connect-MgGraph -AccessToken $graphTokenSecure -NoWelcome
    $userId = $azContext.Account.Id
} else {
    $scopes = @(
        'RoleManagement.Read.Directory',
        'RoleEligibilitySchedule.Read.Directory',
        'RoleAssignmentSchedule.ReadWrite.Directory',
        'User.Read'
    )
    Connect-MgGraph -Scopes $scopes -NoWelcome
    $userId = (Get-MgContext).Account
}

$context = Get-MgContext
if (-not $context) { throw "Failed to connect to Microsoft Graph." }

$me = Get-MgUser -UserId $userId
$principalId = $me.Id
Write-Host "Connected as $($me.UserPrincipalName) (auth method: $AuthMethod)" -ForegroundColor Cyan

# 3. Get eligible role assignments for the current user
Write-Host "Retrieving eligible PIM roles..." -ForegroundColor Cyan
try {
    $eligibleRoles = Get-MgRoleManagementDirectoryRoleEligibilityScheduleInstance `
        -Filter "principalId eq '$principalId'" `
        -ExpandProperty "roleDefinition" `
        -All `
        -ErrorAction Stop
} catch {
    if ($_.Exception.Message -match 'PermissionScopeNotGranted') {
        throw "No admin consent has been granted in this tenant for the PIM Graph scopes (RoleManagement.Read.Directory / RoleEligibilitySchedule.Read.Directory / RoleAssignmentSchedule.ReadWrite.Directory). This must be granted once by a Global/Privileged Role/Cloud Application Administrator - see Grant-PimGraphConsent.ps1. Original error: $($_.Exception.Message)"
    }
    throw
}

if (-not $eligibleRoles -or $eligibleRoles.Count -eq 0) {
    Write-Host "No eligible PIM roles found for this account." -ForegroundColor Yellow
    return
}

Write-Host "Found $($eligibleRoles.Count) eligible role assignment(s)." -ForegroundColor Cyan

# 4. Get currently active assignments so we don't try to re-activate them
$activeRoles = Get-MgRoleManagementDirectoryRoleAssignmentScheduleInstance -Filter "principalId eq '$principalId'" -All
$activeKeys = @($activeRoles | ForEach-Object { "$($_.RoleDefinitionId)|$($_.DirectoryScopeId)" })

$succeeded = @()
$failed = @()
$skipped = @()

foreach ($role in $eligibleRoles) {
    $roleName  = $role.RoleDefinition.DisplayName
    $roleDefId = $role.RoleDefinitionId
    $scopeId   = $role.DirectoryScopeId
    $key       = "$roleDefId|$scopeId"

    if ($activeKeys -contains $key) {
        Write-Host "Skipping '$roleName' (scope $scopeId) - already active." -ForegroundColor DarkGray
        $skipped += "$roleName (scope $scopeId) - already active"
        continue
    }

    # 5. Read the role's activation policy to find the max allowed duration
    $maxDurationIso = 'PT8H'  # fallback if policy lookup fails
    try {
        $policyAssignment = Get-MgPolicyRoleManagementPolicyAssignment `
            -Filter "scopeId eq '$scopeId' and roleDefinitionId eq '$roleDefId'" `
            -ErrorAction Stop

        if ($policyAssignment) {
            $policy = Get-MgPolicyRoleManagementPolicy `
                -UnifiedRoleManagementPolicyId $policyAssignment[0].PolicyId `
                -ExpandProperty "rules" `
                -ErrorAction Stop

            $expirationRule = $policy.Rules | Where-Object { $_.Id -eq 'Expiration_EndUser_Assignment' }
            if ($expirationRule -and $expirationRule.AdditionalProperties.maximumDuration) {
                $maxDurationIso = $expirationRule.AdditionalProperties.maximumDuration
            }
        }
    } catch {
        Write-Host "  Could not read policy for '$roleName'; using fallback duration $maxDurationIso." -ForegroundColor DarkYellow
    }

    $durationToUse = $maxDurationIso
    if ($PSBoundParameters.ContainsKey('DurationHours')) {
        $maxHours = Get-MaxDurationHours -IsoDuration $maxDurationIso
        if ($maxHours -and $DurationHours -gt $maxHours) {
            Write-Host "  Requested ${DurationHours}h exceeds max (${maxHours}h) for '$roleName'; using max." -ForegroundColor DarkYellow
            $durationToUse = $maxDurationIso
        } else {
            $durationToUse = "PT$([int]$DurationHours)H"
        }
    }

    $body = @{
        Action           = "selfActivate"
        PrincipalId      = $principalId
        RoleDefinitionId = $roleDefId
        DirectoryScopeId = $scopeId
        Justification    = $Justification
        ScheduleInfo     = @{
            StartDateTime = (Get-Date).ToUniversalTime()
            Expiration    = @{
                Type     = "AfterDuration"
                Duration = $durationToUse
            }
        }
    }

    if ($PSCmdlet.ShouldProcess("$roleName (scope $scopeId)", "Activate for $durationToUse")) {
        try {
            New-MgRoleManagementDirectoryRoleAssignmentScheduleRequest -BodyParameter $body -ErrorAction Stop | Out-Null
            Write-Host "Activated '$roleName' (scope $scopeId) for $durationToUse." -ForegroundColor Green
            $succeeded += "$roleName (scope $scopeId) for $durationToUse"
        } catch {
            Write-Host "Failed to activate '$roleName' (scope $scopeId): $($_.Exception.Message)" -ForegroundColor Red
            $failed += "$roleName (scope $scopeId): $($_.Exception.Message)"
        }
    }
}

Write-Host ""
Write-Host "Summary:" -ForegroundColor Cyan
Write-Host "  Activated: $($succeeded.Count)" -ForegroundColor Green
Write-Host "  Skipped (already active): $($skipped.Count)" -ForegroundColor DarkGray
Write-Host "  Failed: $($failed.Count)" -ForegroundColor $(if ($failed.Count -gt 0) { 'Red' } else { 'DarkGray' })
