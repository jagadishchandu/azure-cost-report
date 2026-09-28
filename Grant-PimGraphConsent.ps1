<#
.SYNOPSIS
    One-time, tenant-wide admin consent grant for the Microsoft Graph PowerShell app to call
    the PIM directory-role APIs (needed by Activate-PimRoles.ps1). Run this ONCE, by someone
    holding Global Administrator, Privileged Role Administrator, or Cloud Application
    Administrator in the tenant.

.DESCRIPTION
    Activate-PimRoles.ps1 fails with "PermissionScopeNotGranted" when nobody in the tenant has
    ever consented the required Graph scopes for the client app it signs in as. Admin consent
    for these scopes cannot be granted by an individual end user - it must come from an admin,
    once, for the whole tenant. This script creates that consent grant directly via Microsoft
    Graph so nobody has to hunt through the Entra admin center UI.

.PARAMETER ClientAppId
    The application (client) ID whose consent grant you're creating. Defaults to the
    first-party "Microsoft Graph PowerShell" app (14d82eec-204b-4c2f-b7e8-296a70dab67e),
    which is what Activate-PimRoles.ps1 uses with -AuthMethod GraphInteractive (the default).
    Pass 1950a258-227b-4e31-a9cf-717495945fc2 instead if you're using -AuthMethod AzAccount
    ("Azure PowerShell" app).

.EXAMPLE
    .\Grant-PimGraphConsent.ps1

.EXAMPLE
    .\Grant-PimGraphConsent.ps1 -ClientAppId 1950a258-227b-4e31-a9cf-717495945fc2
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter()]
    [string]$ClientAppId = '14d82eec-204b-4c2f-b7e8-296a70dab67e'  # Microsoft Graph PowerShell
)

$ErrorActionPreference = 'Stop'

$requiredModules = @('Microsoft.Graph.Authentication', 'Microsoft.Graph.Applications', 'Microsoft.Graph.Identity.SignIns')
foreach ($mod in $requiredModules) {
    if (-not (Get-Module -ListAvailable -Name $mod)) {
        Write-Host "Installing module $mod ..." -ForegroundColor Yellow
        Install-Module -Name $mod -Scope CurrentUser -Force -AllowClobber
    }
    Import-Module $mod -ErrorAction Stop
}

# Admin-only scopes needed to grant the delegated consent below
Connect-MgGraph -Scopes "Application.Read.All", "DelegatedPermissionGrant.ReadWrite.All" -NoWelcome

$graphResourceAppId = '00000003-0000-0000-c000-000000000000'  # Microsoft Graph
$graphSp  = Get-MgServicePrincipal -Filter "appId eq '$graphResourceAppId'"
$clientSp = Get-MgServicePrincipal -Filter "appId eq '$ClientAppId'"

if (-not $clientSp) {
    throw "No service principal found for app id '$ClientAppId' in this tenant. Sign in interactively once with that app (e.g. run Connect-MgGraph or Connect-AzAccount) so Entra creates its service principal, then re-run this script."
}

$requiredScopes = "RoleManagement.Read.Directory RoleEligibilitySchedule.Read.Directory RoleAssignmentSchedule.ReadWrite.Directory User.Read"

$existingGrant = Get-MgOauth2PermissionGrant -Filter "clientId eq '$($clientSp.Id)' and resourceId eq '$($graphSp.Id)' and consentType eq 'AllPrincipals'"

if ($existingGrant) {
    $mergedScopes = (($existingGrant.Scope -split ' ') + ($requiredScopes -split ' ') | Select-Object -Unique) -join ' '
    if ($PSCmdlet.ShouldProcess("OAuth2 permission grant $($existingGrant.Id)", "Update scopes to: $mergedScopes")) {
        Update-MgOauth2PermissionGrant -OAuth2PermissionGrantId $existingGrant.Id -BodyParameter @{ Scope = $mergedScopes }
        Write-Host "Updated existing tenant-wide grant for '$ClientAppId' to include: $mergedScopes" -ForegroundColor Green
    }
} else {
    if ($PSCmdlet.ShouldProcess("Microsoft Graph ($($graphSp.Id))", "Grant '$ClientAppId' tenant-wide consent for: $requiredScopes")) {
        New-MgOauth2PermissionGrant -BodyParameter @{
            ClientId    = $clientSp.Id
            ConsentType = "AllPrincipals"
            ResourceId  = $graphSp.Id
            Scope       = $requiredScopes
        } | Out-Null
        Write-Host "Granted tenant-wide consent for '$ClientAppId' to call: $requiredScopes" -ForegroundColor Green
    }
}

Write-Host "Done. Users can now run Activate-PimRoles.ps1 without a consent prompt." -ForegroundColor Cyan
