param(
    [string]$SourceDir = "C:\Users\sagarbathe\fabric_shortcuts_downstream_usage\scripts\_report_out",
    [string]$WorkspaceId = "c5c50c6e-30d1-4d2e-8766-d82917e13592",
    [string]$DisplayName = "RPT_ShortcutMonitoring",
    [string]$Description = "Sample Power BI report over SM_ShortcutMonitoring: Executive Summary, Copy Events, Duplicate Shortcuts, and Inventory & Churn pages.",
    [string]$ExistingItemId = ""
)

$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

$parts = @()
Get-ChildItem -Path $SourceDir -Recurse -File | ForEach-Object {
    $rel = $_.FullName.Substring($SourceDir.Length + 1).Replace('\', '/')
    $bytes = [System.IO.File]::ReadAllBytes($_.FullName)
    $b64 = [Convert]::ToBase64String($bytes)
    $parts += @{ path = $rel; payload = $b64; payloadType = "InlineBase64" }
}
Write-Output "Collected $($parts.Count) parts"

if ($ExistingItemId -ne "") {
    $body = @{ definition = @{ parts = $parts } } | ConvertTo-Json -Depth 20
    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$ExistingItemId/updateDefinition"
} else {
    $body = @{ displayName = $DisplayName; description = $Description; definition = @{ parts = $parts } } | ConvertTo-Json -Depth 20
    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items"
}

try {
    $resp = Invoke-WebRequest -Uri $uri -Headers $headers -Method Post -Body $body -UseBasicParsing
    Write-Output "Status: $($resp.StatusCode)"
    Write-Output $resp.Content
    $loc = $resp.Headers['Location']
    if ($loc) {
        Write-Output "Operation: $loc"
        $opId = $loc.Split('/')[-1]
        $opUrl = "https://api.fabric.microsoft.com/v1/operations/$opId"
        for ($i = 0; $i -lt 20; $i++) {
            Start-Sleep -Seconds 3
            $r2 = Invoke-WebRequest -Uri $opUrl -Headers $headers -Method Get -UseBasicParsing
            $c2 = $r2.Content | ConvertFrom-Json
            Write-Output "Poll: $($c2.status)"
            if ($c2.status -eq "Succeeded" -or $c2.status -eq "Failed") {
                Write-Output $r2.Content
                break
            }
        }
    }
} catch {
    Write-Output "ERROR: $($_.Exception.Message)"
    if ($_.Exception.Response) {
        $stream = $_.Exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        Write-Output $reader.ReadToEnd()
    }
}
