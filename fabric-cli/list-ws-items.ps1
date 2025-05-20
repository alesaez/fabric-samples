###
# This script lists all items in a specified workspace using the Microsoft Fabric CLI.
# Please ensure you have the Microsoft Fabric CLI installed and authenticated before running this script.
# pip install ms-fabric-cli
# 

# Importing Helper functions
. ./utils.ps1

$AuthType = "Interactive" # Set to  'Identity', 'Interactive', or 'ServicePrincipal'
$global:clientId = "<client_id>" # Replace with your client ID
$global:clientSecret = "<client_secret>" # Replace with your client secret
$global:tenantId = "<tenant_id>" # Replace with your tenant ID
$global:refreshTokenAfterDate = (Get-Date)

FabricLogin -AuthType $AuthType 

$workspaceName = "<WORKSPACE NAME>" # Replace with your workspace name if Empty, it will list all workspaces
$listItems = $true # Set to $true to list items in the workspace
$exportCSV = $true # Set to $true to export items to CSV
$pauseEachWorkspace = $false # Set to $true to pause after each workspace

# List all workspaces for the current user
$workspaces = (fab api -X get workspaces --show_headers | ConvertFrom-Json).text.value
if ($exportCSV) {
    $workspaces | Export-Csv -Path ".\workspaces.csv" -NoTypeInformation
}

if ($workspaceName) {
    $workspaces = $workspaces | Where-Object { $_.displayName -eq $workspaceName }
}

foreach ($ws in $workspaces) {
    $wsId =  $ws.id
    $wsName = $ws.displayName
    $wsFullName = $ws.displayName + "." + $ws.type

    WriteHeaderMessage "Listing items in workspace: $wsName [$wsId]"

    $wsItems = (fab api workspaces/$wsId/items | ConvertFrom-Json).text.value

    Write-Host "Total items in workspace '$wsName': $($wsItems.Count) " 
    if ($wsItems.Count -eq 0) {
        continue
    }

    WriteSubHeaderMessage "Workspace Subfolders"
    $wsFolders = (fab api workspaces/$wsId/folders | ConvertFrom-Json).text.value
    $sortedFolders = SortFolders -Folders $wsFolders
    WriteTable ($sortedFolders | Select-Object id, displayName,RelativePath)

    # Adding Subfolder information to items
    $wsItems | ForEach-Object {
        $folderId = $_.folderId
        if ($folderId) {
            $subfolder = ($sortedFolders | Where-Object {$_.id -eq $folderId}).RelativePath
            $_ | Add-Member -MemberType NoteProperty -Name subfolder -Value $subfolder -Force
        }
    }

    $itemsCountByType = $wsItems | Group-Object type | ForEach-Object {
        [PSCustomObject]@{
            Type  = $_.Name
            Count = $_.Count
        }
    }
    WriteSubHeaderMessage "Items Count by Type"
    WriteTable $itemsCountByType
    # $wsItemsWithSubfolders = $wsItems | Sort-Object Type, DisplayName | Select-Object DisplayName, Type, Subfolder@{Name="Subfolder";Expression={if($_.folderId){$folderId = $_.folderId; ($sortedFolders | Where-Object {$_.id -eq $folderId}).RelativePath}else{""}}}

    if ($listItems) {
        WriteSubHeaderMessage "Items Sorted by Type and Name"
        WriteTable ($wsItems | Sort-Object Type, DisplayName | Select-Object DisplayName, Type, Subfolder)
    }

    if ($exportCSV) {
        Write-Host "Exported items to .\$($wsName)_items.csv"
        $wsItems | Select-Object id, displayName, type, description, workspaceId, folderId, subfolder | Export-Csv -Path ".\$($wsName)_items.csv" -NoTypeInformation
    }

    if ($pauseEachWorkspace) {
        Write-Host "Press Enter to continue to the next workspace or Ctrl+C to stop execution..."
    }
}
