###
# This script lists all items in a specified workspace using the Microsoft Fabric CLI.
# Please ensure you have the Microsoft Fabric CLI installed and authenticated before running this script.
# pip install ms-fabric-cli
# 

# Importing Helper functions
. ./utils.ps1

# Login to Microsoft Fabric CLI if needed - uncomment the line below if you haven't logged in yet
# fab auth login

$workspaceName = "<WORKSPACE NAME>" # Replace with your workspace name if Empty, it will list all workspaces
$listItems = $true # Set to $true to list items in the workspace
$pauseEachWorkspace = $false # Set to $true to pause after each workspace

# List all workspaces for the current user
$workspaces = (fab api -X get workspaces --show_headers | ConvertFrom-Json).text.value

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

    $itemsCountByType = $wsItems | Group-Object type | ForEach-Object {
        [PSCustomObject]@{
            Type  = $_.Name
            Count = $_.Count
        }
    }
    WriteSubHeaderMessage "Items Count by Type"
    WriteTable $itemsCountByType

    if ($listItems) {
        WriteSubHeaderMessage "Items Sorted by Type and Name"
        WriteTable ($wsItems | Sort-Object Type, DisplayName | Select-Object DisplayName, Type, @{Name="Subfolder";Expression={if($_.folderId){$folderId = $_.folderId; ($sortedFolders | Where-Object {$_.id -eq $folderId}).RelativePath}else{""}}})
    }

    if ($pauseEachWorkspace) {
        Write-Host "Press Enter to continue to the next workspace or Ctrl+C to stop execution..."
    }
}
