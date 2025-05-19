###
# This script exports all items or specific types of items in a specified workspace using the Microsoft Fabric CLI.
# Please ensure you have the Microsoft Fabric CLI installed and authenticated before running this script.
# pip install ms-fabric-cli
# 

# Importing Helper functions
. ./utils.ps1

# Login to Microsoft Fabric CLI if needed - uncomment the line below if you haven't logged in yet
# fab auth login

$workspaceName = "EWLH-POC" #"<WORKSPACE NAME>" # Replace with your workspace name

$pauseEachWorkspace = $false # Set to $true to pause after each workspace

$timestamp = Get-Date -Format "yyyyMMddHHmm"
$exportParentPath = ".\exports\$timestamp" # Path to export items 
$exportItemTypes = @("DataPipeline") # Empty to Export all Items or you can specify especific items, currently supported: @("CopyJob","DataPipeline","Eventhouse","Eventstream","KQLDashboard","KQLDatabase","KQLQueryset","MirroredDatabase","MountedDataFactory","Notebook","Reflex","Report","SemanticModel","SparkJobDefinition","VariableLibrary")  

if (-not (Test-Path -Path "$exportParentPath")) {
    New-Item -ItemType Directory -Path "$exportParentPath" | Out-Null
}

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

    # Create new directory for exports if it doesn't exist
    $exportPath = "$exportParentPath\$wsFullName"
    if (-not (Test-Path -Path "$exportPath")) {
        New-Item -ItemType Directory -Path "$exportPath" | Out-Null
    }

    WriteSubHeaderMessage "Creating Subfolders in the Export Path: $exportPath"
    # Create subfolders for each item type
    foreach ($folder in $sortedFolders) {
        $folderPath = Join-Path -Path $exportPath -ChildPath $folder.RelativePath
        if (-not (Test-Path -Path $folderPath)) {
            New-Item -ItemType Directory -Path $folderPath | Out-Null
            Write-Host "New folder: $folderPath created."
        }
    }

    WriteSubHeaderMessage "Exporting items to folder: $exportPath"
    Write-Host "When you export item definition, the sensitivity label is not a part of the definition." -ForegroundColor Yellow

    if ($exportItemTypes.Count -eq 0) {
        Write-Host "Exporting all supported items in the workspace."
        $exportItemTypes = @("CopyJob","DataPipeline","Eventhouse","Eventstream","KQLDashboard","KQLDatabase","KQLQueryset","MirroredDatabase","MountedDataFactory","Notebook","Reflex","Report","SemanticModel","SparkJobDefinition","VariableLibrary") # Supported item types for export
        
        # Commenting full export for now since it doesn't support folders
        #fab export $wsFullName -a -o $exportPath -f

    } 

    Write-Host "Exporting items of types: $($exportItemTypes -join ', ')" 

    $exportableItems = $wsItems| Where-Object { $exportItemTypes -contains $_.Type }
    $notExportedItems = $wsItems | Where-Object { -not ($exportItemTypes -contains $_.Type) }

    foreach ($item in $exportableItems) {
        $itemFullName = $item.DisplayName + "." + $item.Type
        $itemName = $item.DisplayName
        $itemType = $item.Type

        Write-Host "Exporting item: $itemFullName"
        $itemExportPath = $exportPath
        if ($item.folderId) {
            $itemFolder = $sortedFolders | Where-Object { $_.id -eq $item.folderId }
            if ($itemFolder) {
                $itemExportPath = Join-Path -Path $exportPath -ChildPath $itemFolder.RelativePath
            } else {
                Write-Host "Folder for item '$itemName' not found. Exporting to root folder." -ForegroundColor Yellow
            }
        }

        try {
            fab export "$wsFullName/$itemFullName" -o $itemExportPath -f
        } catch {
            Write-Host "Failed to export item '$itemName' of type '$itemType'. Error: $_" -ForegroundColor Red
            $notExportedItems += $item
        }
    }

    if ($notExportedItems.Count -gt 0) {
        WriteSubHeaderMessage "Items Not Exported Summary"
        WriteTable ($notExportedItems | Group-Object type | ForEach-Object {
            [PSCustomObject]@{
                Type  = $_.Name
                Count = $_.Count
            }
        })
    } else {
        Write-Host "All items exported successfully." -ForegroundColor Green
    }
    

    if ($pauseEachWorkspace) {
        Write-Host "Press Enter to continue to the next workspace or Ctrl+C to stop execution..."
    }
}


# End of script