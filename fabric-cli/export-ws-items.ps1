###
# This script exports all items or specific types of items in a specified workspace using the Microsoft Fabric CLI.
# Please ensure you have the Microsoft Fabric CLI installed and authenticated before running this script.
# pip install ms-fabric-cli
# 

# Importing Helper functions
. ./utils.ps1

$AuthType = "Interactive" # Set to  'Identity', 'Interactive', or 'ServicePrincipal'
$global:clientId = "<client_id>" # Replace with your client ID
$global:clientSecret = "<client_secret>" # Replace with your client secret
$global:tenantId = "<tenant_id>" # Replace with your tenant ID

$workspaceName = "<WORKSPACE NAME>" # Replace with your workspace name
$listItems = $true # Set to $true to list items in the workspace
$exportCSV = $true # Set to $true to export items to CSV
$pauseEachWorkspace = $false # Set to $true to pause after each workspace

$timestamp = (Get-Date -Format "yyyyMMddHHmm") # You can specify a custom timestamp if needed to re-run the script where you left off
$exportParentPath = ".\exports\$timestamp" # Path to export items 
$global:LogFilePath = "$exportParentPath\log.txt" # Path to log files

$exportItemTypes = @() # Empty to Export all Items or you can specify especific items, currently supported: @("CopyJob","DataPipeline","Eventhouse","Eventstream","KQLDashboard","KQLDatabase","KQLQueryset","MirroredDatabase","MountedDataFactory","Notebook","Reflex","Report","SemanticModel","SparkJobDefinition","VariableLibrary")  

if (-not (Test-Path -Path "$exportParentPath")) {
    New-Item -ItemType Directory -Path "$exportParentPath" | Out-Null
}
$global:refreshTokenAfterDate = (Get-Date)

FabricLogin -AuthType $AuthType 

# List all workspaces for the current user
$workspaces = (fab api -X get workspaces --show_headers | ConvertFrom-Json).text.value

if ($exportCSV) {
    $workspaces | Export-Csv -Path "$exportParentPath\workspaces.csv" -NoTypeInformation -Force:$replaceFileIfExists
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

    WriteMessage "Total items in workspace '$wsName': $($wsItems.Count)" 
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

    if ($listItems) {
        WriteSubHeaderMessage "Items Sorted by Type and Name"
        WriteTable ($wsItems | Sort-Object Type, DisplayName | Select-Object DisplayName, Type, Subfolder)
    }
    
    if ($exportCSV) {
        Write-Host "Exported items to $exportParentPath\$($wsName)_items.csv"
        $wsItems | Select-Object id, displayName, type, description, workspaceId, folderId, subfolder | Export-Csv -Path "$exportParentPath\$($wsName)_items.csv" -NoTypeInformation
    }

    # Create new directory for exports if it doesn't exist
    $exportPath = "$exportParentPath\$wsName"
    if (-not (Test-Path -Path "$exportPath")) {
        New-Item -ItemType Directory -Path "$exportPath" | Out-Null
    }

    WriteSubHeaderMessage "Creating Subfolders in the Export Path: $exportPath"
    # Create subfolders for each item type
    foreach ($folder in $sortedFolders) {
        $folderPath = Join-Path -Path $exportPath -ChildPath $folder.RelativePath
        if (-not (Test-Path -Path $folderPath)) {
            New-Item -ItemType Directory -Path $folderPath | Out-Null
            WriteMessage "New folder: $folderPath created."
        }
    }

    WriteSubHeaderMessage "Exporting items to folder: $exportPath"
    WriteMessage "When you export item definition, the sensitivity label is not a part of the definition." -ForegroundColor Yellow

    if ($exportItemTypes.Count -eq 0) {
        WriteMessage "Exporting all supported items in the workspace."
        $exportItemTypes = @("CopyJob","DataPipeline","Eventhouse","Eventstream","KQLDashboard","KQLDatabase","KQLQueryset","MirroredDatabase","MountedDataFactory","Notebook","Reflex","Report","SemanticModel","SparkJobDefinition","VariableLibrary") # Supported item types for export
        
        # Commenting full export for now since it doesn't support folders
        #fab export $wsFullName -a -o $exportPath -f

    } 

    WriteMessage "Exporting items of types: $($exportItemTypes -join ', ')" 

    $exportableItems = $wsItems| Where-Object { $exportItemTypes -contains $_.Type }
    $notExportedItems = $wsItems | Where-Object { -not ($exportItemTypes -contains $_.Type) }
    $successfullyExportedItems = @()
    $erroredItems = @()

    $itemsCount = $exportableItems.Count
    WriteMessage "Total exportable items in workspace '$wsName': $itemsCount/$($wsItems.Count)" 
    AddLogMessage -Message "Total exportable items in workspace '$wsName': $itemsCount/$($wsItems.Count)"
    
    for ($i = 0; $i -lt $itemsCount; $i++) {
        $item = $exportableItems[$i]
        $itemFullName = $item.DisplayName + "." + $item.Type
        $itemName = $item.DisplayName
        $itemType = $item.Type

        $itemExportPath = $exportPath
        if ($item.folderId) {
            $itemFolder = $sortedFolders | Where-Object { $_.id -eq $item.folderId }
            if ($itemFolder) {
                $itemExportPath = Join-Path -Path $exportPath -ChildPath $itemFolder.RelativePath
            } else {
                WriteMessage "Folder for item '$itemName' not found. Exporting to root folder." -ForegroundColor Yellow
            }
        }
        
        WriteMessage "Exporting item [$($i+1)/$itemsCount]: $itemExportPath\$itemFullName" -ForegroundColor Cyan

        try {
            if (-not (Test-Path -Path (Join-Path -Path $itemExportPath -ChildPath $itemFullName))) {
                fab export "$wsFullName/$itemFullName" -o $itemExportPath -f 

                if ($LASTEXITCODE -gt 0) {
                    $errorMessage = (fab export "$wsFullName/$itemFullName" -o $itemExportPath -f) | Out-String
                    WriteMessage "Failed to export item '$itemName' of type '$itemType'. Error Code: $LASTEXITCODE" -ForegroundColor Red
                    WriteMessage $errorMessage -ForegroundColor Red
                    $eItem = $item
                    $eItem | Add-Member -MemberType NoteProperty -Name errorMessage -Value $errorMessage -Force
                    $erroredItems += $eItem
                }
                else {
                    WriteMessage "Item '$itemName' of type '$itemType' exported successfully." -ForegroundColor Green
                    $successfullyExportedItems += $item
                }

            }
            else {
                WriteMessage "Item '$itemFullName' already exists in the export path. Skipping export." -ForegroundColor Yellow
                $successfullyExportedItems += $item
            }
        } catch {
            WriteMessage "Failed to export item '$itemName' of type '$itemType'. Error: $_" -ForegroundColor Red
            $eItem = $item
            $eItem | Add-Member -MemberType NoteProperty -Name errorMessage -Value $_ -Force
            $erroredItems += $eItem
        }

        FabricLogin -AuthType $AuthType # Re-login to ensure the token is valid
    }

    if ($exportCSV) {
        WriteMessage "Exported successfully exported items to $exportParentPath\$($wsName)_successfully_exported_items.csv"
        $successfullyExportedItems | Select-Object id, displayName, type, description, workspaceId, folderId, subfolder | Export-Csv -Path "$exportParentPath\$($wsName)_successfully_exported_items.csv" -NoTypeInformation
    }

    if ($notExportedItems.Count -gt 0) {
        WriteSubHeaderMessage "Items Not Exported Summary"
        WriteTable ($notExportedItems | Group-Object type | ForEach-Object {
            [PSCustomObject]@{
                Type  = $_.Name
                Count = $_.Count
            }
        })
        
        WriteSubHeaderMessage "Items Not Exported Sorted by Type and Name"
        WriteTable ($notExportedItems | Sort-Object Type, DisplayName | Select-Object DisplayName, Type, Subfolder)

        if ($exportCSV) {
            WriteMessage "Exported items not exported to $exportParentPath\$($wsName)_not_exported_items.csv"
            $notExportedItems | Select-Object id, displayName, type, description, workspaceId, folderId, subfolder | Export-Csv -Path "$exportParentPath\$($wsName)_not_exported_items.csv" -NoTypeInformation
        }
        
    } 

    if ($erroredItems.Count -gt 0) {
        WriteSubHeaderMessage "Errored Items Summary"
        WriteTable ($erroredItems | Group-Object type | ForEach-Object {
            [PSCustomObject]@{
                Type  = $_.Name
                Count = $_.Count
            }
        })

        WriteSubHeaderMessage "Errored Items List Sorted by Type and Name"
        WriteTable ($erroredItems | Sort-Object Type, DisplayName | Select-Object DisplayName, Type, Subfolder)

        if ($exportCSV) {
            WriteMessage "Exported errored items to $exportParentPath\$($wsName)_errored_items.csv"
            $erroredItems | Select-Object id, displayName, type, description, workspaceId, folderId, subfolder, errorMessage | Export-Csv -Path "$exportParentPath\$($wsName)_errored_items.csv" -NoTypeInformation
        }
    }
    
    WriteMessage "Total successfully exported items: $($successfullyExportedItems.Count)/$($wsItems.Count)" -ForegroundColor Green
    WriteMessage "Total errored items: $($erroredItems.Count)/$($wsItems.Count)" -ForegroundColor Red
    WriteMessage "Total not exported items: $($notExportedItems.Count)/$($wsItems.Count)" -ForegroundColor Yellow

    if ($pauseEachWorkspace) {
        WriteMessage "Press Enter to continue to the next workspace or Ctrl+C to stop execution..."
    }
}

WriteMessage "Export completed. Check the log file at $global:LogFilePath for details." -ForegroundColor Green

# End of script
