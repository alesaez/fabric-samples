$global:fabricJsonMetadataMapper = @{
    DataPipeline = "pipelineId"
    Notebook = "notebookId"
    Report = "reportId"
    Workspace = "workspaceId"
}

$importItemsOrder = @(
    "VariableLibrary",
    "Lakehouse",
    "MirroredDatabase",
    "Environment",
    "Notebook",
    "SemanticModel",
    "Report",
    "DataPipeline",
    "CopyJob",
    "Eventhouse",
    "KQLDatabase",
    "KQLQueryset",
    "Reflex",
    "Eventstream",
    "Warehouse"
)

function WriteHeaderMessage($message) {
    $messageSeparator = ("=" * 10)
    $lineSeparator = "=" * ($message.Length + $messageSeparator.Length * 2 + 2)
    Write-Host ""
    Write-Host $lineSeparator -ForegroundColor Green
    Write-Host ($messageSeparator + " " + $message + " " + $messageSeparator) -ForegroundColor Green
    Write-Host $lineSeparator -ForegroundColor Green
    Write-Host ""

    # Add log message
    AddLogMessage -Message $lineSeparator 
    AddLogMessage -Message ($messageSeparator + " " + $message + " " + $messageSeparator)
    AddLogMessage -Message $lineSeparator 
}

function WriteSubHeaderMessage($message) {
    $messageSeparator = ("=" * 10)
    $lineSeparator = "=" * ($message.Length + $messageSeparator.Length * 2 + 2)
    Write-Host ""
    Write-Host ($messageSeparator + " " + $message + " " + $messageSeparator) -ForegroundColor Green
    Write-Host ""

    # Add log message
    AddLogMessage -Message ($messageSeparator + " " + $message + " " + $messageSeparator)
}

function WriteTable($data) {
    
    AddLogMessage -Message ($data | Format-Table -AutoSize | Out-String)

    # Get property names dynamically
    $properties = $data[0].PSObject.Properties.Name

    # Calculate max width for each column
    $columnWidths = @{}
    foreach ($prop in $properties) {
        $maxLength = ($data | ForEach-Object { if (($_.$prop) -eq $null) { 0 } else { ($_.$prop).ToString().Length } } | Measure-Object -Maximum).Maximum
        $columnWidths[$prop] = [Math]::Max($maxLength, $prop.Length) + 2  # Add padding
    }

    # Print header
    $header = ""
    foreach ($prop in $properties) {
        $header += (("{0,-" + $columnWidths[$prop] + "}") -f $prop)
    }
    Write-Host $header -ForegroundColor Cyan
    # AddLogMessage -Message $header 

    # Print separator
    $separator = ""
    foreach ($prop in $properties) {
        $separator += "-" * $columnWidths[$prop]
    }
    Write-Host $separator -ForegroundColor DarkGray
    # AddLogMessage -Message $separator

    # Print rows
    foreach ($item in $data) {
        $row = ""
        foreach ($prop in $properties) {
            $row += (("{0,-" + $columnWidths[$prop] + "}") -f $item.$prop)
        }
        Write-Host $row
        # AddLogMessage -Message $row
    }

}

function FindAndReplaceMetadata($filePath, $fabricJsonMetadataMapper, $itemsMetadata) {
    if (Test-Path -Path $filePath) {
        $content = Get-Content -Path $filePath -Raw
        $itemsMetadata = $itemsMetadata | Where-Object { $_.TargetId -ne $null } # Removing items with null TargetId
        
        foreach ($item in $itemsMetadata) {
            $itemType = $item.Type
            if ($fabricJsonMetadataMapper.ContainsKey($itemType)) {
                $metadataKey = $fabricJsonMetadataMapper[$itemType]
                $oldMetadata = '\"' + $metadataKey + '\":\s*\"' + $($item.SourceId) + '\"'
                $newMetadata = '"' + $metadataKey + '": "' + $($item.TargetId) + '"'
                if ($content -match $oldMetadata) {
                    $updatedContent = $content -replace $oldMetadata, $newMetadata
                    Set-Content -Path $filePath -Value $updatedContent
                    $content = $updatedContent # Update content variable to avoid multiple replacements in the same file
                    Write-Host "Updated metadata in file: $filePath" -ForegroundColor Green
                    AddLogMessage -Message "Updated metadata in file: $filePath" 
                    Write-Host "- $($item.SourceName) -> $($item.TargetName)" -ForegroundColor Green
                    AddLogMessage -Message "- $($item.SourceName) -> $($item.TargetName)"
                } else {
                    Write-Host "No metadata found to update in file: $filePath" -ForegroundColor Yellow
                    AddLogMessage -Message "No metadata found to update in file: $filePath"
                }
            } else {
                Write-Host "No metadata mapping found for item type '$itemType'. Skipping file: $filePath" -ForegroundColor Yellow
                AddLogMessage -Message "No metadata mapping found for item type '$itemType'. Skipping file: $filePath"
            }
        }
        
    } else {
        Write-Host "File not found: $filePath" -ForegroundColor Red
        AddLogMessage -Message "File not found: $filePath"
    }
}

# Function to sort local items based on the specified order
function SortLocalItems {
    param (
        [Parameter(Mandatory = $true)]
        [System.IO.DirectoryInfo[]]$Items,
        [Parameter(Mandatory = $true)]
        [string[]]$Order
    )

    $sortedItems = @()
    foreach ($itemType in $Order) {
        $itemsOfType = $Items | Where-Object { $_.Name.EndsWith(".$itemType") } | ForEach-Object { 
            [PSCustomObject]@{
                Name = $_.Name
                Type = $_.Name.Split(".")[-1]
                FullName = $_.FullName
                DisplayName = $_.Name.Split(".")[0]
                Item = $_
            }
        }

        if ($itemType -eq "DataPipeline") {
            # Check DataPipeline dependencies and sort by dependencies
            $dataPipelinesSortedByDependencies = @()
            $itemsWithDependencies = @()
            WriteSubHeaderMessage "Checking DataPipeline dependencies for items of type: $itemType"
            $itemsOfType | ForEach-Object {
                $dependencies = @(CheckDataPipelineDependencies -Item $_.Item)
                $_ | Add-Member -MemberType NoteProperty -Name Dependencies -Value $dependencies -Force
            }

            # Adding items with no dependencies to the sorted list first
            $dataPipelinesSortedByDependencies += $itemsOfType | Where-Object { $_.Dependencies.Count -eq 0 } 
            
            # Check dataPipelinesSortedByDependencies and add items where its dependencies have been already added
            $itemsWithDependencies += $itemsOfType | Where-Object { $_.Dependencies.Count -gt 0 }

            do {
                foreach ($item in $itemsWithDependencies) {
                    Write-Host "Checking dependencies for item: $($item.DisplayName)"
                    AddLogMessage -Message "Checking dependencies for item: $($item.DisplayName)"
                    $dependenciesNames = $item.Dependencies | ForEach-Object { $_.displayName + "." + $_.type }
                    
                    if (-not (($dependenciesNames | ForEach-Object { ($dataPipelinesSortedByDependencies.Name) -contains $_ }) -contains $false)) {
                        $dataPipelinesSortedByDependencies += $item
                        $itemsWithDependencies = $itemsWithDependencies | Where-Object { $_.Name -ne $item.Name } # Remove item from itemsWithDependencies
                    }
                }
            
            } while ($itemsWithDependencies.Count -gt 0)

            $itemsOfType = $dataPipelinesSortedByDependencies
        }

        $sortedItems += $itemsOfType
    }

    # Add any items that are not in the specified order at the end
    $remainingItems = $Items | Where-Object { -not ($_.Name -in $sortedItems.Name) } | ForEach-Object { 
        [PSCustomObject]@{
            Name = $_.Name
            Type = $_.Name.Split(".")[-1]
            FullName = $_.FullName
            DisplayName = $_.Name.Split(".")[0]
            Item = $_
        }
    }
    
    $sortedItems += $remainingItems

    return $sortedItems
}

# Check DataPipelines dependencies by checking if file pipeline-content.json contains references to other pipelines
function CheckDataPipelineDependencies {
    param (
        [Parameter(Mandatory = $true)]
        [System.IO.DirectoryInfo]$Item
    )
    # Define an empty array to hold dependencies
    $dependencies = @()
    $regexPattern = '\"pipelineId\":\s*\"([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})\"'

    $pipelineFile = Join-Path -Path $Item.FullName -ChildPath "pipeline-content.json"
    
    if (Test-Path -Path $pipelineFile) {
        $pipelineContent = Get-Content -Path $pipelineFile -Raw
        
        # Check if JSON matches regex pattern
        if ($pipelineContent -match $regexPattern) {
            # Write-Host "Pipeline file '$pipelineFile' contains dependencies." -ForegroundColor Green
            $dependenciesId = @()
            $dependenciesId += ([regex]::Matches($pipelineContent, $regexPattern).Groups | ? { $_.Value -notlike '"pipelineId"*' }).Value | Select-Object -Unique
            
            # foreach ($depId in $dependenciesId) {
            #     $dependencies += $wsItems | Where-Object { $_.Id -eq $depId -and $_.Type -eq "DataPipeline" }
            # }
            # Add Worksapace items that match the dependenciesId
            # Write-Host "Found dependencies with IDs: $($dependenciesId -join ', ')" -ForegroundColor Yellow
            $dependencies += $wsItems | Where-Object { $_.Type -eq "DataPipeline" -and $_.Id -in $dependenciesId }
        } 
        
    }

    return $dependencies
}

# Function to Sort Folders using ParentFolderId to determine hierarchy
function SortFolders {
    param (
        [Parameter(Mandatory = $true)]
        [System.Collections.ArrayList]$Folders
    )

    $sortedFolders = @()
    $folderLookup = @{}

    # Create a lookup for folders by their ID
    foreach ($folder in $Folders) {
        $folderLookup[$folder.id] = $folder
    }

    # Add root folders (those without ParentFolderId) adding property RelativePath
    $rootFolders = $Folders | Where-Object { -not $_.ParentFolderId }
    $rootFolders | ForEach-Object {
        $_ | Add-Member -MemberType NoteProperty -Name "RelativePath" -Value $_.DisplayName -Force
        $_ | Add-Member -MemberType NoteProperty -Name "TargetId" -Value "" -Force
    }
    $sortedFolders += $rootFolders

    # Get all folders that have a ParentFolderId
    $foldersWithParent = $Folders | Where-Object { $_.ParentFolderId }

    do {
        # Sort folders by ParentFolderId
        foreach ($folder in $foldersWithParent) {
            if ($folderLookup.ContainsKey($folder.ParentFolderId) -and ($sortedFolders | Where-Object { $_.id -eq $folder.ParentFolderId })) {
                $parentFolder = $folderLookup[$folder.ParentFolderId]
                if ($parentFolder) {
                    $folder | Add-Member -MemberType NoteProperty -Name "RelativePath" -Value ($parentFolder.RelativePath + "\" + $folder.DisplayName) -Force
                    $folder | Add-Member -MemberType NoteProperty -Name "TargetId" -Value "" -Force
                    $sortedFolders += $folder
                    # Remove the folder from foldersWithParent to avoid reprocessing
                    $foldersWithParent = $foldersWithParent | Where-Object { $_.id -ne $folder.id }
                }
            }
            
        }
    
    } while ($foldersWithParent.Count -gt 0)
    

    return $sortedFolders
}

# Function to add information message on a log file
function AddLogMessage {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Message
    )
    
    $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $logMessage = "$timestamp - $Message"
    Add-Content -Path $global:LogFilePath -Value $logMessage 
}

# Function to handle authentication options
function FabricLogin {
    param (
        [Parameter(Mandatory = $true)]
        [ValidateSet("Identity", "Interactive", "ServicePrincipal")]
        [string]$AuthType
    )

    if ((Get-Date) -gt $global:refreshTokenAfterDate) {
        Write-Host "Fabric CLI token authentication - Refreshing token..." -ForegroundColor Yellow
        AddLogMessage -Message "Fabric CLI token authentication - Refreshing token..."
        switch ($AuthType) {
            "Identity" {
                fab auth login --identity
            }
            "Interactive" {
                fab auth login 
            }
            "ServicePrincipal" {
                fab auth login -u $global:clientId -p $global:clientSecret --tenant $global:tenantId
            }
            default {
                Write-Host "Invalid authentication type. Please choose 'Identity', 'Interactive', or 'ServicePrincipal'."
            }
        }
        $global:refreshTokenAfterDate = (Get-Date).AddMinutes(45)
    }
}