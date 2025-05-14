# This sample script calls the Fabric API to programmatically commit selective changes from workspace to Git.

# For documentation, please see:
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/get-status
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/commit-to-git

# Instructions:
# 1. Install PowerShell (https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
# 2. Install Azure PowerShell Az module (https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell)
# 3. Run PowerShell as an administrator
# 4. Fill in the parameters below
# 5. Change PowerShell directory to where this script is saved
# 6. > ./GitIntegration-CommitSelective.ps1
# 7. [Optional] Wait for long running operation to be completed - see LongRunningOperation-Polling.ps1

# Parameters - fill these in before running the script!
# =====================================================

$workspaceName = "<WORKSPACE NAME>"       # The name of the workspace

$commitMessage = "<COMMIT MESSAGE>"       # The commit message

$itemTypesNames = @("CopyJob", "Dashboard", "DataPipeline", "Dataflow", "Datamart", "Environment", "Eventhouse", "Eventstream", "GraphQLApi", "KQLDashboard", "KQLDatabase", "KQLQueryset", "Lakehouse", "MLExperiment", "MLModel", "MirroredDatabase", "MirroredWarehouse", "MountedDataFactory", "Notebook", "PaginatedReport", "Reflex", "Report", "SQLDatabase", "SQLEndpoint", "SemanticModel", "SparkJobDefinition", "VariableLibrary", "Warehouse")  

$batchSize = 1000 # The number of items to be processed in each batch. The default value is 1000.

$principalType = "<PRINCIPAL TYPE>" # Choose either "UserPrincipal" or "ServicePrincipal"

# Relevant for ServicePrincipal
$clientId = "<CLIENT ID>"                   #The application (client) ID of the service principal
$tenantId = "<TENANT ID>"                   #The directory (tenant) ID of the service principal
$servicePrincipalSecret = "<SECRET VALUE>"  #The secret value of the service principal

# End Parameters =======================================

$global:baseUrl = "<Base URL>" # Replace with environment-specific base URL. For example: "https://api.fabric.microsoft.com/v1"

$global:resourceUrl = "https://api.fabric.microsoft.com"

$global:fabricHeaders = @{}

function SetFabricHeaders() {
    if ($principalType -eq "UserPrincipal") {
        $secureFabricToken = GetSecureTokenForUserPrincipal
    }
    elseif ($principalType -eq "ServicePrincipal") {
        $secureFabricToken = GetSecureTokenForServicePrincipal

    }
    else {
        throw "Invalid principal type. Please choose either 'UserPrincipal' or 'ServicePrincipal'."
    }

    # Convert SecureString to plain text
    $fabricToken = ConvertSecureStringToPlainText($secureFabricToken)

    $global:fabricHeaders = @{
        'Content-Type'  = "application/json"
        'Authorization' = "Bearer $fabricToken"
    }
}

function GetSecureTokenForUserPrincipal() {
    #Login to Azure interactively
    Connect-AzAccount | Out-Null

    # Get authentication
    $secureFabricToken = (Get-AzAccessToken -AsSecureString -ResourceUrl $global:resourceUrl).Token

    return $secureFabricToken
}

function GetSecureTokenForServicePrincipal() {
    $secureServicePrincipalSecret = ConvertTo-SecureString -String $servicePrincipalSecret -AsPlainText -Force
    $credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $clientId, $secureServicePrincipalSecret

    #Login to Azure using service principal
    Connect-AzAccount -ServicePrincipal -TenantId $tenantId -Credential $credential | Out-Null

    # Get authentication
    $secureFabricToken = (Get-AzAccessToken -AsSecureString -ResourceUrl $global:resourceUrl).Token
    
    return $secureFabricToken
}

function ConvertSecureStringToPlainText($secureString) {
    $ssPtr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureString)
    try {
        $plainText = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($ssPtr)
    }
    finally {
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ssPtr)
    }
    return $plainText
}

function GetWorkspaceByName($workspaceName) {
    # Get workspaces    
    $getWorkspacesUrl = "$global:baseUrl/workspaces"
    $workspaces = (Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getWorkspacesUrl -Method GET).value

    # Try to find the workspace by display name
    $workspace = $workspaces | Where-Object { $_.DisplayName -eq $workspaceName }

    return $workspace
}

function GetErrorResponse($exception) {
    # Relevant only for PowerShell Core
    $errorResponse = $_.ErrorDetails.Message
 
    if (!$errorResponse) {
        # This is needed to support Windows PowerShell
        if (!$exception.Response) {
            return $exception.Message
        }
        $result = $exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($result)
        $reader.BaseStream.Position = 0
        $reader.DiscardBufferedData()
        $errorResponse = $reader.ReadToEnd();
    }
 
    return $errorResponse
}

function WriteHeaderMessage($message) {
    $messageSeparator = ("=" * 10)
    $lineSeparator = "=" * ($message.Length + $messageSeparator.Length * 2 + 2)
    Write-Host ""
    Write-Host $lineSeparator -ForegroundColor Green
    Write-Host ($messageSeparator + " " + $message + " " + $messageSeparator) -ForegroundColor Green
    Write-Host $lineSeparator -ForegroundColor Green
    Write-Host ""
}

function WriteTable($data) {
    
    # Get property names dynamically
    $properties = $data[0].PSObject.Properties.Name

    # Calculate max width for each column
    $columnWidths = @{}
    foreach ($prop in $properties) {
        $maxLength = ($data | ForEach-Object { if (($_.$prop) -eq $null) { 0 } else { ($_.$prop).ToString().Length } } | Measure-Object -Maximum).Maximum
        $columnWidths[$prop] = [Math]::Max($maxLength, $prop.Length) + 2  # Add padding
    }

    # Print header
    $header = ""
    foreach ($prop in $properties) {
        $header += (("{0,-" + $columnWidths[$prop] + "}") -f $prop)
    }
    Write-Host $header -ForegroundColor Cyan

    # Print separator
    $separator = ""
    foreach ($prop in $properties) {
        $separator += "-" * $columnWidths[$prop]
    }
    Write-Host $separator -ForegroundColor DarkGray

    # Print rows
    foreach ($item in $data) {
        $row = ""
        foreach ($prop in $properties) {
            $row += (("{0,-" + $columnWidths[$prop] + "}") -f $item.$prop)
        }
        Write-Host $row
    }

}

    
function GetLongRunningOperationStatus($operationId, $retryAfter, $wait = $false) {

    try {    
        # Get Long Running Operation
        Write-Host "Polling long running operation ID '$operationId' has been started with a retry-after time of '$retryAfter' seconds."
        
        $getOperationState = "{0}/operations/{1}" -f $global:baseUrl, $operationId
        do {
            $operationState = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getOperationState -Method GET

            Write-Host "Long running operation status: $($operationState.Status)"

            if ($operationState.Status -in @("NotStarted", "Running")) {
                Start-Sleep -Seconds [int]$retryAfter
            }
        } while (($operationState.Status -in @("NotStarted", "Running")) -and $wait)

        
        if ($operationState.Status -eq "Failed") {
            Write-Host "The long running operation has been completed with failure. Error reponse: $($operationState.Error | ConvertTo-Json)" -ForegroundColor Red
        }
        
        return $operationState.Status

    }
    catch {
        $errorResponse = GetErrorResponse($_.Exception)
        Write-Host "Failed to retrieve long running operation status. Error reponse: $errorResponse" -ForegroundColor Red
    }
    
}

try {
    SetFabricHeaders

    $workspace = GetWorkspaceByName $workspaceName 
    
    # Verify the existence of the requested workspace
    if (!$workspace) {
        Write-Host "A workspace with the requested name was not found." -ForegroundColor Red
        return
    }

    # Get Status
    WriteHeaderMessage "Calling GET Status REST API - Git Status."

    $gitStatusUrl = "$global:baseUrl/workspaces/$($workspace.Id)/git/status"
    $gitStatusResponse = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $gitStatusUrl -Method GET
    Write-Host "Git Status Result: $($gitStatusResponse.Changes.Count) changes" -ForegroundColor Green

    # Get selected changes
    $selectedChanges = @($gitStatusResponse.Changes | Where-Object {
        ($itemTypesNames -icontains $_.ItemMetadata.ItemType)
        })

    # Checking if selected changes are greater than the batch size
    WriteHeaderMessage "Filtering selected changes by item type"
    $filteredChangesToDisplay = @()
    $selectedChanges| ForEach-Object { 
        $filteredChangesToDisplay += [PSCustomObject]@{
            displayName = $_.ItemMetadata.DisplayName
            itemType = $_.ItemMetadata.ItemType
            workspaceChange = $_.WorkspaceChange
            remoteChange = $_.RemoteChange
            conflictType = $_.ConflictType
        }
    }
    WriteTable $filteredChangesToDisplay
    ### Commenting the line below to see the filtered changes since Format Table is not displaying correctly
    ### $selectedChanges | Format-Table -Property @{Label = "DisplayName"; Expression = { $_.itemMetadata.displayName } }, @{Label = "ItemType"; Expression = { $_.itemMetadata.itemType } }, workspaceChange, remoteChange, conflictType -AutoSize
    
    # Loop through the selected changes and group them by type
    $groupedChanges = $selectedChanges | Group-Object -Property { $_.ItemMetadata.ItemType }
    $batchOperations = @()

    # iterate through each group and process the changes and commit them to Git by batches
    foreach ($group in $groupedChanges) {
        $itemType = $group.Name
        $items = $group.Group

        Write-Host "Processing changes for item type: $itemType. Items count $($items.Count)" -ForegroundColor Green
        
        # Process the items in batches
        for ($i = 0; $i -lt $items.Count; $i += $batchSize) {
            $batch = $items[$i..[math]::Min($i + $batchSize - 1, $items.Count - 1)]

            # Commit the batch to Git
            Write-Host "Committing batch of size: $($batch.Count) for item type: '$itemType' from workspace '$workspaceName' to Git." -ForegroundColor Green
                    
            # Commit to Git
            $commitToGitUrl = "$global:baseUrl/workspaces/$($workspace.Id)/git/commitToGit"

            $commitToGitBody = @{ 		
                mode    = "Selective"
                items   = @($batch | ForEach-Object { @{
                            objectId  = $_.ItemMetadata.ItemIdentifier.ObjectId
                            logicalId = $_.ItemMetadata.ItemIdentifier.LogicalId
                        }
                    })
                comment = $commitMessage + " - item type: $itemType - batch count: $($batch.Count)" # Adding item type and batch count to the commit message for tracking
            } | ConvertTo-Json

            $commitToGitResponse = Invoke-WebRequest -Headers $global:fabricHeaders -Uri $commitToGitUrl -Method POST -Body $commitToGitBody

            $operationId = $commitToGitResponse.Headers['x-ms-operation-id']
            $retryAfter = $commitToGitResponse.Headers['Retry-After']
            Write-Host "Long Running Operation ID: '$operationId' has been scheduled for committing changes from workspace '$workspaceName' to Git with a retry-after time of '$retryAfter' seconds." -ForegroundColor Green
            Start-Sleep -Seconds [int]$retryAfter
            $operationStatus = GetLongRunningOperationStatus -operationId $operationId -retryAfter $retryAfter -wait $false

            $batchOperations += [PSCustomObject]@{ 		
                batchId       = $i
                operationId   = $operationId
                retryAfter    = $retryAfter
                workspaceName = $workspaceName
                itemType      = $itemType
                itemCount     = $batch.Count
                status        = $operationStatus
            }

        }
    
    }

    # Print the batch operations
    WriteHeaderMessage "Git Commit Batches Operations Summary"
    
    ### $batchOperations | Format-Table -Property workspaceName, itemType, batchId, itemCount, operationId, retryAfter, status -AutoSize
    WriteTable $batchOperations

    Write-Host "Optionally, you can wait for the long-running operations to complete here or handle them separately." -ForegroundColor Yellow
    Write-Host "You can use the LongRunningOperation-Polling.ps1 script to check the status of the operations." -ForegroundColor Yellow
    Write-Host "Example: GetLongRunningOperationStatus -operationId {operation-id} -retryAfter {retry-after}" -ForegroundColor Yellow
    Write-Host "Example waiting for completion: GetLongRunningOperationStatus -operationId {operation-id} -retryAfter {retry-after} -wait `$true" -ForegroundColor Yellow
    
}
catch {
    $errorResponse = GetErrorResponse($_.Exception)
    Write-Host "Failed to commit changes from workspace '$workspaceName' to Git. Error reponse: $errorResponse" -ForegroundColor Red
}