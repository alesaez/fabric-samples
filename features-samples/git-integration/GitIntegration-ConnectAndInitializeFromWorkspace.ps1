# This sample script calls the Fabric API to programmatically connect to and update the workspace with content in Git.

# For documentation, please see:
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/connect
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/initialize-connection
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-from-git
# https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-state

# Instructions:
# 1. Install PowerShell (https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
# 2. Install Azure PowerShell Az module (https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell)
# 3. Run PowerShell as an administrator
# 4. Fill in the parameters below
# 5. Change PowerShell directory to where this script is saved
# 6. > ./GitIntegration-ConnectAndUpdateFromGit.ps1

# Parameters - fill these in before running the script!
# =====================================================

$workspaceName = "<WORKSPACE NAME>"      # The name of the workspace

$shouldDisconnect = $false               # Determines whether the workspace should be disconnected before connecting to a new Azure DevOps connection details.

# AzureDevOps details
$azureDevOpsDetails = @{
    gitProviderType = "AzureDevOps"
    organizationName = "<ORGANIZATION NAME>"
    projectName = "<PROJECT NAME>"
    repositoryName = "<REPOSITORY NAME>"
    branchName = "<BRANCH NAME>"
    directoryName = "<DIRECTORY NAME>"
}

# GitHub details
$gitHubDetails = @{
    gitProviderType = "GitHub"
    ownerName = "<OWNER NAME>"
    repositoryName = "<REPOSITORY NAME>"
    branchName = "<BRANCH NAME>"
    directoryName = "<DIRECTORY NAME>"
}

# Relevant for GitHub authentication
$connectionName = "" # Replace with the connection display name that stores the gitProvider credentials (Required for GitHub)

$gitProviderDetails = $gitHubDetails # Replace with specific Git provider, $azureDevOpsDetails or $gitHubDetails. Default to $gitHubDetails.

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
    } elseif ($principalType -eq "ServicePrincipal") {
        $secureFabricToken = GetSecureTokenForServicePrincipal

    } else {
        throw "Invalid principal type. Please choose either 'UserPrincipal' or 'ServicePrincipal'."
    }

    # Convert SecureString to plain text
    $fabricToken = ConvertSecureStringToPlainText($secureFabricToken)

    $global:fabricHeaders = @{
        'Content-Type' = "application/json"
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
    $secureServicePrincipalSecret  = ConvertTo-SecureString -String $servicePrincipalSecret -AsPlainText -Force
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
    } finally {
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ssPtr)
    }
    return $plainText
}

function GetWorkspaceByName($workspaceName) {
    # Get workspaces    
    $getWorkspacesUrl = "$global:baseUrl/workspaces"
    $workspaces = (Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getWorkspacesUrl -Method GET).value

    # Try to find the workspace by display name
    $workspace = $workspaces | Where-Object {$_.DisplayName -eq $workspaceName}

    return $workspace
}

function GetConnectionByName($connectionName) {
    # Get workspaces    
    $getConnectionsUrl = "$global:baseUrl/connections"
    $connections = (Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getConnectionsUrl -Method GET).value

    # Try to find the connection by display name
    $connection = $connections | Where-Object {$_.DisplayName -eq $connectionName}

    return $connection
}

function SelectConnectionByName() {
    # Get workspaces    
    $getConnectionsUrl = "$global:baseUrl/connections"
    $connections = (Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getConnectionsUrl -Method GET).value
    WriteHeaderMessage "Available Connections"
    $connections | Format-Table -Property Id, DisplayName, GitProviderType

    # Prompt user for connection name
    $connectionName = Read-Host "Please enter the connection name to use for GitHub authentication"

    # Try to find the connection by display name
    $connection = $connections | Where-Object {$_.DisplayName -eq $connectionName}

    return $connection
}

function GetErrorResponse($exception) {
    # Relevant only for PowerShell Core
    $errorResponse = $_.ErrorDetails.Message
 
    if(!$errorResponse) {
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
	if(!$workspace) {
	  Write-Host "A workspace with the requested name was not found." -ForegroundColor Red
	  return
	}
	
    if ($shouldDisconnect)
    {
        # Disconnect from Git
        WriteHeaderMessage "Disconnecting from Git"
        Write-Host "Disconnecting the workspace '$workspaceName' from Git."

        $disconnectUrl = "$global:baseUrl/workspaces/$($workspace.Id)/git/disconnect"
        Invoke-RestMethod -Headers $global:fabricHeaders -Uri $disconnectUrl -Method POST

        Write-Host "The workspace '$workspaceName' has been successfully disconnected from Git." -ForegroundColor Green
    }

    # Connect to Git
    WriteHeaderMessage "Connecting to Git"
    Write-Host "Connecting the workspace '$workspaceName' to Git."

    $connectUrl = "$global:baseUrl/workspaces/$($workspace.Id)/git/connect"
    
    $connectToGitBody = @{}

    if ($gitProviderDetails.GitProviderType -eq "AzureDevOps") {
        $connectToGitBody = @{
            gitProviderDetails =$gitProviderDetails
        } | ConvertTo-Json
    }

    if ($gitProviderDetails.GitProviderType -eq "GitHub") {
        
        if ([String]::IsNullOrEmpty($connectionName)) {
            $connection = SelectConnectionByName
        }
        else {
            $connection = GetConnectionByName $connectionName
        }

        # Verify the existence of the requested connection
    	if(!$connection) {
    	  Write-Host "A connection with the requested name was not found." -ForegroundColor Red
    	  return
    	}

        $connectToGitBody = @{
            gitProviderDetails = $gitProviderDetails
            myGitCredentials = @{
                source = "ConfiguredConnection"
                connectionId = $connection.id
            }
        } | ConvertTo-Json
    }
   
    Invoke-RestMethod -Headers $global:fabricHeaders -Uri $connectUrl -Method POST -Body $connectToGitBody

    Write-Host "The workspace '$workspaceName' has been successfully connected to Git." -ForegroundColor Green

    # Initialize Connection
    WriteHeaderMessage "Initializing Git Connection"
    Write-Host "Initializing Git connection for workspace '$workspaceName'."

    $initializeConnectionUrl = "$global:baseUrl/workspaces/$($workspace.Id)/git/initializeConnection"
    $initializeConnectionResponse = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $initializeConnectionUrl -Method POST

    WriteTable $initializeConnectionResponse

    Write-Host "The Git connection for workspace '$workspaceName' has been successfully requested." -ForegroundColor Green

    $operationId = $initializeConnectionResponse.Headers['x-ms-operation-id']
    $retryAfter = $initializeConnectionResponse.Headers['Retry-After']
    Write-Host "Long Running Operation ID: '$operationId' has been scheduled for initializing git connection with workspace '$workspaceName' with a retry-after time of '$retryAfter' seconds." -ForegroundColor Green
    Start-Sleep -Seconds [int]$retryAfter
    GetLongRunningOperationStatus -operationId $operationId -retryAfter $retryAfter -wait $true

    ### Commented out as this is not needed for the sample
    # if ($initializeConnectionResponse.RequiredAction -eq "UpdateFromGit") {

    #     # Update from Git
    #     Write-Host "Updating the workspace '$workspaceName' from Git."

    #     $updateFromGitUrl = "$global:baseUrl/workspaces/$($workspace.Id)/git/updateFromGit"

    #     $updateFromGitBody = @{ 
    #         remoteCommitHash = $initializeConnectionResponse.RemoteCommitHash
	# 	    workspaceHead = $initializeConnectionResponse.WorkspaceHead
    #     } | ConvertTo-Json

    #     $updateFromGitResponse = Invoke-WebRequest -Headers $global:fabricHeaders -Uri $updateFromGitUrl -Method POST -Body $updateFromGitBody

    #     $operationId = $updateFromGitResponse.Headers['x-ms-operation-id']
    #     $retryAfter = $updateFromGitResponse.Headers['Retry-After']
    #     Write-Host "Long Running Operation ID: '$operationId' has been scheduled for updating the workspace '$workspaceName' from Git with a retry-after time of '$retryAfter' seconds." -ForegroundColor Green
        
    #     # Poll Long Running Operation
    #     $getOperationState = "$global:baseUrl/operations/$operationId"
    #     do
    #     {
    #         $operationState = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getOperationState -Method GET

    #         Write-Host "Update from Git operation status: $($operationState.Status)"

    #         if ($operationState.Status -in @("NotStarted", "Running")) {
    #             Start-Sleep -Seconds $retryAfter
    #         }
    #     } while($operationState.Status -in @("NotStarted", "Running"))
    # }
    # else {
    #     Write-Host "Expected RequiredAction to be UpdateFromGit but found $($initializeConnectionResponse.RequiredAction)" -ForegroundColor Red
    # }

    # if ($operationState.Status -eq "Failed") {
    #     Write-Host "Failed to update the workspace '$workspaceName' with content from Git. Error reponse: $($operationState.Error | ConvertTo-Json)" -ForegroundColor Red
    # }
    # else{
    #     Write-Host "The workspace '$workspaceName' has been successfully updated with content from Git." -ForegroundColor Green
    # }
} catch {
    $errorResponse = GetErrorResponse($_.Exception)
    Write-Host "Failed to connect and update the workspace '$workspaceName' with content from Git. Error reponse: $errorResponse" -ForegroundColor Red
}