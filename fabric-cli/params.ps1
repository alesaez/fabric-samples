
### Fabric authentication details
$AuthType = "Interactive" # Set to  'Identity', 'Interactive', or 'ServicePrincipal'
$global:clientId = "<client_id>" # Replace with your client ID
$global:clientSecret = "<client_secret>" # Replace with your client secret
$global:tenantId = "<tenant_id>" # Replace with your tenant ID

### Fabric workspace details to audit/export
$workspaceName = "<WORKSPACE NAME>" # Replace with your workspace name if Empty, it will list all workspaces

# You can specify a custom timestamp if needed to re-run the script where you left off
$timestamp = (Get-Date -Format "yyyyMMddHHmm") 

# Export-WS-Items parameters
$exportItemTypes = @() # Empty to Export all Items or you can specify especific items, currently supported: @("CopyJob","DataPipeline","Eventhouse","Eventstream","KQLDashboard","KQLDatabase","KQLQueryset","MirroredDatabase","MountedDataFactory","Notebook","Reflex","Report","SemanticModel","SparkJobDefinition","VariableLibrary")  
$exportParentPath = ".\exports\$timestamp" # Path to export items 

# List-WS-Items parameters
$auditParentPath = ".\audit\$timestamp" # Path to export audit files like workspace list

# General parameters
$listItems = $true # Set to $true to list items in the workspace
$exportCSV = $true # Set to $true to export items to CSV
$pauseEachWorkspace = $false # Set to $true to pause after each workspace
$replaceFileIfExists = $true # Set to $true to replace the file if it already exists

