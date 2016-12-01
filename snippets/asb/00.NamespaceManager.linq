<Query Kind="Program">
  <NuGetReference>WindowsAzure.ServiceBus</NuGetReference>
  <Namespace>Microsoft.ServiceBus</Namespace>
  <Namespace>Microsoft.ServiceBus.Messaging</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
</Query>

void Main()
{
	MainAsync().GetAwaiter().GetResult();
}

static async Task MainAsync()
{
	//var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus.ConnectionString");
	//var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
	
	var manageKeyName = Environment.GetEnvironmentVariable("AzureServiceBus.ManageKeyName");
	var manageKey = Environment.GetEnvironmentVariable("AzureServiceBus.ManageKey");
	var ns = Environment.GetEnvironmentVariable("AzureServiceBus.Namespace");
	
	TokenProvider credentials = TokenProvider.CreateSharedAccessSignatureTokenProvider(manageKeyName, manageKey);
	Uri serviceBusUri = ServiceBusEnvironment.CreateServiceUri("sb", ns, string.Empty);	
	NamespaceManager namespaceManager = new NamespaceManager(serviceBusUri, credentials);
	
	var queueDescription = new QueueDescription("myQueue");
	queueDescription.EnablePartitioning = true;
	
	if(!await namespaceManager.QueueExistsAsync(queueDescription.Path)){
		await namespaceManager.CreateQueueAsync(queueDescription).ConfigureAwait(false);
		$"Queue created".Dump();
	}
	else{
		$"Queue already exists".Dump();
	}	

}