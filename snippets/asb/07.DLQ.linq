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

static MessagingFactory factory;

static async Task MainAsync()
{
	var manageKeyName = Environment.GetEnvironmentVariable("AzureServiceBus.ManageKeyName", EnvironmentVariableTarget.User);
	var manageKey = Environment.GetEnvironmentVariable("AzureServiceBus.ManageKey", EnvironmentVariableTarget.User);
	var ns = Environment.GetEnvironmentVariable("AzureServiceBus.Namespace", EnvironmentVariableTarget.User);
	
	ServiceBusEnvironment.SystemConnectivity.Mode = ConnectivityMode.Tcp;
	
	TokenProvider credentials = TokenProvider.CreateSharedAccessSignatureTokenProvider(manageKeyName, manageKey);
	Uri serviceBusUri = ServiceBusEnvironment.CreateServiceUri("sb", ns, string.Empty);	
	
	await EnsureQueueExists("myQueue", credentials, serviceBusUri).ConfigureAwait(false);	


	var receiver = await CreateReceiver("myQueue", credentials, serviceBusUri).ConfigureAwait(false);
    var sender = await CreateSender("myQueue", credentials, serviceBusUri).ConfigureAwait(false);
	await sender.SendAsync(new BrokeredMessage("some data"));
	
	var x = "";
	while(x != "x")
	{
		var processingAttempt = 1;
		
		receiver = await EnsureActiveReceiver(receiver, "myQueue", credentials, serviceBusUri).ConfigureAwait(false);
		
		var receivedMessage = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
		
		while (receivedMessage != null)
		{				
			var body = receivedMessage.GetBody<string>();

			$"Proceccing attempt #{processingAttempt++}. Received message with body: {body}".Dump();

			await ProcessMessage(receivedMessage).ConfigureAwait(false);
			
			receivedMessage = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
		}
		$"No more messages to process".Dump();

		x = Util.ReadLine();
		
		if(x == "c"){
			$"Closing factory, this will also close the senders & receivers".Dump();
			await factory.CloseAsync();
		}
	}
	
	$"Stopping".Dump();
}

static Task ProcessMessage(BrokeredMessage message)
{
	// example: talking to a 3rd party that throws an exception or timesout
	return message.AbandonAsync();
}

static async Task EnsureConnection(TokenProvider credentials, Uri serviceBusUri){
    if(factory == null || factory.IsClosed){
	
	    $"Factory null or closed, creating a new one".Dump();
	
		var factorySettings = new MessagingFactorySettings{
			TokenProvider = credentials,
			TransportType = TransportType.NetMessaging
	    };
			
		factory = await MessagingFactory.CreateAsync(serviceBusUri, factorySettings).ConfigureAwait(false);
	}
}
static async Task<MessageReceiver> EnsureActiveReceiver(MessageReceiver receiver, string queuename, TokenProvider credentials, Uri serviceBusUri){
		if(receiver.IsClosed){
			receiver = await CreateReceiver(queuename, credentials, serviceBusUri).ConfigureAwait(false);
		}
		return receiver;
}
static async Task<MessageSender> EnsureActiveSender(MessageSender sender, string queuename, TokenProvider credentials, Uri serviceBusUri){
		if(sender.IsClosed)
		{
			sender = await CreateSender(queuename, credentials, serviceBusUri).ConfigureAwait(false);
		}
		return sender;
}
static async Task<MessageReceiver> CreateReceiver(string queuename, TokenProvider credentials, Uri serviceBusUri ){
	await EnsureConnection(credentials, serviceBusUri);

	$"Creating receiver".Dump();

	return await factory.CreateMessageReceiverAsync(queuename, ReceiveMode.PeekLock).ConfigureAwait(false);
}

static async Task<MessageSender> CreateSender(string queuename, TokenProvider credentials, Uri serviceBusUri){
	await EnsureConnection(credentials, serviceBusUri);

	$"Creating sender".Dump();

	return await factory.CreateMessageSenderAsync(queuename).ConfigureAwait(false);
}

static async Task EnsureQueueExists(string queuename, TokenProvider credentials, Uri serviceBusUri){
	NamespaceManager namespaceManager = new NamespaceManager(serviceBusUri, credentials);
	
	var queueDescription = new QueueDescription(queuename);
	queueDescription.EnablePartitioning = true;

	try
	{
		if (await namespaceManager.QueueExistsAsync(queueDescription.Path).ConfigureAwait(false))
		{
			await namespaceManager.DeleteQueueAsync(queueDescription.Path).ConfigureAwait(false);
			$"Queue exists, clearing.".Dump();
		}
		
		await namespaceManager.CreateQueueAsync(queueDescription).ConfigureAwait(false);
		$"Queue created".Dump();
	}
	catch(UnauthorizedAccessException){
		$"Not allowed to perform manage operation, should abort here".Dump();
		//todo: implement abort
	}
	catch (TimeoutException){
		$"Timeout due to connectivity problems, queue may or may not exist, can retry.".Dump();
		//todo: implement retry
	}
	catch(MessagingEntityAlreadyExistsException){
		$"Queue already exists, there is a race condition with other instance, this is ok.".Dump();		
	}	
	catch (MessagingException ex)
    {
        if (ex.IsTransient)
        {
            $"Transient exception occured in the broker, can retry after backoff".Dump();
			//todo: implement retry with back off
        }
		else{
			$"Something really bad happend in the broker, must abort".Dump();
			//todo: implement abort
		}		
    }
}