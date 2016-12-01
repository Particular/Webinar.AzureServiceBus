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
	await EnsureQueueExists("queueA", credentials, serviceBusUri).ConfigureAwait(false);
	await EnsureQueueExists("queueB", credentials, serviceBusUri).ConfigureAwait(false);

	var sender = await CreateSender("myQueue", credentials, serviceBusUri).ConfigureAwait(false);
	var receiver = await CreateReceiver("myQueue", credentials, serviceBusUri).ConfigureAwait(false);
	var senderA = await CreateSender("queueA", credentials, serviceBusUri, "myQueue").ConfigureAwait(false);
	var senderB = await CreateSender("queueB", credentials, serviceBusUri, "myQueue").ConfigureAwait(false);
	
	var original = new BrokeredMessage("original message");
	original.PartitionKey = "viapartitionkey";
	await sender.SendAsync(original).ConfigureAwait(false);

	if (receiver.IsClosed)
	{
		receiver = await CreateReceiver("myQueue", credentials, serviceBusUri).ConfigureAwait(false);
	}

	var receivedMessage = await receiver.ReceiveAsync().ConfigureAwait(false);
	var body = receivedMessage.GetBody<string>();

	$"Received message with body: {body}".Dump();

	try
	{
		using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
		{
			var message1 = new BrokeredMessage("message for QueueA");
			message1.ViaPartitionKey = "viapartitionkey";
			if (senderA.IsClosed)
			{
				senderA = await CreateSender("queueA", credentials, serviceBusUri, "myQueue").ConfigureAwait(false);
			}
			await senderA.SendAsync(message1).ConfigureAwait(false);
			$"Sending message to queueA via myQueue".Dump();

			var message2 = new BrokeredMessage("message for QueueB");
			message2.ViaPartitionKey = "viapartitionkey";
			if (senderB.IsClosed)
			{
				senderB = await CreateSender("queueB", credentials, serviceBusUri, "myQueue").ConfigureAwait(false);
			}
			await senderB.SendAsync(message2).ConfigureAwait(false);
			$"Sending message to queueB via myQueue".Dump();

			await receivedMessage.CompleteAsync().ConfigureAwait(false);
			transaction.Complete();
			"Completing transaction. Original message should be processed and QueueA & QueueB have one message each".Dump();
		}
	}
	catch (Exception ex)
	{
		$"Rolling back due to exception: {ex}".Dump();
		await receivedMessage.AbandonAsync().ConfigureAwait(false);
	}

	$"Closing factory, this will also close the senders & receivers".Dump();
	await factory.CloseAsync();
	
	$"Stopping".Dump();
}


public static class MessageSenderExtensions
{
	public static async Task SendWithRetriesAsync(this MessageSender sender, Func<MessageSender, Task> action, Func<MessageSender, Task> retryAction, TimeSpan wait, int maxRetryAttempts, int usedRetryAttempts = 0)
	{
		try
		{	        
			var actionToTake = usedRetryAttempts == 0 ? action : retryAction;
			await actionToTake(sender).ConfigureAwait(false);
		}
		catch (ServerBusyException)
		{
			if (usedRetryAttempts < maxRetryAttempts)
			{
				await Task.Delay(wait).ConfigureAwait(false);
				await sender.SendWithRetriesAsync(action, retryAction, wait, maxRetryAttempts, usedRetryAttempts + 1).ConfigureAwait(false);
			}
			throw;
		}
	}
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

static async Task<MessageReceiver> CreateReceiver(string queuename, TokenProvider credentials, Uri serviceBusUri){
	await EnsureConnection(credentials, serviceBusUri);

	$"Creating receiver".Dump();

	return await factory.CreateMessageReceiverAsync(queuename, ReceiveMode.PeekLock).ConfigureAwait(false);
}

static async Task<MessageSender> CreateSender(string queuename, TokenProvider credentials, Uri serviceBusUri, string sendVia = null){
	await EnsureConnection(credentials, serviceBusUri);

	$"Creating sender".Dump();
	
	if (sendVia == null)
	{
		return await factory.CreateMessageSenderAsync(queuename).ConfigureAwait(false);
	}

	return await factory.CreateMessageSenderAsync(queuename, sendVia).ConfigureAwait(false);
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