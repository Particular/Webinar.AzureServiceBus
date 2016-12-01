<Query Kind="Program">
  <NuGetReference>WindowsAzure.ServiceBus</NuGetReference>
  <Namespace>Microsoft.ServiceBus</Namespace>
  <Namespace>Microsoft.ServiceBus.Messaging</Namespace>
  <Namespace>System.Diagnostics</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
</Query>

void Main()
{
	MainAsync().GetAwaiter().GetResult();
}

static MessagingFactory factory;

static async Task MainAsync()
{
	var manageKeyName = Environment.GetEnvironmentVariable("AzureServiceBus.ManageKeyName");
	var manageKey = Environment.GetEnvironmentVariable("AzureServiceBus.ManageKey");
	var ns = Environment.GetEnvironmentVariable("AzureServiceBus.Namespace");
	
	ServiceBusEnvironment.SystemConnectivity.Mode = ConnectivityMode.Tcp;
	
	TokenProvider credentials = TokenProvider.CreateSharedAccessSignatureTokenProvider(manageKeyName, manageKey);
	Uri serviceBusUri = ServiceBusEnvironment.CreateServiceUri("sb", ns, string.Empty);	
	
	await EnsureQueueExists("myQueue", credentials, serviceBusUri);	
    
	var factoryPerSender = true;
	var numberOfSenders = 16;
	var senders = new List<MessageSender>();
	for(var i= 0; i < numberOfSenders; i++)
	{
		var sender = await CreateSender("myQueue", credentials, serviceBusUri, factoryPerSender).ConfigureAwait(false);
		senders.Add(sender);
	}	
	
	var messagesToSend = 100000;
	var stopwatch = new System.Diagnostics.Stopwatch();
	stopwatch.Start();
	
	var tasks = new List<Task>();
	for(var i = 0; i < messagesToSend; i++)
	{
		var senderIndex = i % senders.Count;
		var sender = senders[senderIndex];
		tasks.Add(sender.SendAsync(new BrokeredMessage("some data1")));		
	}
	
	await Task.WhenAll(tasks).ConfigureAwait(false);
	stopwatch.Stop();
	$"Sending messages took {stopwatch.ElapsedMilliseconds} ms:".Dump();
    
	$"Stopping".Dump();
}

static async Task EnsureConnection(TokenProvider credentials, Uri serviceBusUri){
    if(factory == null || factory.IsClosed){
	
	    $"Factory null or closed, creating a new one".Dump();
	
		var factorySettings = new MessagingFactorySettings{
			TokenProvider = credentials,
	        TransportType = TransportType.NetMessaging,
			NetMessagingTransportSettings = new NetMessagingTransportSettings
            {
                BatchFlushInterval = TimeSpan.FromMilliseconds(20)
            }
	    };
			
		factory = await MessagingFactory.CreateAsync(serviceBusUri, factorySettings).ConfigureAwait(false);
	}
}

static async Task<MessageSender> EnsureActiveSender(MessageSender sender, string queuename, TokenProvider credentials, Uri serviceBusUri, bool newFactory){
		if(sender.IsClosed)
		{
			sender = await CreateSender("myQueue", credentials, serviceBusUri, newFactory).ConfigureAwait(false);
		}
		return sender;
}
static async Task<MessageReceiver> EnsureActiveReceiver(MessageReceiver receiver, string queuename, TokenProvider credentials, Uri serviceBusUri, ReceiveMode receiveMode, bool newFactory){
		if(receiver.IsClosed){
			receiver = await CreateReceiver("myQueue", credentials, serviceBusUri, receiveMode, newFactory).ConfigureAwait(false);
		}
		return receiver;
}

static async Task<MessageReceiver> CreateReceiver(string queuename, TokenProvider credentials, Uri serviceBusUri, ReceiveMode receiveMode, bool newFactory){
	$"Creating receiver".Dump();
	
	if(!newFactory){
		await EnsureConnection(credentials, serviceBusUri);
		return await factory.CreateMessageReceiverAsync(queuename, receiveMode).ConfigureAwait(false);
	}
	else{
		var factorySettings = new MessagingFactorySettings{
			TokenProvider = credentials,
	        TransportType = TransportType.NetMessaging
	    };
			
		var f = await MessagingFactory.CreateAsync(serviceBusUri, factorySettings).ConfigureAwait(false);
		return await f.CreateMessageReceiverAsync(queuename, receiveMode).ConfigureAwait(false);
	}
}

static async Task<MessageSender> CreateSender(string queuename, TokenProvider credentials, Uri serviceBusUri, bool newFactory){
	$"Creating sender".Dump();
	
	if(!newFactory){	
		await EnsureConnection(credentials, serviceBusUri);
		return await factory.CreateMessageSenderAsync(queuename).ConfigureAwait(false);
	}
	else{
		var factorySettings = new MessagingFactorySettings{
			TokenProvider = credentials,
	        TransportType = TransportType.NetMessaging,
			NetMessagingTransportSettings = new NetMessagingTransportSettings
            {
                BatchFlushInterval = TimeSpan.FromMilliseconds(20)
            }
	    };
			
		var f = await MessagingFactory.CreateAsync(serviceBusUri, factorySettings).ConfigureAwait(false);
		return await f.CreateMessageSenderAsync(queuename).ConfigureAwait(false);
	}
}

static async Task EnsureQueueExists(string queuename, TokenProvider credentials, Uri serviceBusUri){
	NamespaceManager namespaceManager = new NamespaceManager(serviceBusUri, credentials);
	
	var queueDescription = new QueueDescription(queuename);
	queueDescription.EnablePartitioning = true;
	
	try{
		if(!await namespaceManager.QueueExistsAsync(queueDescription.Path)){
			await namespaceManager.CreateQueueAsync(queueDescription).ConfigureAwait(false);
			$"Queue created".Dump();
		}
		else{
			$"Queue already exists".Dump();
		}		
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