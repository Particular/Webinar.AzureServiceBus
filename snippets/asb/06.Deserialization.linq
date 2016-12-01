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
	var manageKeyName = Environment.GetEnvironmentVariable("AzureServiceBus.ManageKeyName");
	var manageKey = Environment.GetEnvironmentVariable("AzureServiceBus.ManageKey");
	var ns = Environment.GetEnvironmentVariable("AzureServiceBus.Namespace");
	
	ServiceBusEnvironment.SystemConnectivity.Mode = ConnectivityMode.Tcp;
	
	TokenProvider credentials = TokenProvider.CreateSharedAccessSignatureTokenProvider(manageKeyName, manageKey);
	Uri serviceBusUri = ServiceBusEnvironment.CreateServiceUri("sb", ns, string.Empty);	
	
	await EnsureQueueExists("myQueue", credentials, serviceBusUri);	

	var receiveMode = ReceiveMode.ReceiveAndDelete;
	var receiver = await CreateReceiver("myQueue", credentials, serviceBusUri, receiveMode).ConfigureAwait(false);
    var sender = await CreateSender("myQueue", credentials, serviceBusUri).ConfigureAwait(false);
    
	// method 1
	await sender.SendAsync(new BrokeredMessage("some data1")).ConfigureAwait(false);
	
	// method 2
	var bytes = System.Text.Encoding.UTF8.GetBytes("some data2");
	await sender.SendAsync(new BrokeredMessage(bytes)).ConfigureAwait(false);
	
	// method 3
	bytes = System.Text.Encoding.UTF8.GetBytes("some data3");
	var stream = new MemoryStream(bytes);
	await sender.SendAsync(new BrokeredMessage(stream)).ConfigureAwait(false);
	

//	var x = "";
//	while(x != "x"){
//	
//	    receiver = await EnsureActiveReceiver(receiver, "myQueue", credentials, serviceBusUri, receiveMode).ConfigureAwait(false);
//		
//		var receivedMessage = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
//		
//		if(receivedMessage == null){
//			$"No message received".Dump();
//		}
//		else{		
//			try			
//			{
//				//method 1
////				var bodyString = receivedMessage.GetBody<string>();
//
//				//method 2
////				var receivedbytes = receivedMessage.GetBody<byte[]>();
//				//method 3
////				var receivedstream = receivedMessage.GetBody<Stream>();
////				var receivedbytes = new byte[receivedstream.Length];
////                receivedstream.Read(receivedbytes, 0, (int)receivedstream.Length);
//		
//				var bodyString = System.Text.Encoding.UTF8.GetString(receivedbytes);
//				$"Received message with body: {bodyString}".Dump();				
//			}
//			catch(Exception ex){
//				$"Exception handled: {ex.Message}".Dump();				
//			}
//		}
//		$"Press any key to retry or x to exit:".Dump();
//		x = Util.ReadLine();	
//		
//	}
	
	$"Stopping".Dump();
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

static async Task<MessageSender> EnsureActiveSender(MessageSender sender, string queuename, TokenProvider credentials, Uri serviceBusUri){
		if(sender.IsClosed)
		{
			sender = await CreateSender("myQueue", credentials, serviceBusUri).ConfigureAwait(false);
		}
		return sender;
}
static async Task<MessageReceiver> EnsureActiveReceiver(MessageReceiver receiver, string queuename, TokenProvider credentials, Uri serviceBusUri, ReceiveMode receiveMode){
		if(receiver.IsClosed){
			receiver = await CreateReceiver(queuename, credentials, serviceBusUri, receiveMode).ConfigureAwait(false);
		}
		return receiver;
}

static async Task<MessageReceiver> CreateReceiver(string queuename, TokenProvider credentials, Uri serviceBusUri, ReceiveMode receiveMode){
	await EnsureConnection(credentials, serviceBusUri);

	$"Creating receiver".Dump();

	return await factory.CreateMessageReceiverAsync(queuename, receiveMode).ConfigureAwait(false);
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