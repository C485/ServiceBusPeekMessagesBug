using Azure.Messaging.ServiceBus;

namespace ServiceBusPeekMessagesBug;

internal class SBReceiver : IDisposable
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly HashSet<string> _processedMessages;
    private readonly string _serviceBusConnectionString;
    private readonly string _subscriptionName;
    private readonly PeriodicTimer _timer;
    private readonly Task _timerTask;
    private readonly string _topicName;
    private readonly bool _withFix;
    private ServiceBusClient _client;
    private ServiceBusReceiver _serviceBusReceiver;

    public SBReceiver(string serviceBusConnectionString, string topicName, string subscriptionName, bool withFix)
    {
        _serviceBusConnectionString = serviceBusConnectionString;
        _topicName = topicName;
        _processedMessages = new HashSet<string>();
        _cancellationTokenSource = new CancellationTokenSource();
        _subscriptionName = subscriptionName;
        _withFix = withFix;
        _timer = new PeriodicTimer(TimeSpan.FromMilliseconds(1000));
        _timerTask = HandleTimerAsync();
    }

    public void Dispose()
    {
        _cancellationTokenSource?.Cancel();
        _timerTask?.Wait();
        _timerTask?.Dispose();
        _timer?.Dispose();
        _cancellationTokenSource?.Dispose();
    }

    private async Task HandleTimerAsync()
    {
        try
        {
            while (await _timer.WaitForNextTickAsync(_cancellationTokenSource.Token))
                try
                {
                    await GetMessages();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
        }
        catch (Exception exc)
        {
            Console.WriteLine(exc.ToString());
        }
    }

    private async Task GetMessages()
    {
        if (_client?.IsClosed != false) _client = new ServiceBusClient(_serviceBusConnectionString);

        if (_serviceBusReceiver?.IsClosed != false)
        {
            ServiceBusReceiverOptions options = new()
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                SubQueue = SubQueue.None,
                PrefetchCount = 110
            };
            _serviceBusReceiver = _client.CreateReceiver(_topicName, _subscriptionName, options);
        }

        try
        {
            while (true)
            {
                var messages =
                    await _serviceBusReceiver.PeekMessagesAsync(500);

                if (messages == null || messages.Count == 0)
                {
                    if (_withFix)
                    {
                        await _serviceBusReceiver.CloseAsync();
                        _serviceBusReceiver = null;
                    }

                    break;
                }

                foreach (var serviceBusReceivedMessage in messages)
                {
                    if (_processedMessages.Contains(serviceBusReceivedMessage.MessageId))
                    {
                        Console.WriteLine(
                            $"\t\tReceived already received message, ID: {serviceBusReceivedMessage.MessageId}");
                        continue;
                    }

                    _processedMessages.Add(serviceBusReceivedMessage.MessageId);
                    Console.WriteLine($"Received message with ID: {serviceBusReceivedMessage.MessageId}, total of {_processedMessages.Count}");
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
    }
}