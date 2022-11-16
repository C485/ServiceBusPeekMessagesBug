using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace ServiceBusPeekMessagesBug;

internal class SBSender
{
    private readonly string _connectionString;
    private readonly string _subscriptionName;
    private readonly string _topicName;
    private ServiceBusAdministrationClient _client;
    private ServiceBusClient _clientSender;
    private ServiceBusSender _serviceBusSender;

    public SBSender(string connectionString, string topicName, string subscriptionName)
    {
        _connectionString = connectionString;
        _topicName = topicName;
        _subscriptionName = subscriptionName;
    }

    public async Task Send10TestMessages()
    {
        foreach (var i in Enumerable.Range(0, 10))
            await _serviceBusSender.SendMessageAsync(new ServiceBusMessage(Guid.NewGuid().ToString())
            {
                SessionId = Guid.NewGuid().ToString()
            });
    }

    public async Task Init()
    {
        /*
         * This will create Topic with one subscription with default filter.
         * Look at params.
         */
        _client ??= new ServiceBusAdministrationClient(_connectionString);
        var testTopic = await GetTopic();
        if (testTopic == null)
            await _client.CreateTopicAsync(new CreateTopicOptions(_topicName)
            {
                EnableBatchedOperations = true,
                EnablePartitioning = true,
                RequiresDuplicateDetection = false,
                SupportOrdering = false,
                Status = EntityStatus.Active,
                AutoDeleteOnIdle = TimeSpan.FromMinutes(5) //Cleanup
            });

        var subscription = await GetSubscription();
        if (subscription == null)
            await _client.CreateSubscriptionAsync(new CreateSubscriptionOptions(_topicName, _subscriptionName)
            {
                EnableBatchedOperations = true,
                EnableDeadLetteringOnFilterEvaluationExceptions = false,
                DeadLetteringOnMessageExpiration = false,
                RequiresSession = true
            });

        /*
         * This will create simple sender.
         */

        if (_clientSender?.IsClosed != false) _clientSender = new ServiceBusClient(_connectionString);

        if (_serviceBusSender?.IsClosed != false) _serviceBusSender = _clientSender.CreateSender(_topicName);
    }

    private async Task<SubscriptionProperties> GetSubscription()
    {
        try
        {
            return (await _client.GetSubscriptionAsync(_topicName, _subscriptionName))?.Value;
        }
        catch (Exception e)
        {
            return null;
        }
    }

    private async Task<TopicProperties> GetTopic()
    {
        try
        {
            return (await _client.GetTopicAsync(_topicName))?.Value;
        }
        catch (Exception e)
        {
            return null;
        }
    }
}