namespace ServiceBusPeekMessagesBug;

public class Program
{
    /// <summary>
    ///     Full rights! / RootManageSharedAccessKey
    /// </summary>
    private static readonly string ConnectionString =
        "";

    private static readonly string TopicName = Guid.NewGuid().ToString();
    private static readonly string SubscriptionName = Guid.NewGuid().ToString();

    /// <summary>
    /// This is "FIX", i'll just recreate receiver each time it want to retrieve messages.
    /// If true, will spam "Received already received message..." on console 
    /// </summary>
    private const bool WithFix = false;

    public static async Task Main(string[] args)
    {
        Console.WriteLine($"Starting..., Topic: {TopicName}");
        var sbSender = new SBSender(ConnectionString, TopicName, SubscriptionName);
        await sbSender.Init();
        await sbSender.Send10TestMessages();
        Console.WriteLine("10 messages sent, waiting 10 sec to retrieve them from other class...");
        using var sbRec = new SBReceiver(ConnectionString, TopicName, SubscriptionName, WithFix);
        Thread.Sleep(TimeSpan.FromSeconds(10));
        Console.WriteLine("Sending another 10 messages sent, waiting to retrieve them from other class...");
        await sbSender.Send10TestMessages();
        Thread.Sleep(TimeSpan.FromDays(1));
    }
}