using CosmosDistributedLock.Services;

namespace Cosmos_Patterns_GlobalLock
{
    public record ConsoleMessage(
        string Message,
        ConsoleColor Color
    );
    // Delegate that defines the signature for the callback method.
    public delegate void PostMessageCallback(ConsoleMessage msg);

    public class LockTest
    {
        public DistributedLockService dls;

        private readonly string lockName;

        private int lockDuration;
               
        public volatile bool isActive = true;

        private ConsoleColor color;

        string threadName;

        PostMessageCallback postMessage;

        public LockTest(DistributedLockService dls, string lockName, int lockDuration, string threadName, PostMessageCallback postMessage , ConsoleColor color)
        {
            this.dls = dls;
            this.lockName = lockName;
            this.lockDuration = lockDuration;
            this.threadName = threadName;
            this.color = color;
            this.postMessage = postMessage;
        }


        public async void StartThread()
        {
            int prevFenceToken=0;
                       
            
            //postMessage(new ConsoleMessage( $"{mutex.Name}: Says Hello", this.color));

            postMessage(new ConsoleMessage($"{threadName}: Says Hello", this.color));

            while (this.isActive)
            {                
                
            }
        }

        private async Task DoWork(string threadName, string lockName)
        {
            //wait some random time
            Random r = new Random();
            int delay = r.Next(500, 50_000);
            postMessage(new ConsoleMessage("DO WORK", this.color));
            await Task.Delay(delay);
            postMessage(new ConsoleMessage("FINISHED", this.color));
        }
    }

}
