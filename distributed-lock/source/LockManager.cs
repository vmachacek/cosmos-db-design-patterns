using CosmosDistributedLock.Services;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using System.Net;
using System.Threading.Channels;
using Spectre.Console;
using Color = System.Drawing.Color;

namespace Cosmos_Patterns_GlobalLock
{
    /// <summary>
    /// This represents a lock in the Cosmos DB.  Also used as the target of the lock.
    /// </summary>
    public class DistributedLock
    {
        [JsonProperty("id")]
        public string LockName { get; set; } //Lock Name

        [JsonProperty("_etag")]
        public string ETag { get; set; } //Can I update or has someone else taken the lock

        [JsonProperty("_ts")]
        public long Ts { get; set; }

        public string OwnerId { get; set; } //ownerId, ClientId

        public long FenceToken { get; set; } //Incrementing token
    }

    public class Lease
    {
        [JsonProperty("id")]
        public string OwnerId { get; set; } //ownerId, clientId

        [JsonProperty("ttl")]
        public int LeaseDuration { get; set; } //leaseDuration in seconds

        [JsonProperty("_ts")]
        public long Ts { get; set; }
    }

    public class LockManager : IDisposable
    {
        DistributedLockService dls;

        string lockName;
        public string ownerId;
        public string Name;

        public string leaseOwnerId;

        private Task renewalTask;

        private Channel<int> latestFenceToken = Channel.CreateUnbounded<int>();

        private CancellationTokenSource renewalTokenSource = new CancellationTokenSource();

        /// <summary>
        /// This creates a container that has the TTL feature enabled.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="lockDbName"></param>
        /// <param name="lockContainerName"></param>
        /// <param name="lockName"></param>
        /// <param name="refreshIntervalS"></param>
        public LockManager(DistributedLockService dls, string lockName, string threadName)
        {
            this.dls = dls;

            this.lockName = lockName;

            this.ownerId = Guid.NewGuid().ToString();

            this.Name = threadName;
        }

        /// <summary>
        /// Simple static constructor
        /// </summary>
        /// <param name="client"></param>
        /// <param name="lockDb"></param>
        /// <param name="lockContainer"></param>
        /// <param name="lockName"></param>
        /// <returns></returns>
        static public async Task<LockManager> CreateLockAsync(DistributedLockService dls, string lockName, string threadName)
        {
            return new LockManager(dls, lockName, threadName);
        }

        static public async Task<LockManager> MakeSureRunsOnOneMachine(Func<Task> Work, DistributedLockService dls, string lockName)
        {
            var prevFenceToken = 0;
            var lockDuration = 20;
            var threadName = "";

            while (true)
            {
                using (var mutex = await LockManager.CreateLockAsync(dls, lockName, threadName))
                {
                    LeaseRequestStatus reqStatus = await mutex.AcquireLeaseAsync(lockDuration, prevFenceToken);

                    var latestFenceToken = reqStatus.fenceToken;
                    var newOwner = reqStatus.currentOwner;

                    Console.WriteLine($"{mutex.Name}: Sees lock [{lockName}] having token {latestFenceToken}, attempting to acquire lease.");

                    if (latestFenceToken <= prevFenceToken)
                    {
                        new Exception($"[{DateTime.Now}]: {mutex.Name} : Violation: {latestFenceToken} was acquired after {prevFenceToken} was seen");
                    }

                    if (latestFenceToken > 0 && newOwner == mutex.ownerId)
                    {
                        Console.WriteLine($"{mutex.Name}: Attempt to aquire lease on lock [{lockName}] using token {latestFenceToken}  ==> SUCESS");

                        var tokenSource = new CancellationTokenSource();
                        CancellationToken ct = tokenSource.Token;

                        Task.Run(async () =>
                        {
                            while (!ct.IsCancellationRequested)
                            {
                                var ms = lockDuration * 1000;

                                await Task.Delay(ms / 2, ct);
                                await mutex.AcquireLeaseAsync(lockDuration, latestFenceToken);
                                var bigText1 = new FigletText($"Renew lease")
                                    .Centered()
                                    .Color(Spectre.Console.Color.Green);

                                // Render the big text to the console
                                AnsiConsole.Write(bigText1);
                            }

                            var bigText = new FigletText($"Renew stopped")
                                .Centered()
                                .Color(Spectre.Console.Color.Yellow);

                            // Render the big text to the console
                            AnsiConsole.Write(bigText);
                        }, tokenSource.Token);

                        await Work();
                        await tokenSource.CancelAsync();

                        Console.WriteLine($"{mutex.Name}: Releasing the lock [{lockName}].");
                        await mutex.ReleaseLeaseAsync();
                    }
                    else
                    {
                        Console.WriteLine($"{mutex.Name}: Attempt to acquire lease on lock [{lockName}] using token {latestFenceToken}  ==> FAILED");
                    }
                }

                await Task.Delay(1000);
            }
        }

        /// <summary>
        /// This function will check for a lease object (if it exists).  If it does, it checks to see if the current client has the lease.  If the lease is expired, it will automatically be deleted by Cosmos DB via the TTL property.
        /// </summary>
        /// <param name="leaseDurationS"></param>
        /// <returns></returns>
        public async Task<LeaseRequestStatus> AcquireLeaseAsync(int leaseDuration, long existingFenceToken)
        {
            try
            {
                var reqStatus = await dls.AcquireLeaseAsync(lockName, ownerId, leaseDuration, existingFenceToken);
                leaseOwnerId = reqStatus.currentOwner;
                return reqStatus;
            }
            catch (Exception e)
            {
                throw;
            }
        }

        public async Task<bool> ReleaseLeaseAsync()
        {
            try
            {
                if (leaseOwnerId == ownerId)
                    await dls.ReleaseLeaseAsync(ownerId);

                return true;
            }
            catch (CosmosException e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }

                throw;
            }
        }

        /// <summary>
        /// This function will check to see if the current token is valid.  It is possible that the lease has expired and a new lease needs to be created.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<bool> HasLeaseAsync(long token)
        {
            try
            {
                return await dls.ValidateLeaseAsync(lockName, ownerId, token);
            }
            catch (CosmosException e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }

                throw;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Task<bool> releaseTask = ReleaseLeaseAsync();
            }
        }

        public async Task StartLeaseRenewal(int leaseDuration)
        {
            while (!renewalTokenSource.IsCancellationRequested)
            {
                Console.WriteLine("Renewal", ConsoleColor.Yellow);

                await Task.Delay(leaseDuration * 1000 - 5000, renewalTokenSource.Token);
                await dls.AcquireLeaseAsync(lockName, ownerId, leaseDuration, 0);
            }
        }

        public async Task StopLeaseRenewal()
        {
            await renewalTokenSource.CancelAsync();
        }
    }
}