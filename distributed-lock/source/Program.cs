using CosmosDistributedLock.Services;
using Microsoft.Extensions.Configuration;
using Spectre.Console;

namespace Cosmos_Patterns_GlobalLock
{
    internal class Program
    {
        static DistributedLockService dls;

        static DateTime dtLog;
        private static object _lock;

        static PostMessageCallback postMessage;

        private static ConsoleColor color = ConsoleColor.Yellow;

        static async Task Main(string[] args)
        {
            postMessage = (s) => Console.WriteLine(s.Message, s.Color);

            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"appsettings.development.json", optional: true);

            var config = configuration.Build();

            dls = new DistributedLockService(config);
            await dls.InitDatabaseAsync();

            var lockName = "RESTORE-DB";
            
            await LockManager.MakeSureRunsOnOneMachine(async () => await DoWork(), dls, lockName);
        }

        private static async Task DoWork()
        {
            foreach (var VARIABLE in Enumerable.Range(0, 20))
            {
                var bigText = new FigletText($"Working #{VARIABLE}")
                    .Centered()
                    .Color(Color.Red);

                // Render the big text to the console
                AnsiConsole.Write(bigText);

                await Task.Delay(3_000);
            }
        }
    }
}