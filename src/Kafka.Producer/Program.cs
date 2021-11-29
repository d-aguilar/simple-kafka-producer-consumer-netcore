using Kafka.Producer.Services;
using Serilog;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            ConfigureLog();

            var cts = ConfigureCancelationToken();

            while (!cts.IsCancellationRequested)
            {
                Console.WriteLine("Type a message:");
                var message = Console.ReadLine();

                var producer = new ProducerKafkaService();

                await producer.Produce("streaming.test", Guid.NewGuid().ToString(), message);
            }

            Console.WriteLine("");
            Console.ReadKey();
        }

        private static void ConfigureLog()
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console(theme: Serilog.Sinks.SystemConsole.Themes.AnsiConsoleTheme.Literate)
               .CreateLogger();

            Log.Logger = logger;
        }

        private static CancellationTokenSource ConfigureCancelationToken()
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            return cts;
        }

    }
}
