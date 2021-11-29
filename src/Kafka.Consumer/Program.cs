using Kafka.Consumer.Services;
using Serilog;
using System;
using System.Threading;

namespace Kafka.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            ConfigureLog();

            Log.Information("Press {key} to cancel", "ctrl + c");

            CancellationTokenSource cts = ConfigureCancelationToken();

            var consumerKafkaService = new ConsumerKafkaService("localhost:9092", "consumer-group-test-1");

            consumerKafkaService.Consume<string>("streaming.test", cts.Token, null);
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
