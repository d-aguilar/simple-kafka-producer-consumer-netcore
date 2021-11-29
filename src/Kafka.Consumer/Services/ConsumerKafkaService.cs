using Confluent.Kafka;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Consumer.Services
{
    public class ConsumerKafkaService
    {
        private readonly string _bootstrapServer;
        private readonly string _groupId;

        public ConsumerKafkaService(string bootstrapServer, string groupId)
        {
            _bootstrapServer = bootstrapServer;
            _groupId = groupId;
        }

        public void Consume<TValue>(string topic, CancellationToken cts, Func<string, TValue, bool> callback = null)
        {
            using (var consumer = CreateConsumer<TValue>())
            {
                consumer.Subscribe(topic);
                Log.Information("Subscribe to topic '{topic}'", topic);
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        Log.Information("Waiting for new messages...");
                        var result = consumer.Consume(cts);

                        Log.Information("Message received topic {topic} partition {partition} and offset {offset}", result.Topic, result.Partition, result.Offset);
                        Log.Information("Message key {key} / content {content}", result.Message.Key, result.Message.Value);
                        var success = ExecuteCallback(callback, result);

                        if (success)
                            consumer.Commit(result);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
        }

        private static bool ExecuteCallback<TValue>(Func<string, TValue, bool> callback, ConsumeResult<string, TValue> result)
        {
            var success = true;

            if (callback != null)
                success = callback(result.Message.Key, result.Message.Value);

            return success;
        }

        private IConsumer<string, TValue> CreateConsumer<TValue>()
        {
            var consumerBuilder = new ConsumerBuilder<string, TValue>(CreateConsumerConfig());

            return consumerBuilder.Build();
        }

        private ConsumerConfig CreateConsumerConfig()
        {
            var consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = _bootstrapServer,
                GroupId = _groupId,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            return consumerConfig;
        }
    }
}
