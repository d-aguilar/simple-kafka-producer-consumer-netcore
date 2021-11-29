using Confluent.Kafka;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Producer.Services
{
    public class ProducerKafkaService
    {
        public async Task Produce<TValue> (string topic, string key, TValue message)
        {
            var producer = new ProducerBuilder<string, TValue>(new ProducerConfig() { BootstrapServers = "localhost:9092" }).Build();

            var result = await producer.ProduceAsync(topic, new Message<string, TValue>() { Key = key, Value = message });

            Log.Information("Delivery success - partition: {partiton} / Offset: {offset}", result.Partition, result.Offset);

        }
    }
}
