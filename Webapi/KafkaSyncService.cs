using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Webapi
{
    public interface IKafkaSyncService
    {
        Task<string> Send(string topic, string data);
    }

    public class KafkaSyncService : IKafkaSyncService
    {
        private readonly string _returnTopicName;
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        private readonly List<int> _returnPartitions;
        private readonly List<int> _usedPartitions;

        private const string KafkaUrl = "kafka:9092";

        public KafkaSyncService(string returnTopicName, int partitionCount)
        {
            _returnTopicName = returnTopicName;
            _returnPartitions = new List<int>(Enumerable.Range(0, partitionCount - 1));
            _usedPartitions = new List<int>();
            _producerConfig = new ProducerConfig { BootstrapServers = KafkaUrl };
            _consumerConfig = new ConsumerConfig { BootstrapServers = KafkaUrl, GroupId = "webapi" };
        }

        private int ReserveNextReturnPartition()
        {
            if (!_returnPartitions.Except(_usedPartitions).Any())
            {
                throw new InvalidOperationException("No available return partition to use");
            }

            var nextPartition = _returnPartitions.Except(_usedPartitions).First();
            _usedPartitions.Add(nextPartition);
            return nextPartition;
        }

        public async Task<string> Send(string topic, string data)
        {
            using var producer = new ProducerBuilder<string, string>(_producerConfig).Build();
            using var consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build();

            try
            {
                var requestPartition = ReserveNextReturnPartition();
                var message = new Message<string, string> { Key = $"{_returnTopicName}:{requestPartition}", Value = data };
                await producer.ProduceAsync(topic, message);

                //consumer.Subscribe(_returnTopicName);
                consumer.Assign(new TopicPartition(_returnTopicName, requestPartition));
                var consumeResult = consumer.Consume();
                _usedPartitions.Remove(requestPartition);

                return consumeResult.Value;
            }
            catch (Exception e)
            {
                consumer.Close();
                Console.WriteLine($"Delivery failed: {e.Message}");
                throw;
            }
        }
    }
}
