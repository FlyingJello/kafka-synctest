using System;
using System.Text.Json;
using Confluent.Kafka;

namespace Userservice
{
    class Program
    {
        static void Main()
        {
            var consumerConfig = new ConsumerConfig { BootstrapServers = "kafka:9092", GroupId = "userservice" };
            var producerConfig = new ProducerConfig { BootstrapServers = "kafka:9092" };

            using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

            consumer.Subscribe("GetUser");

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume();
                    if (consumeResult.Key == null)
                    {
                        continue;
                    }

                    var key = consumeResult.Key.Split(':');
                    var user = new User {Id = Guid.Parse(consumeResult.Value), Money = 68 + 1, Name = "Robert"};

                    var message = new Message<string, string> { Key = consumeResult.Key, Value = JsonSerializer.Serialize(user) };
                    producer.Produce(new TopicPartition(key[0], int.Parse(key[1])), message);
                }
            }
            catch (Exception)
            {
                consumer.Close();
            }
        }
    }
}
