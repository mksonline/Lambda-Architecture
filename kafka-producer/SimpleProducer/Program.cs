using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System.Threading;

namespace SimpleProducer
{
    internal class Program
    {
        public static void Main()
        {
            const string brokerList = "localhost:9092";
            const string topicName = "sales";
            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };
            List<string> lines = System.IO.File.ReadAllLines(@"D:\Apache\Logs\spark\input\products.csv").ToList<string>();

            using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
            {
                var random = new Random(Guid.NewGuid().GetHashCode());
                Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");
                var counter = 0;
                while (lines.Count>0)
                {
                    var rec = random.Next(0, lines.Count - 1);
                    Console.WriteLine("Sending record " + rec + " : " + lines[rec]);
                    var deliveryReport = producer.ProduceAsync(topicName, null, lines[rec]);
                    lines.RemoveAt(rec);

                    if (lines.Count == 0){
                        Console.WriteLine("No items remaining ... exiting with status 0.");
                        break;
                    }

                    counter++;
                    if(counter>=100 && (counter % 100 == 0)){
                        Console.WriteLine("Sleeping for 1 sec. counter = " + counter );
                        Thread.Sleep(1000);
                    }
                    if (counter > 99999) break;
                }

                // Tasks are not synchronous ... so it's possible they may still be in progress here.
                producer.Flush();
            }

            Console.ReadLine();
        }
    }
}
