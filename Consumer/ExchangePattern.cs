using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Consumer
{
    internal class ExchangePattern
    {
        public static void ExchangePatternFanout()
        {
            using (IConnection con = ConnFactoty.GetConnectionFactory().CreateConnection())//创建连接对象
            {
                using (IModel channel = con.CreateModel())//创建连接会话对象
                {
                    var exchangeName = "fanout-exchange";
                    var queueName1 = $"{exchangeName}-quene1";
                    var queueName2 = $"{exchangeName}-quene2";
                    var queueName3 = $"{exchangeName}-quene3";
                    // 声明一个交换机
                    // durable - 是否持久化 
                    // autoDelete - 是否自动删除
                    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Fanout, durable: false, autoDelete: false);
                    // 声明多个队列
                    channel.QueueDeclare(queue: queueName1, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queue: queueName2, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queue: queueName3, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    // 将队列与交换机进行绑定
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: "");
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: "");
                    channel.QueueBind(queue: queueName3, exchange: exchangeName, routingKey: "");

                    //创建队列1消费者
                    var consumer1 = new EventingBasicConsumer(channel);
                    consumer1.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body.ToArray();//接收到的消息
                        Console.WriteLine($"队列1接收到信息为:DeliveryTag:{ea.DeliveryTag},{Encoding.UTF8.GetString(message)}");

                        // 手动确认消息
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: queueName1, autoAck: false, consumer: consumer1);

                    // 队列2消费者
                    var consumer2 = new EventingBasicConsumer(channel);
                    consumer2.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body.ToArray();//接收到的消息
                        Console.WriteLine($"队列2接收到信息为:DeliveryTag:{ea.DeliveryTag},{Encoding.UTF8.GetString(message)}");

                        // 手动确认消息
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: queueName2, autoAck: false, consumer: consumer2);

                    // 队列3消费者
                    var consumer3 = new EventingBasicConsumer(channel);
                    consumer3.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body.ToArray();//接收到的消息
                        Console.WriteLine($"队列3接收到信息为:DeliveryTag:{ea.DeliveryTag},{Encoding.UTF8.GetString(message)}");

                        // 手动确认消息
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: queueName3, autoAck: false, consumer: consumer3);


                    Console.ReadLine();
                }
            }
        }

        public static void ExchangePatternDirect()
        {
            using (IConnection con = ConnFactoty.GetConnectionFactory().CreateConnection())//创建连接对象
            {
                using (IModel channel = con.CreateModel())//创建连接会话对象
                {
                    var exchangeName = "direct-exchange";
                    var queueName1 = $"{exchangeName}-direct-quene1";
                    var queueName2 = $"{exchangeName}-direct-quene2";
                    var queueName3 = $"{exchangeName}-direct-quene3";

                    var routingName1 = $"{queueName1}-routing";
                    var routingName2 = $"{queueName2}-routing";
                    var routingName3 = $"{queueName3}-routing";

                    // 声明一个交换机
                    // durable - 是否持久化 
                    // autoDelete - 是否自动删除
                    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Direct, durable: false, autoDelete: false);
                    // 声明多个队列
                    channel.QueueDeclare(queue: queueName1, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queue: queueName2, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queue: queueName3, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    // 将队列与交换机进行绑定
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: routingName1);
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: routingName2);
                    channel.QueueBind(queue: queueName3, exchange: exchangeName, routingKey: routingName3);

                    //创建队列1消费者
                    var consumer1 = new EventingBasicConsumer(channel);
                    consumer1.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body.ToArray();//接收到的消息
                        Console.WriteLine($"队列1接收到信息为:DeliveryTag:{ea.DeliveryTag},{Encoding.UTF8.GetString(message)}");

                        // 手动确认消息
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: queueName1, autoAck: false, consumer: consumer1);

                    // 队列2消费者
                    var consumer2 = new EventingBasicConsumer(channel);
                    consumer2.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body.ToArray();//接收到的消息
                        Console.WriteLine($"队列2接收到信息为:DeliveryTag:{ea.DeliveryTag},{Encoding.UTF8.GetString(message)}");

                        // 手动确认消息
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: queueName2, autoAck: false, consumer: consumer2);

                    // 队列3消费者
                    var consumer3 = new EventingBasicConsumer(channel);
                    consumer3.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body.ToArray();//接收到的消息
                        Console.WriteLine($"队列3接收到信息为:DeliveryTag:{ea.DeliveryTag},{Encoding.UTF8.GetString(message)}");

                        // 手动确认消息
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: queueName3, autoAck: false, consumer: consumer3);


                    Console.ReadLine();
                }
            }
        }

        public static void ExchangePatternTopic()
        {
            using (IConnection con = ConnFactoty.GetConnectionFactory().CreateConnection())//创建连接对象
            {
                using (IModel channel = con.CreateModel())//创建连接会话对象
                {
                    var exchangeName = "topic-exchange";
                    var queueName1 = $"{exchangeName}-topic-quene1";
                    var queueName2 = $"{exchangeName}-topic-quene2";
                    var queueName3 = $"{exchangeName}-topic-quene3";

                    var routingName1 = $"red.*";
                    var routingName2 = $"blue.*";

                    // 声明一个交换机
                    // durable - 是否持久化 
                    // autoDelete - 是否自动删除
                    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Topic, durable: false, autoDelete: false);
                    // 声明多个队列
                    channel.QueueDeclare(queue: queueName1, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queue: queueName2, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queue: queueName3, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    // 将队列与交换机进行绑定
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: routingName1);
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: routingName2);
                    channel.QueueBind(queue: queueName3, exchange: exchangeName, routingKey: routingName2);

                    //创建队列1消费者
                    var consumer1 = new EventingBasicConsumer(channel);
                    consumer1.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body.ToArray();//接收到的消息
                        Console.WriteLine($"队列1接收到信息为:DeliveryTag:{ea.DeliveryTag},{Encoding.UTF8.GetString(message)}");

                        // 手动确认消息
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: queueName1, autoAck: false, consumer: consumer1);

                    // 队列2消费者
                    var consumer2 = new EventingBasicConsumer(channel);
                    consumer2.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body.ToArray();//接收到的消息
                        Console.WriteLine($"队列2接收到信息为:DeliveryTag:{ea.DeliveryTag},{Encoding.UTF8.GetString(message)}");

                        // 手动确认消息
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: queueName2, autoAck: false, consumer: consumer2);

                    // 队列3消费者
                    var consumer3 = new EventingBasicConsumer(channel);
                    consumer3.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body.ToArray();//接收到的消息
                        Console.WriteLine($"队列3接收到信息为:DeliveryTag:{ea.DeliveryTag},{Encoding.UTF8.GetString(message)}");

                        // 手动确认消息
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: queueName3, autoAck: false, consumer: consumer3);


                    Console.ReadLine();
                }
            }
        }

    }
}
