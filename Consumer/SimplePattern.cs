using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Consumer
{
    public class SimplePattern
    {
        /// <summary>
        /// 简单模式
        /// Worker模式和简单模式的区别为Worker模式有多个消费者，即启动多个消费者即可。
        /// </summary>
        public static void SimplePatternRun()
        {
            using (IConnection conn = ConnFactoty.GetConnectionFactory().CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    // 声明一个队列
                    // durable - 是否持久化 
                    // autoDelete - 是否自动删除，false时需要消费者手动确认
                    var queueName = "simple-queue";
                    channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                    //创建消费者对象
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body.ToArray();//接收到的消息
                        Console.WriteLine($"接收到信息为:{Encoding.UTF8.GetString(message)}");

                        // 手动确认消息
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
                    Console.ReadLine();
                }
            }
        }
    }
}
