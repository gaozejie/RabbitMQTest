using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Producter
{
    /// <summary>
    /// 简单模式
    /// </summary>
    public class SimplePattern
    {
        public static void SimplePatternRun()
        {
            using (IConnection con = ConnFactoty.GetConnectionFactory().CreateConnection())//创建连接对象
            {
                using (IModel channel = con.CreateModel())//创建连接会话对象
                {
                    // 声明一个队列
                    // durable - 是否持久化 
                    // autoDelete - 是否自动删除
                    var queueName = "simple-queue";
                    channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                    var i = 0;
                    while (i <= 10)
                    {
                        var msg = $"消息内容:{i}";
                        //消息内容
                        byte[] body = Encoding.UTF8.GetBytes(msg);
                        //发送消息
                        // 一对一中routingKey必须和queueName一致
                        channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: null, body: body);
                        Console.WriteLine($"成功发送消息:{msg}");
                        i++;
                    }
                }
            }
        }
    }
}
