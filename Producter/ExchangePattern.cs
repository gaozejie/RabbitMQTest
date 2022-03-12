using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Producter
{
    internal class ExchangePattern
    {
        /// <summary>
        /// 发布订阅模式(fanout)
        /// </summary>
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

                    var i = 0;
                    while (i <= 10)
                    {
                        var msg = $"fanout消息内容:{i}";
                        //消息内容
                        byte[] body = Encoding.UTF8.GetBytes(msg);
                        //发送消息
                        // routingKey：广播模式没有routingKey，传了也会忽略
                        channel.BasicPublish(exchangeName, routingKey: "", basicProperties: null, body: body);
                        Console.WriteLine($"成功发送fanout消息:{msg}");
                        i++;
                    }

                }
            }
        }

        /// <summary>
        /// 路由模式(direct)
        /// </summary>
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

                    var i = 0;
                    while (i <= 10)
                    {
                        var msg = $"direct消息内容:{i}";
                        //消息内容
                        byte[] body = Encoding.UTF8.GetBytes(msg);
                        //发送消息
                        // routingKey：
                        channel.BasicPublish(exchangeName, routingKey: routingName1, basicProperties: null, body: body);
                        channel.BasicPublish(exchangeName, routingKey: routingName2, basicProperties: null, body: body);
                        channel.BasicPublish(exchangeName, routingKey: routingName3, basicProperties: null, body: body);
                        Console.WriteLine($"成功发送direct消息:{msg}");
                        i++;
                    }

                }
            }
        }

        /// <summary>
        /// 通配符模式(topic)
        /// </summary>
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

                    var i = 0;
                    while (i <= 10)
                    {
                        var msg = $"topic消息内容:{i}";
                        //消息内容
                        byte[] redBody = Encoding.UTF8.GetBytes(msg + "-red");
                        byte[] blueBody = Encoding.UTF8.GetBytes(msg + "-blue");
                        //发送消息
                        // topics模式的routingKey必须是一个英文“.”分隔的字符串，可以存在两种特殊字符"*"与“#”，“*”用于匹配一个单词，“#”用于匹配多个单词（可以是零个）。
                        channel.BasicPublish(exchangeName, routingKey: "red.ABC", basicProperties: null, body: redBody);
                        channel.BasicPublish(exchangeName, routingKey: "blue.BCD", basicProperties: null, body: blueBody);
                        Console.WriteLine($"成功发送topic消息:{msg}");
                        i++;
                    }

                }
            }
        }

    }
}
