using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Consumer
{
    /// <summary>
    /// 死信队列/延时队列，在topic模式基础上添加死信队列
    /// </summary>
    internal class DLXQueue
    {
        public static void DLXQueueRun()
        {
            using (IConnection con = ConnFactoty.GetConnectionFactory().CreateConnection())//创建连接对象
            {
                using (IModel channel = con.CreateModel())//创建连接会话对象
                {
                    // DLX：死信队列交换机
                    var dlxExchangeName = "exchange_dlx";
                    channel.ExchangeDeclare(dlxExchangeName, ExchangeType.Direct, true, false, null);

                    #region 定义第一个死信队列，绑定该队列后，超时的消息将进入该队列，并设定路由key为死信队列绑定的路由key

                    //var dlxQueueName1 = "queue_dlx";
                    //var dlxRoutingKey1 = "dlx_routingkey";

                    ////参数设置
                    //Dictionary<string, object> dlxArgs = new Dictionary<string, object>();
                    //dlxArgs.Add("x-message-ttl", 10000);//TTL
                    //dlxArgs.Add("x-dead-letter-exchange", dlxExchangeName);//DLX
                    //dlxArgs.Add("x-dead-letter-routing-key", dlxRoutingKey1);//routingKey

                    ////死信队列绑定
                    //channel.QueueDeclare(dlxQueueName1, true, false, false, dlxArgs);
                    //channel.QueueBind(dlxQueueName1, dlxExchangeName, dlxRoutingKey1, null);

                    #endregion

                    #region 定义第二个死信队列，绑定该队列后，超时的消息将进入该队列，并设定路由key为死信队列绑定的路由key

                    var dlxQueueName2 = "queue_dlx1";
                    var dlxRoutingKey2 = "dlx_routingkey1";

                    //参数设置
                    Dictionary<string, object> dlxArgs1 = new Dictionary<string, object>();
                    dlxArgs1.Add("x-message-ttl", 10000);//TTL
                    dlxArgs1.Add("x-dead-letter-exchange", dlxExchangeName);//DLX
                    dlxArgs1.Add("x-dead-letter-routing-key", dlxRoutingKey2);//routingKey

                    // 注意死信队列的超时时间不能设置太短，否则超时后消息丢失
                    Dictionary<string, object> dlxArgs2 = new Dictionary<string, object>();
                    dlxArgs2.Add("x-message-ttl", 10000000);//TTL
                    dlxArgs2.Add("x-dead-letter-exchange", dlxExchangeName);//DLX
                    dlxArgs2.Add("x-dead-letter-routing-key", dlxRoutingKey2);//routingKey

                    //死信队列绑定
                    channel.QueueDeclare("queue_dlx1", true, false, false, dlxArgs2);
                    channel.QueueBind(dlxQueueName2, dlxExchangeName, dlxRoutingKey2, null);

                    #endregion

                    #region 定义普通队列

                    var exchangeName = "topic-exchange";
                    //var queueName1 = $"{exchangeName}-topic-quene1";
                    var queueName2 = $"{exchangeName}-topic-quene2";
                    //var queueName3 = $"{exchangeName}-topic-quene3";

                    // topics模式的routingKey必须是一个英文“.”分隔的字符串，可以存在两种特殊字符"*"与“#”，“*”用于匹配一个单词，“#”用于匹配多个单词（可以是零个）。
                    var routingName1 = $"red.*";
                    var routingName2 = $"blue.*";

                    // 声明一个交换机
                    // durable - 是否持久化 
                    // autoDelete - 是否自动删除
                    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Topic, durable: true, autoDelete: false);
                    // 声明多个队列
                    //channel.QueueDeclare(queue: queueName1, durable: true, exclusive: false, autoDelete: false, arguments: dlxArgs);// 绑定死信队列1
                    channel.QueueDeclare(queue: queueName2, durable: true, exclusive: false, autoDelete: false, arguments: dlxArgs1);// 绑定死信队列2
                    //channel.QueueDeclare(queue: queueName3, durable: true, exclusive: false, autoDelete: false, arguments: dlxArgs);// 绑定死信队列1
                    // 将队列与交换机进行绑定
                    //channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: routingName1);
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: routingName2);
                    //channel.QueueBind(queue: queueName3, exchange: exchangeName, routingKey: routingName2);

                    #endregion

                    //告诉Rabbit每次只能向消费者发送一条信息,再消费者未确认之前,不再向他发送信息
                    channel.BasicQos(0, 1, true);

                    #region 死信队列1的消费者

                    ////创建队列1消费者
                    //var consumer1 = new EventingBasicConsumer(channel);
                    //consumer1.Received += (model, ea) =>
                    //{
                    //    byte[] message = ea.Body.ToArray();//接收到的消息
                    //    Console.WriteLine($"死信队列接收到信息为:DeliveryTag:{ea.DeliveryTag},routingkey:{ea.RoutingKey},{Encoding.UTF8.GetString(message)}");

                    //    // 手动确认消息
                    //    channel.BasicAck(ea.DeliveryTag, true);
                    //};
                    ////消费者开启监听
                    //// autoAck - true：自动确认消息；false：手动确认消息。
                    //channel.BasicConsume(queue: dlxQueueName1, autoAck: false, consumer: consumer1);

                    #endregion

                    #region 死信队列2消费者

                    //创建队列2消费者
                    var consumer2 = new EventingBasicConsumer(channel);
                    consumer2.Received += (model, ea) =>
                    {
                        try
                        {


                            byte[] message = ea.Body.ToArray();//接收到的消息
                            Console.WriteLine($"死信队列2接收到信息为:DeliveryTag:{ea.DeliveryTag},routingkey:{ea.RoutingKey},{Encoding.UTF8.GetString(message)}");

                            // 手动确认消息
                            channel.BasicAck(ea.DeliveryTag, true);
                        }
                        catch (Exception)
                        {

                            // requeue:被拒绝的是否重新入队列；true：重新进入队列 fasle：抛弃此条消息
                            // multiple：是否批量.true:将一次性拒绝所有小于deliveryTag的消息
                            // 这种情况是消费者告诉RabbitMQ服务器,因为某种原因我无法立即处理这条消息，这条消息重新回到队列，或者丢弃.requeue: false表示丢弃这条消息，为true表示重回队列
                            channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                        }
                    };
                    //消费者开启监听
                    // autoAck - true：自动确认消息；false：手动确认消息。
                    channel.BasicConsume(queue: dlxQueueName2, autoAck: false, consumer: consumer2);

                    #endregion

                    Console.ReadLine();
                }
            }
        }
    }
}
