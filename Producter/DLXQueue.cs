using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Producter
{
    internal class DLXQueue
    {
        /// <summary>
        /// 死信队列/延时队列，在topic模式基础上添加死信队列
        /// 
        /// 交换机、队列持久化：设置durable=true
        /// 消息持久化：channel.CreateBasicProperties().Persistent = true;
        /// 
        /// </summary>
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

                    // 消息持久化
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    properties.DeliveryMode = 2;

                    channel.ConfirmSelect();

                    // 生产者消息确认：4、异步确认
                    //消息发送成功的时候进入到这个事件：即RabbitMq服务器告诉生产者，我已经成功收到了消息
                    EventHandler<BasicAckEventArgs> BasicAcks = new EventHandler<BasicAckEventArgs>((o, basic) =>
                    {
                        Console.WriteLine("调用了ack;DeliveryTag:" + basic.DeliveryTag.ToString() + ";Multiple:" + basic.Multiple.ToString() + "时间:" + DateTime.Now.ToString());
                    });
                    //消息发送失败的时候进入到这个事件：即RabbitMq服务器告诉生产者，你发送的这条消息我没有成功的投递到Queue中，或者说我没有收到这条消息。
                    EventHandler<BasicNackEventArgs> BasicNacks = new EventHandler<BasicNackEventArgs>((o, basic) =>
                    {
                        //MQ服务器出现了异常，可能会出现Nack的情况
                        Console.WriteLine("调用了Nacks;DeliveryTag:" + basic.DeliveryTag.ToString() + ";Multiple:" + basic.Multiple.ToString() + "时间:" + DateTime.Now.ToString());
                    });
                    channel.BasicAcks += BasicAcks;
                    channel.BasicNacks += BasicNacks;

                    var i = 0;
                    while (i <= 10)
                    {
                        var msg = $"topic消息内容:{i}";
                        //消息内容
                        //byte[] redBody = Encoding.UTF8.GetBytes(msg + "-red");
                        byte[] blueBody = Encoding.UTF8.GetBytes(msg + "-blue");
                        //发送消息
                        // properties:消息持久化
                        //channel.BasicPublish(exchangeName, routingKey: "red.ABC", basicProperties: properties, body: redBody);
                        channel.BasicPublish(exchangeName, routingKey: "blue.BCD", basicProperties: properties, body: blueBody);
                        Console.WriteLine($"成功发送topic消息:{msg}");
                        i++;

                        // 生产者消息确认：1、普通Confirm，每发送一条消息，等待ma的ack回应
                        //var isOk = channel.WaitForConfirms();
                        //if (isOk)
                        //{
                        //    Console.WriteLine("消息发送成功，MQ服务器确认已经收到消息");
                        //}
                        //else
                        //{
                        //    Console.WriteLine("消息发送失败");
                        //}
                    }

                    // 生产者消息确认：2、等消息全部发送完毕后，等待MQ服务器的ack响应
                    //bool isOk = channel.WaitForConfirms();
                    //if (isOk)
                    //{
                    //    Console.WriteLine("消息发送成功，MQ服务器确认已经收到消息");
                    //}
                    //else
                    //{
                    //    Console.WriteLine("消息发送失败");
                    //}

                    // 生产者消息确认：3、WaitForConfirmsOrDie表示等待已经发送给broker的消息act或者nack之后才会继续执行；即：直到所有信息都发送成功，如果有任何一个消息触发了Nack（即：MQ服务器未确认消息，即：发送失败）则抛出IOException异常
                    //channel.WaitForConfirmsOrDie( );

                    Console.ReadLine();
                }
            }
        }

    }
}
