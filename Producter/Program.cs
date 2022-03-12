// See https://aka.ms/new-console-template for more information
using Producter;
using RabbitMQ.Client;
using System.Text;

Console.WriteLine("Hello, World!");

// 1、简单模式
//SimplePattern.SimplePatternRun();

// 2、交换机模式
// 发布订阅模式
//ExchangePattern.ExchangePatternFanout();

// 路由模式
//ExchangePattern.ExchangePatternDirect();

// 通配符模式
//ExchangePattern.ExchangePatternTopic();


// 死信队列
DLXQueue.DLXQueueRun();