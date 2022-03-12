// See https://aka.ms/new-console-template for more information
using Consumer;

Console.WriteLine("Hello, World!");


// 1、简单模式/worker模式消费
//SimplePattern.SimplePatternRun();

// 2、交换机模式
// 发布订阅模式(fanout) 消费
//ExchangePattern.ExchangePatternFanout();

// 路由模式 消费
//ExchangePattern.ExchangePatternDirect();

// 通配符模式 消费
//ExchangePattern.ExchangePatternTopic();

// 死信队列
DLXQueue.DLXQueueRun();


