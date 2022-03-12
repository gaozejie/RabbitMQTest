using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Producter
{
    public class ConnFactoty
    {

        public static IConnectionFactory GetConnectionFactory()
        {
            IConnectionFactory conFactory = new ConnectionFactory//创建连接工厂对象
            {
                HostName = "192.168.58.129",//IP地址
                Port = 5672,//端口号
                UserName = "guest",//用户账号
                Password = "guest"//用户密码
            };
            return conFactory;
        }
    }
}
