using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisDemo
{
    public class Class1
    {
        public static void Main()
        {
            DateTime dateTime = DateTime.Now.AddSeconds(60);

            bool isSuccess = RedisHelper.StringSet("Test", new List<int> { 1, 2, 3 }, dateTime.Subtract(DateTime.Now));

            Console.WriteLine($"IsSuccess:{isSuccess}");

            List<int> test = RedisHelper.StringGet<List<int>>("Test");

            Console.WriteLine($"Length:{test.Count}");

            Console.ReadKey();
        }
    }
}
