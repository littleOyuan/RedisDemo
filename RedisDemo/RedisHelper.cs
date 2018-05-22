using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;

namespace RedisDemo
{
    public class RedisHelper
    {
        private static readonly object Locker = new object();
        private static IConnectionMultiplexer _connMultiplexer;

        private RedisHelper()
        {
        }

        private static IConnectionMultiplexer ConnectionMultiplexer
        {
            get
            {
                if (_connMultiplexer == null)
                {
                    lock (Locker)
                    {
                        if (_connMultiplexer == null)
                            _connMultiplexer = StackExchange.Redis.ConnectionMultiplexer.Connect("10.10.21.214:6379");
                    }
                }

                RegisterEvent();

                return _connMultiplexer;
            }
        }

        /// <summary>
        /// 获取Redis数据库
        /// </summary>
        /// <param name="db"></param>
        public static IDatabase GetDatabase(int db = -1)
        {
            return ConnectionMultiplexer.GetDatabase(db);
        }

        public static bool KeyExists(string redisKey)
        {
            return GetDatabase().KeyExists(redisKey);
        }

        public static bool KeyRename(string redisKey, string redisNewKey)
        {
            return GetDatabase().KeyRename(redisKey, redisNewKey);
        }

        public static bool KeyExpire(string redisKey, TimeSpan? expiry)
        {
            return GetDatabase().KeyExpire(redisKey, expiry);
        }

        public static bool KeyDelete(string redisKey)
        {
            return GetDatabase().KeyDelete(redisKey);
        }

        public static long KeyDelete(IEnumerable<string> redisKeys)
        {
            return GetDatabase().KeyDelete(redisKeys.Select(key => (RedisKey)key).ToArray());
        }

        public static bool StringSet(string redisKey, string redisValue, TimeSpan? expiry = null)
        {
            return GetDatabase().StringSet(redisKey, redisValue, expiry);
        }

        public static bool StringSet<T>(string redisKey, T redisValue, TimeSpan? expiry = null)
        {
            return GetDatabase().StringSet(redisKey, Serialize(redisValue), expiry);
        }

        public static bool StringSet(IEnumerable<KeyValuePair<RedisKey, RedisValue>> keyValuePairs)
        {
            keyValuePairs = keyValuePairs.Select(kv => new KeyValuePair<RedisKey, RedisValue>(kv.Key, kv.Value));

            return GetDatabase().StringSet(keyValuePairs.ToArray());
        }

        public static string StringGet(string redisKey)
        {
            return GetDatabase().StringGet(redisKey);
        }

        public static T StringGet<T>(string redisKey)
        {
            return Deserialize<T>(GetDatabase().StringGet(redisKey));
        }

        #region 序列化、反序列化
        private static byte[] Serialize(object obj)
        {
            if (obj == null)
                return null;

            BinaryFormatter binaryFormatter = new BinaryFormatter();

            using (MemoryStream memoryStream = new MemoryStream())
            {
                binaryFormatter.Serialize(memoryStream, obj);

                byte[] objectDataAsStream = memoryStream.ToArray();

                return objectDataAsStream;
            }
        }

        private static T Deserialize<T>(byte[] stream)
        {
            if (stream == null)
                return default(T);

            BinaryFormatter binaryFormatter = new BinaryFormatter();

            using (MemoryStream memoryStream = new MemoryStream(stream))
            {
                T result = (T)binaryFormatter.Deserialize(memoryStream);

                return result;
            }
        }
        #endregion

        #region 注册事件
        private static void RegisterEvent()
        {
            _connMultiplexer.ConnectionRestored += ConnMultiplexer_ConnectionRestored;
            _connMultiplexer.ConnectionFailed += ConnMultiplexer_ConnectionFailed;
            _connMultiplexer.ErrorMessage += ConnMultiplexer_ErrorMessage;
            _connMultiplexer.ConfigurationChanged += ConnMultiplexer_ConfigurationChanged;
            _connMultiplexer.HashSlotMoved += ConnMultiplexer_HashSlotMoved;
            _connMultiplexer.InternalError += ConnMultiplexer_InternalError;
            _connMultiplexer.ConfigurationChangedBroadcast += ConnMultiplexer_ConfigurationChangedBroadcast;
        }

        /// <summary>
        /// 建立物理连接时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_ConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            Console.WriteLine($"{nameof(ConnMultiplexer_ConnectionRestored)}: {e.Exception}");
        }

        /// <summary>
        /// 物理连接失败时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_ConnectionFailed(object sender, ConnectionFailedEventArgs e)
        {
            Console.WriteLine($"{nameof(ConnMultiplexer_ConnectionFailed)}: {e.Exception}");
        }

        /// <summary>
        /// 发生错误时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_ErrorMessage(object sender, RedisErrorEventArgs e)
        {
            Console.WriteLine($"{nameof(ConnMultiplexer_ErrorMessage)}: {e.Message}");
        }

        /// <summary>
        /// 配置更改时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_ConfigurationChanged(object sender, EndPointEventArgs e)
        {
            Console.WriteLine($"{nameof(ConnMultiplexer_ConfigurationChanged)}: {e.EndPoint}");
        }

        /// <summary>
        /// 更改集群时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_HashSlotMoved(object sender, HashSlotMovedEventArgs e)
        {
            Console.WriteLine($"{nameof(ConnMultiplexer_HashSlotMoved)}: {nameof(e.OldEndPoint)}-{e.OldEndPoint} To {nameof(e.NewEndPoint)}-{e.NewEndPoint}, ");
        }

        /// <summary>
        /// 发生内部错误时（主要用于调试）
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_InternalError(object sender, InternalErrorEventArgs e)
        {
            Console.WriteLine($"{nameof(ConnMultiplexer_InternalError)}: {e.Exception}");
        }

        /// <summary>
        /// 重新配置广播时（通常意味着主从同步更改）
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_ConfigurationChangedBroadcast(object sender, EndPointEventArgs e)
        {
            Console.WriteLine($"{nameof(ConnMultiplexer_ConfigurationChangedBroadcast)}: {e.EndPoint}");
        }
        #endregion

    }
}
