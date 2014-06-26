using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Sharding
{
    internal class Program
    {

        public const int NUMBER_OF_PROVIDER_NODES = 345;
        private static void Main(string[] args)
        {
            
            buildShardMap(NUMBER_OF_PROVIDER_NODES);
            while (true)
            {
                Console.WriteLine("Enter string:");
                string line = Console.ReadLine();
                if (line == "exit")
                {
                    break;
                }
                long key = Hash(line);
                Console.WriteLine(GetNodeNumber(NUMBER_OF_PROVIDER_NODES, key));
                //Console.WriteLine(getShardNumber( key));
            }
            var list = new List<int>();
            for (int i = 0; i < 10000; i++)
            {
                list.Add(GetNodeNumber(NUMBER_OF_PROVIDER_NODES, Hash(RandomString(10))));
               // list.Add(getShardNumber( Hash(RandomString(10))));
            }
            for (int i = 0; i < NUMBER_OF_PROVIDER_NODES; i++)
            {
                Console.WriteLine(list.Count(x => x == i));
            }
            Console.ReadKey();
        }
        private static Random random = new Random((int)DateTime.Now.Ticks);
        private static string RandomString(int size)
        {
            StringBuilder builder = new StringBuilder();
            char ch;
            for (int i = 0; i < size; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
                builder.Append(ch);
            }

            return builder.ToString();
        }
        private static byte[] GetBytes(string str)
        {
            var bytes = new byte[str.Length * sizeof(char)];
            Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
            return bytes;
        }

        public static long Hash(string key)
        {
            var md5CryptoServiceProvider = new MD5CryptoServiceProvider();
            byte[] computeHash = md5CryptoServiceProvider.ComputeHash(GetBytes(key));
            return ((long)(computeHash[3] & 0xFF) << 24) | ((long)(computeHash[2] & 0xFF) << 16) |
                   ((long)(computeHash[1] & 0xFF) << 8) | (computeHash[0] & 0xFF);
        }

        public static int GetNodeNumber(int providerNodes, long hash)
        {
            return (int)(hash % providerNodes);
        }

        public static SortedDictionary<long, int> nodes;

        public static void buildShardMap(int shardsCount)
        {
            nodes = new SortedDictionary<long, int>();
            for (int i = 0; i < shardsCount; i++)
            {
                for (int n = 0; n < 160; n++)
                {
                    nodes.Add(Hash("SHARD-" + i + "-NODE-" + n), i);
                }
            }
        }

        public static int getShardNumber(long key)
        {
            if (!nodes.ContainsKey(key))
            {
                var tailMap = nodes.Where(x=>x.Key > key);
                var j = new SortedDictionary<long, int>();
                foreach (var e in tailMap)
                {
                    j.Add(e.Key, e.Value);
                }
                key = !tailMap.Any() ? nodes.First().Key : j.First().Key;
            }
            return nodes[key];
        }
        
    }
}