using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace example
{
    class Program
    {
        static void Main(string[] args)
        {

            var sw = System.Diagnostics.Stopwatch.StartNew();

            string infile = AppDomain.CurrentDomain.BaseDirectory + @"\..\..\..\..\ratings.txt";
            int maxCapacity = 10000;
            BlockingCollection<string> shareStructure = new BlockingCollection<string>(maxCapacity);

            sw.Start();

            var tf = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

            Task producer = tf.StartNew(() => {
                foreach (string record in File.ReadLines(infile))
                {
                    shareStructure.Add(record);
                }
                shareStructure.CompleteAdding();
            });

            Task<Dictionary<int, int>>[] consumers = new Task<Dictionary<int, int>>[4];

            for (int i = 0; i < 4; i++)
            {
                consumers[i] = tf.StartNew(() => {
                    Dictionary<int, int> tls = new Dictionary<int, int>();
                    while (!shareStructure.IsCompleted)
                    {
                        try
                        {
                            string record = shareStructure.Take();
                            int userId = parse(record);
                            if (tls.ContainsKey(userId))
                            {
                                tls[userId]++;
                            } else
                            {
                                tls.Add(userId, 1);
                            }
                        } catch (ObjectDisposedException)
                        {
                            //ignore Exception
                        } catch (InvalidOperationException)
                        {
                            //ignore Exception
                        }
                    }

                    return tls;
                });
            }

            Dictionary<int, int> allUsersRating = new Dictionary<int, int>();
            int completed = 0;
            while(completed < 4)
            {
                int taskIndex = Task.WaitAny(consumers);
                Dictionary<int, int> tls = consumers[taskIndex].Result;
                foreach (int userId in tls.Keys)
                {
                    int reviewsCounter = tls[userId];
                    if (allUsersRating.ContainsKey(userId))
                    {
                        allUsersRating[userId]++;
                    }
                    else
                    {
                        allUsersRating.Add(userId, 1);
                    }
                }
                completed++;
            }

            var top10 = allUsersRating.OrderByDescending(x => x.Value).Take(10);
            foreach (var userVotes in top10)
            {
                Console.WriteLine("{0}: {1}", userVotes.Key, userVotes.Value);
            }

            sw.Stop();
            Console.WriteLine("Time: {0}", sw.ElapsedMilliseconds / 1000);
        }

        private static int parse(string line)
        {
            char[] separators = { ',' };

            //
            // movie id, user id, rating (1..5), date (YYYY-MM-DD)
            //
            string[] tokens = line.Split(separators);

            int movieid = Convert.ToInt32(tokens[0]);
            int userid = Convert.ToInt32(tokens[1]);
            int rating = Convert.ToInt32(tokens[2]);
            DateTime date = Convert.ToDateTime(tokens[3]);

            return userid;
        }
    }
}
