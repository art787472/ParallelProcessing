// See https://aka.ms/new-console-template for more information


using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Reflection.Metadata;
using CSVLibrary;
using 平行處理練習;
using 平行處理練習.Model;


namespace 平行處理
{
    internal class Program
    {
        private static readonly object key = new object();
        private static readonly ReaderWriterLockSlim _lockSlim = new ReaderWriterLockSlim();
        private static Mutex mutex = new Mutex();
        static async Task Main(string[] args)
        {

            //ConcurrentBag<int> numbers = new ConcurrentBag<int>();

            //await Parallel.ForAsync(0, 100, (x, token) =>
            //{

            //    numbers.Add(x);
            //    return ValueTask.CompletedTask; 
            //});

            //Console.WriteLine(numbers.Count);






            //Stopwatch stopwatch = new Stopwatch();
            //stopwatch.Start();


            var parameter = 100_000_000;
            int batchSize = 3_000_000;
            int batchCount = (parameter % batchSize == 0) ? (parameter / batchSize) : (parameter / batchSize) + 1;
            var rootPath = $@"D:\c_sharp\平行處理進階操作\Data";
            var readPath = @"D:\c_sharp\平行處理進階操作\Data\Input\1億.csv";
            var writeFileName = "1億";
            var writePath = @"D:\c_sharp\平行處理進階操作\Data\Output";
            var writeDirPath = Path.Combine(rootPath, @"Output");
            Directory.Delete(writeDirPath, true);
            Directory.CreateDirectory(writeDirPath);

            var readTimeRecords = new List<double>();
            var writeTimeRecords = new List<double>();

            //await Parallel.ForAsync(0, batchCount, new ParallelOptions { MaxDegreeOfParallelism = 6 }, async (x, y) =>
            //{
            //    await Task.Run(() =>
            //    {
            //        var taskStopWatch = new Stopwatch();
            //        Console.WriteLine($"任務{x + 1}啟動");
            //        taskStopWatch.Start();
            //        var users = CSVHelper.OptimizeRead<User>(readPath, x * batchSize, batchSize);
            //        taskStopWatch.Stop();
            //        var taskReadTime = taskStopWatch.ElapsedMilliseconds;
            //        readTimeRecords.Add(taskReadTime);
            //        Console.WriteLine($"任務{x + 1}讀取時間：{taskReadTime / 1000.0}s");
            //        taskStopWatch.Restart();
            //        lock (key)
            //        {
            //            CSVHelper.OptimizeWrite<User>(users, Path.Combine(writePath, @$"{writeFileName}.csv"));

            //        }

            //        taskStopWatch.Stop();
            //        var taskWriteTime = taskStopWatch.ElapsedMilliseconds;
            //        writeTimeRecords.Add(taskWriteTime);
            //        Console.WriteLine($"任務{x + 1}寫入時間：{taskWriteTime / 1000.0}s");
            //    });

            //});

            //var queue = new ConcurrentQueue<List<User>>();
            //var queueSignal = new AutoResetEvent(false);

            //string fullWritePath = Path.Combine(writePath, $"{writeFileName}.csv");

            //// 啟動一個單獨寫入執行緒
            //var writerTask = Task.Run(() =>
            //{
            //    while (true)
            //    {
            //        queueSignal.WaitOne(); // 等待訊號
            //        while (queue.TryDequeue(out var users))
            //        {
            //            var sw = Stopwatch.StartNew();
            //            CSVHelper.OptimizeWrite<User>(users, fullWritePath);
            //            sw.Stop();
            //            writeTimeRecords.Add(sw.ElapsedMilliseconds);
            //        }
            //    }
            //});

            //await Parallel.ForAsync(0, batchCount, new ParallelOptions { MaxDegreeOfParallelism = 6 }, async (x, _) =>
            //{
            //    var sw = Stopwatch.StartNew();
            //    Console.WriteLine($"任務{x + 1}啟動");

            //    // 讀資料
            //    var users = await Task.Run(() => CSVHelper.OptimizeRead<User>(readPath, x * batchSize, batchSize));

            //    sw.Stop();
            //    var readTime = sw.ElapsedMilliseconds;
            //    readTimeRecords.Add(readTime);
            //    Console.WriteLine($"任務{x + 1}讀取時間：{readTime / 1000.0}s");

            //    // 寫入排隊
            //    queue.Enqueue(users);
            //    queueSignal.Set(); // 通知寫入執行緒
            //});



            ////List<Task> list = new List<Task>();
            ////for(int i=0;i<10;i++)
            ////{
            ////    list.Add(Task.Delay(1000));
            ////}
            ////await Task.WhenAll(list);
            //stopwatch.Stop();
            //Console.WriteLine(stopwatch.ElapsedMilliseconds / 1000.0 + "s");
            //Console.WriteLine($"讀取時間:{readTimeRecords.Median() / 1000.0}s");
            //Console.WriteLine($"寫入時間:{writeTimeRecords.Median() / 1000.0}s");
            //Console.WriteLine($"總時間:{stopwatch.ElapsedMilliseconds / 1000.0}s");

            //Console.WriteLine($"| {parameter} | {readTimeRecords.Median() / 1000.0} | {writeTimeRecords.Median() / 1000.0} |{stopwatch.ElapsedMilliseconds / 1000.0}|");

            ConcurrentQueue<User> queue = new ConcurrentQueue<User>();
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            await Parallel.ForAsync(0, batchCount, new ParallelOptions { MaxDegreeOfParallelism = 5 }, async (x, y) =>
            //await Parallel.ForAsync(0, len, async (x, y) =>
            {


                int index = x;
                await Task.Run(() =>
                {
                    Console.WriteLine($"任務{index}開始");
                    var userInfos = CSVHelper.OptimizeRead<User>(readPath, x * batchSize, batchSize);
                    foreach (User userInfo in userInfos)
                        queue.Enqueue(userInfo);
                });
            });
            CSVHelper.OptimizeWrite<User>(queue.ToList(), Path.Combine(writePath, @$"{writeFileName}.csv"));
            stopwatch.Stop();
            Console.WriteLine($"任務結束，{stopwatch.ElapsedMilliseconds / 1000.0}s");


            //concurrentBag
            //ConcurrentBag<User> bags = new ConcurrentBag<User>();
            //Stopwatch stopwatch = new Stopwatch();
            //stopwatch.Start();
            //await Parallel.ForAsync(0, batchCount, new ParallelOptions { MaxDegreeOfParallelism = 5 }, async (x, y) =>
            ////await Parallel.ForAsync(0, len, async (x, y) =>
            //{

            //    int index = x;
            //    await Task.Run(() =>
            //    {
            //        Console.WriteLine($"任務{index}開始");
            //        var userInfos = CSVHelper.OptimizeRead<User>(readPath, x * batchSize, batchSize);
            //        foreach (User userInfo in userInfos)
            //            bags.Add(userInfo);
            //    });
            //});
            //CSVHelper.OptimizeWrite<User>(bags.ToList(), Path.Combine(writePath, @$"{writeFileName}.csv"));
            //stopwatch.Stop();
            //Console.WriteLine($"任務結束，{stopwatch.ElapsedMilliseconds / 1000.0}s");
        }
    }

}



