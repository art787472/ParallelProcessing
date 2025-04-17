// See https://aka.ms/new-console-template for more information


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
        static async Task Main(string[] args)
        {


            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();


            var parameter = 70_000_000;
            int batchSize = 3_000_000;
            int batchCount = (parameter % batchSize == 0) ? (parameter / batchSize) : (parameter / batchSize) + 1;
            var rootPath = $@"D:\c_sharp\平行處理進階操作\Data";
            var readPath = @"D:\c_sharp\平行處理進階操作\Data\Input\7000萬.csv";
            var writeFileName = "7000萬";
            var writePath = @"D:\c_sharp\平行處理進階操作\Data\Output";
            var writeDirPath = Path.Combine(rootPath, @"Output");
            Directory.Delete(writeDirPath, true);
            Directory.CreateDirectory(writeDirPath);

            var readTimeRecords = new List<double>();
            var writeTimeRecords = new List<double>();

            await Parallel.ForAsync(0, batchCount, new ParallelOptions { MaxDegreeOfParallelism = 6 }, async (x, y) =>
            {
                await Task.Run(() =>
                {
                    var taskStopWatch = new Stopwatch();
                    Console.WriteLine($"任務{x + 1}啟動");
                    taskStopWatch.Start();
                    var users = CSVHelper.OptimizeRead<User>(readPath, x * batchSize, batchSize);
                    taskStopWatch.Stop();
                    var taskReadTime = taskStopWatch.ElapsedMilliseconds;
                    readTimeRecords.Add(taskReadTime);
                    Console.WriteLine($"任務{x + 1}讀取時間：{taskReadTime / 1000.0}s");
                    taskStopWatch.Restart();
                    CSVHelper.OptimizeWrite<User>(users, Path.Combine(writePath, @$"{writeFileName}_{x + 1}.csv"));
                    taskStopWatch.Stop();
                    var taskWriteTime = taskStopWatch.ElapsedMilliseconds;
                    writeTimeRecords.Add(taskWriteTime);
                    Console.WriteLine($"任務{x + 1}寫入時間：{taskWriteTime / 1000.0}s");
                });
                
            });


            //List<Task> list = new List<Task>();
            //for(int i=0;i<10;i++)
            //{
            //    list.Add(Task.Delay(1000));
            //}
            //await Task.WhenAll(list);
            stopwatch.Stop();
            Console.WriteLine(stopwatch.ElapsedMilliseconds / 1000.0 + "s");
            Console.WriteLine($"讀取時間:{readTimeRecords.Median() / 1000.0}s");
            Console.WriteLine($"寫入時間:{writeTimeRecords.Median() / 1000.0}s");
            Console.WriteLine($"總時間:{stopwatch.ElapsedMilliseconds / 1000.0}s");

            Console.WriteLine($"| {parameter} | {readTimeRecords.Median() / 1000.0} | {writeTimeRecords.Median() / 1000.0} |{stopwatch.ElapsedMilliseconds / 1000.0}|");


        }
    }

}



