using CSVLibrary;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using 平行處理練習.Model;
using static System.Collections.Specialized.BitVector32;

namespace 平行處理練習
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var parameter = 10_000_000;
            var stopWatch = new Stopwatch();
            var rootPath = $@"D:\c_sharp\平行處理進階操作\Data";
            var readPath = Path.Combine(rootPath, $@"Input\MOCK_{parameter}.csv");
            var writePath = Path.Combine(rootPath, $@"Output\MOCK_{parameter}.csv");
            var writeDirPath = Path.Combine(rootPath, @"Output");
            Directory.Delete(writeDirPath, true );
            Directory.CreateDirectory(writeDirPath);


            //stopWatch.Start();
            //var users = CSVLibrary.CSVHelper.Read<User>(readPath);
            //stopWatch.Stop();
            //var readTime = stopWatch.ElapsedMilliseconds;
            //Console.WriteLine($"只有讀取：{readTime / 1000.0}s");
            //stopWatch.Restart();
            //CSVLibrary.CSVHelper.Write<User>(users, writePath);
            //stopWatch.Stop();
            //var writeTime = stopWatch.ElapsedMilliseconds;
            //Console.WriteLine($"寫入:{writeTime / 1000.0}s");

            //Console.WriteLine($"總時間:{(readTime + writeTime) / 1000.0}s");


            int batchSize = 4_000_000;
            int batchCount = (parameter % batchSize == 0 ) ? (parameter / batchSize) : (parameter / batchSize) + 1;
            
            List<Task> tasks = new List<Task>();


            
            var readTimeRecords = new List<double>();
            var writeTimeRecords = new List<double>();
            stopWatch.Start();
            for (int i = 0; i < batchCount; i++)
            {
                
                int taskNumber = i;
                Task task = Task.Run(() =>
                {
                    var taskStopWatch = new Stopwatch();
                    Console.WriteLine($"任務{taskNumber+1}啟動");
                    taskStopWatch.Start();
                    var users = CSVLibrary.CSVHelper.Read<User>(readPath, taskNumber * batchSize, batchSize);
                    taskStopWatch.Stop();
                    var taskReadTime = taskStopWatch.ElapsedMilliseconds;
                    readTimeRecords.Add(taskReadTime);
                    Console.WriteLine($"任務{taskNumber + 1}讀取時間：{taskReadTime / 1000.0}s");
                    taskStopWatch.Restart();
                    CSVLibrary.CSVHelper.Write<User>(users, Path.Combine(rootPath, $@"Output\MOCK_{parameter}_{taskNumber+1}.csv"));
                    taskStopWatch.Stop();
                    var taskWriteTime = taskStopWatch.ElapsedMilliseconds;
                    writeTimeRecords.Add(taskWriteTime);
                    Console.WriteLine($"任務{taskNumber + 1}寫入時間：{taskWriteTime / 1000.0}s");

                });

                tasks.Add(task);    
            }

            await Task.WhenAll(tasks);
            stopWatch.Stop();


            Console.WriteLine($"讀取時間:{readTimeRecords.Median() / 1000.0}s");
            Console.WriteLine($"寫入時間:{writeTimeRecords.Median() / 1000.0}s");
            Console.WriteLine($"總時間:{stopWatch.ElapsedMilliseconds / 1000.0}s");

            Console.WriteLine($"| {parameter} | {readTimeRecords.Median() / 1000.0} | {writeTimeRecords.Median() / 1000.0} |{stopWatch.ElapsedMilliseconds / 1000.0}|");




            // 只有讀取：34ms
            // 讀取時間：34ms
            // 寫入時間：64ms
            // 1000筆資料：0.053s
            // 1000筆資料：0.049s

            //Stopwatch stopwatch = new Stopwatch();
            //stopwatch.Start();
            //var users = CSVLibrary.CSVHelper.Read<User>(path, 4900000, 50000);
            //stopwatch.Stop();
            //Console.WriteLine($"只有讀取：{stopwatch.ElapsedMilliseconds}ms");
            //GC.Collect();
            //GetWriteDataTime(1000);
            //GC.Collect();

            //Console.WriteLine($"1000筆資料：{GetReadDataTime(1000)}s");
            //Console.WriteLine($"50000筆資料：{GetWriteDataTime(50000)}s");
            //GC.Collect();
            //var x = 5000000;
            //Console.WriteLine($"{x}筆資料：{GetBatchReadWriteDataTime(x)}s");

            ////Console.WriteLine($"{x}筆資料：{GetBatchReadWriteDataTime(x)}s");
            ////Console.WriteLine($"{x}筆資料：{GetBatchReadWriteDataTime(x)}s");
            //var rootPath = @"C:\Users\art78\Documents\平行處理資料";
            //var path = $@"D:\c_sharp\平行處理練習\平行處理練習\MOCK_{x}.csv";

            //int count = x;
            //int batchCount = 50000;
            //var times = new List<long>();

            //stopWatch.Restart();
            //for (int i = 0; i < ((count < batchCount) ? 1 : (count / batchCount)); i++)
            //{
            //    if(i%27 ==0)
            //    {
            //        // 最徹底的 GC 清理方式
            //        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
            //        GC.WaitForPendingFinalizers();
            //        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
            //    }
            //    //GC.Collect();
            //    var users = CSVLibrary.CSVHelper.Read<User>(path, i * batchCount, batchCount);
            //    stopWatch.Stop();
            //    Console.WriteLine(stopWatch.ElapsedMilliseconds/1000f + "s");
            //    stopWatch.Restart();


            //}

            //stopWatch.Stop();
            //var parameters = new List<int>() {  1_000, 5_000, 10_000, 50_000, 100_000, 150_000, 200_000, 500_000, 1_000_000, 5_000_000, 10_000_000 };
            ////GenerateFiles(parameters);
            //Console.WriteLine("輸入第幾號：");

            //int idx = Convert.ToInt32(Console.ReadLine());
            //if(idx > parameters.Count)
            //{
            //    Console.WriteLine("無效輸入");
            //    return;
            //}
            ////PrintResults(parameters);
            ////Console.WriteLine($"{parameters[idx]}筆資料：{GetReadDataTime(parameters[idx])}s");
            //Console.WriteLine($"{parameters[idx]}筆資料：{GetWriteDataTime(parameters[idx])}s");


            //Parallel.For(1, 100, x =>
            //{

            //});

            //Stopwatch stopwatch = new Stopwatch();
            //stopwatch.Start();
            //Parallel.For(0, 10, async x =>
            //{
            //    await Task.Delay(1000);
            //    Console.WriteLine(x);
            //});
            //stopwatch.Stop();
            //Console.WriteLine(stopwatch.ElapsedMilliseconds);



            //Task.Run(() => {

            //    Console.WriteLine("HELLO1");
            //});
            //Task.Run(() => {

            //    Console.WriteLine("HELLO2");
            //});
            //Task.Run(() => {

            //    Console.WriteLine("HELLO3");
            //});

            //Console.WriteLine("WORLD");

            //int count = 10000000;
            //int batchCount = 2000000;
            //var allUsers = new List<User>();
            //var path = @"D:\c_sharp\平行處理練習\平行處理練習\MOCK_10000000.csv";
            //var newDirPath = @"D:\c_sharp\平行處理練習\平行處理練習\writeData";
            //var tasks = new List<Task>();
            //for (int i = 0; i < ((count < batchCount) ? 1 : (count / batchCount)); i++)
            //{
            //    int taskNumber = i;

            //    var task = new Task(() =>
            //    {
            //        Console.WriteLine($"第{taskNumber + 1}項任務啟動！");
            //        var users = CSVLibrary.CSVHelper.Read<User>(path, taskNumber * batchCount, batchCount);
            //        var writePath = Path.Combine(newDirPath, $"MOCK_10000000_{taskNumber + 1}.csv");
            //        CSVLibrary.CSVHelper.Write<User>(users, writePath);
            //        Console.WriteLine($"第{taskNumber + 1}項任務完成！");
            //        // 最徹底的 GC 清理方式
            //        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
            //        GC.WaitForPendingFinalizers();
            //        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);

            //    });
            //    task.Start();
            //    tasks.Add(task);
            //}
            //stopWatch.Start();
            //await Task.WhenAll(tasks);
            //stopWatch.Stop();
            //Console.WriteLine($"test1:{stopWatch.ElapsedMilliseconds / 1000f}s");

            ////五百萬筆15秒
            ////一千萬比65秒
            //var x = 10000000;
            //var sec = await FiveThreadReadWriteDataTime(x);
            //await Console.Out.WriteLineAsync($"test2:{x}筆{sec}秒");


            //6038
            //7173
            //6754
            //7900
            //7386
            //7905
            //9568
            //11107
            //20147

            //var path = @"D:\c_sharp\平行處理練習\平行處理練習\MOCK_10000000.csv";
            //CSVLibrary.CSVHelper.Read<User>(path,  0 ,1000000);
            //Console.WriteLine(stopWatch.ElapsedMilliseconds);
            //stopWatch.Restart();

            //CSVLibrary.CSVHelper.Read<User>(path, 1000000, 1000000);
            //Console.WriteLine(stopWatch.ElapsedMilliseconds);
            //stopWatch.Restart();
            //CSVLibrary.CSVHelper.Read<User>(path, 2000000, 1000000);
            //Console.WriteLine(stopWatch.ElapsedMilliseconds);
            //stopWatch.Restart();
            //CSVLibrary.CSVHelper.Read<User>(path, 3000000, 1000000);
            //Console.WriteLine(stopWatch.ElapsedMilliseconds);
            //stopWatch.Restart();
            //CSVLibrary.CSVHelper.Read<User>(path, 4000000, 1000000);
            //Console.WriteLine(stopWatch.ElapsedMilliseconds);
            //stopWatch.Restart();
            //CSVLibrary.CSVHelper.Read<User>(path, 5000000, 1000000);
            //Console.WriteLine(stopWatch.ElapsedMilliseconds);
            //stopWatch.Restart();
            //CSVLibrary.CSVHelper.Read<User>(path, 6000000, 1000000);
            //Console.WriteLine(stopWatch.ElapsedMilliseconds);
            //stopWatch.Restart();
            //CSVLibrary.CSVHelper.Read<User>(path, 7000000, 1000000);
            //Console.WriteLine(stopWatch.ElapsedMilliseconds);
            //stopWatch.Restart();
            //CSVLibrary.CSVHelper.Read<User>(path, 8000000, 1000000);
            //Console.WriteLine(stopWatch.ElapsedMilliseconds);
            //stopWatch.Restart();

            //CSVLibrary.CSVHelper.Read<User>(path, 9000000, 1000000);
            //Console.WriteLine(stopWatch.ElapsedMilliseconds);
            //stopWatch.Restart();

            //ParallelizedBatchWithPartitioner(5000000, 1000000);

            //var res = ProcessLargeFile(@"D:\c_sharp\平行處理練習\平行處理練習\MOCK_5000000.csv");
            // 或
            //var sec = await FiveThreadReadWriteDataTime(10000000);
            //await Console.Out.WriteLineAsync($"{sec}s");

            // 平行處理多個批次
            //Parallel.ForEach(
            //    Partitioner.Create(0, 10_000_000, 100_000),
            //    range =>
            //    {
            //        for (long i = range.Item1; i < range.Item2; i++)
            //        {

            //            // 處理該行資料
            //        }
            //    }
            //);
            Console.ReadKey();
        }

        public static List<string> ProcessLargeFile(string _filePath)
        {
            List<string> res = new List<string>();
            var reader = new MemoryMappedCsvReader(_filePath);

            var fileSize =  new FileInfo(_filePath).Length;
            // 將檔案分成多個區段
            var segments = CalculateSegments(fileSize, Environment.ProcessorCount);

            Parallel.ForEach(segments, segment =>
            {
                // 每個執行緒處理自己的區段
                var stringData = reader.ReadAt(segment.Start, segment.Length);
                var data = stringData.Split(new string[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries );
                res.AddRange(data);
            });
            return res;
        }
        public class FileSegment
        {
            public long Start { get; set; }    // 區段的起始位置
            public int Length { get; set; }    // 區段的長度
        }

        // 計算區段的方法
        private static List<FileSegment> CalculateSegments(long fileSize, int numberOfSegments)
        {
            var segments = new List<FileSegment>();
            long segmentSize = fileSize / numberOfSegments;  // 計算每個區段的大約大小

            for (int i = 0; i < numberOfSegments; i++)
            {
                long start = i * segmentSize;  // 計算區段起始位置

                // 計算區段長度
                int length;
                if (i == numberOfSegments - 1)
                {
                    // 最後一個區段可能會比較大或小
                    length = (int)(fileSize - start);
                }
                else
                {
                    length = (int)segmentSize;
                }

                segments.Add(new FileSegment
                {
                    Start = start,
                    Length = length
                });
            }

            return segments;
        }

        static long GetRunTime(Action action)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            action.Invoke();
            stopwatch.Stop();
            return stopwatch.ElapsedMilliseconds;
        }

        static void GenerateFiles(List<int> parameters)
        {
            var path = @"D:\c_sharp\平行處理練習\平行處理練習\MOCK_1000.csv";
            var users = CSVLibrary.CSVHelper.Read<User>(path);
            foreach (var count in parameters)
            {
                var p = $@"D:\c_sharp\平行處理練習\平行處理練習\MOCK_{count}.csv";
                if(!File.Exists(p))
                {
                    for (int i = 0; i < count / 1000; i++)
                    {
                        CSVLibrary.CSVHelper.Write<User>(users, p);
                    }
                }
            }
        }

        static void PrintResults(List<int> parameters)
        {
            foreach(var p in parameters)
            {
                GC.Collect();
                Console.WriteLine($"{p}筆資料開始執行");

                var time = GetReadDataTime(p);
                Console.WriteLine($"{p}筆資料執行時間：{time}s");
                Console.WriteLine("按一下繼續下一筆");
                Console.ReadKey();
            }
        }

        static void PrintReadandWriteResults(List<int> parameters)
        {
            foreach (var p in parameters)
            {
                GC.Collect();
                Console.WriteLine($"{p}筆資料開始執行");

                var time = GetReadDataTime(p);
                Console.WriteLine($"{p}筆資料執行時間：{time}s");
                Console.WriteLine("按一下繼續下一筆");
                Console.ReadKey();
            }
        }

        static float GetReadDataTime(int p)
        {
            var path = $@"D:\c_sharp\平行處理練習\平行處理練習\MOCK_{p}.csv";
            return GetRunTime(() => CSVLibrary.CSVHelper.Read<User>(path)) / 1000f;
        }

        static float GetBatchReadDataTime(int p)
        {
            var path = $@"D:\c_sharp\平行處理練習\平行處理練習\MOCK_{p}.csv";
            int batchCount = 50000;
            


            int count = p;
            

            return GetRunTime(() => {

                
                for (int i = 0; i < ((count < batchCount) ? 1 : (count / batchCount)); i++)
                {
                    GC.Collect();
                    var users = CSVLibrary.CSVHelper.Read<User>(path, i * batchCount, batchCount);
                }


            }) / 1000f;
        }


        static float GetWriteDataTime(int p)
        {
            var path = $@"D:\c_sharp\平行處理練習\平行處理練習\MOCK_{p}.csv";
            var newDirPath = @"D:\c_sharp\平行處理練習\平行處理練習\writeData";
            Directory.CreateDirectory(newDirPath);

            //Stopwatch stopwatch1 = new Stopwatch();
            //stopwatch1.Start();
            //var users = CSVLibrary.CSVHelper.Read<User>(path);
            //stopwatch1.Stop();
            //Console.WriteLine($"讀取時間：{stopwatch1.ElapsedMilliseconds}ms");
            //stopwatch1.Restart();
            //CSVLibrary.CSVHelper.Write<User>(users, Path.Combine(newDirPath, $"MOCK_{p}.csv"));
            //stopwatch1.Stop();
            //Console.WriteLine($"寫入時間：{stopwatch1.ElapsedMilliseconds}ms");




            return GetRunTime(() => {
                var user = CSVLibrary.CSVHelper.Read<User>(path);
                CSVLibrary.CSVHelper.Write<User>(user, Path.Combine(newDirPath, $"MOCK_{p}.csv"));
                
            }) / 1000f;
        }

        static float GetBatchWriteDataTime(int p)
        {
            int batchCount = 50000;
            var path = $@"D:\c_sharp\平行處理練習\平行處理練習\MOCK_{p}.csv";
            var newDirPath = @"D:\c_sharp\平行處理練習\平行處理練習\writeData";
            Directory.CreateDirectory(newDirPath);

            //Stopwatch stopwatch1 = new Stopwatch();
            //stopwatch1.Start();
            //var users = CSVLibrary.CSVHelper.Read<User>(path);
            //stopwatch1.Stop();
            //Console.WriteLine($"讀取時間：{stopwatch1.ElapsedMilliseconds}ms");
            //stopwatch1.Restart();
            //CSVLibrary.CSVHelper.Write<User>(users, Path.Combine(newDirPath, $"MOCK_{p}.csv"));
            //stopwatch1.Stop();
            //Console.WriteLine($"寫入時間：{stopwatch1.ElapsedMilliseconds}ms");


            int count = p;
            var writePath = Path.Combine(newDirPath, $"MOCK_{p}.csv");
            File.Delete(writePath);

            return GetRunTime(() => {

                var users = CSVLibrary.CSVHelper.Read<User>(path);
                for (int i = 0; i < ((count < batchCount) ? 1 : (count / batchCount)); i++)
                {
                    if (i % 27 == 0)
                    {
                        // 最徹底的 GC 清理方式
                        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
                        GC.WaitForPendingFinalizers();
                        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
                    }
                    if (count < batchCount)
                    {
                        CSVLibrary.CSVHelper.Write<User>(users, writePath);
                        break;
                    }

                    CSVLibrary.CSVHelper.Write<User>(users.Skip(i * batchCount).Take(batchCount).ToList(), writePath);
                    //for (int j = 0; j < ((count < batchCount) ? count : batchCount) ; j++)
                    //{
                    //    CSVLibrary.CSVHelper.Write<User>(users[i * batchCount + j], writePath);
                    //}
                }
                

            }) / 1000f;
        }

        static float GetBatchReadWriteDataTime(int p)
        {
            int batchCount = 50000;
            //var path = $@"C:\Users\art78\Documents\平行處理資料\MOCK_{p}.csv";
            //var newDirPath = @"C:\Users\art78\Documents\平行處理資料\writeData";
            var path = $@"D:\c_sharp\平行處理練習\平行處理練習\MOCK_{p}.csv";
            var newDirPath = @"D:\c_sharp\平行處理練習\平行處理練習\writeData";
            Directory.CreateDirectory(newDirPath);

            //Stopwatch stopwatch1 = new Stopwatch();
            //stopwatch1.Start();
            //var users = CSVLibrary.CSVHelper.Read<User>(path);
            //stopwatch1.Stop();
            //Console.WriteLine($"讀取時間：{stopwatch1.ElapsedMilliseconds}ms");
            //stopwatch1.Restart();
            //CSVLibrary.CSVHelper.Write<User>(users, Path.Combine(newDirPath, $"MOCK_{p}.csv"));
            //stopwatch1.Stop();
            //Console.WriteLine($"寫入時間：{stopwatch1.ElapsedMilliseconds}ms");


            int count = p;
            var writePath = Path.Combine(newDirPath, $"MOCK_{p}.csv");
            File.Delete(writePath);

            return GetRunTime(() => {

                for (int i = 0; i < ((count < batchCount) ? 1 : (count / batchCount)); i++)
                {
                    if (i % 27 == 0)
                    {
                        // 最徹底的 GC 清理方式
                        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
                        GC.WaitForPendingFinalizers();
                        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
                    }
                    var users = CSVLibrary.CSVHelper.Read<User>(path, i * batchCount, batchCount);
                    if (count < batchCount)
                    {
                        CSVLibrary.CSVHelper.Write<User>(users, writePath);
                        break;
                    }

                    CSVLibrary.CSVHelper.Write<User>(users.Skip(i * batchCount).Take(batchCount).ToList(), writePath);
                    //for (int j = 0; j < ((count < batchCount) ? count : batchCount) ; j++)
                    //{
                    //    CSVLibrary.CSVHelper.Write<User>(users[i * batchCount + j], writePath);
                    //}
                }


            }) / 1000f;
        }

        static async Task<float> FiveThreadReadWriteDataTime(int p)
        {
            int batchCount = p / 5;
            //var path = $@"C:\Users\art78\Documents\平行處理資料\MOCK_{p}.csv";
            //var newDirPath = @"C:\Users\art78\Documents\平行處理資料\writeData";
            var path = $@"D:\c_sharp\平行處理練習\平行處理練習\MOCK_{p}.csv";
            var newDirPath = @"D:\c_sharp\平行處理練習\平行處理練習\writeData";
            Directory.CreateDirectory(newDirPath);

            int count = p;
            var tasks = new List<Task>();
            for (int i = 0; i < ((count < batchCount) ? 1 : (count / batchCount)); i++)
            {
                int taskNumber = i;

                var task = new Task(() =>
                {
                    long total = 0;
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();
                    Console.WriteLine($"第{taskNumber + 1}項任務啟動！");
                    var users = CSVLibrary.CSVHelper.Read<User>(path, taskNumber * batchCount, batchCount);
                    stopwatch.Stop();
                    total += stopwatch.ElapsedMilliseconds;
                    Console.WriteLine($"第{taskNumber + 1}項任務讀取完成，總共耗時:{total}！");
                    stopwatch.Restart();
                    var writePath = Path.Combine(newDirPath, $"MOCK_{count}_{taskNumber + 1}.csv");
                    CSVLibrary.CSVHelper.Write<User>(users, writePath);
                    stopwatch.Stop();

                    Console.WriteLine($"第{taskNumber + 1}項任務讀取完成，總共耗時:{stopwatch.ElapsedMilliseconds}！");
                    total += stopwatch.ElapsedMilliseconds;
                    Console.WriteLine($"第{taskNumber + 1}項任務完成！");

                    Console.WriteLine($"第{taskNumber + 1}項任務總共耗時:{total}！");

                    // 最徹底的 GC 清理方式
                    GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
                    GC.WaitForPendingFinalizers();
                    GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);

                });
                task.Start();
                tasks.Add(task);
            }
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            await Task.WhenAll(tasks);
            stopWatch.Stop();
            return stopWatch.ElapsedMilliseconds / 1000f;
        }

        static void ParallelizedBatchWithPartitioner(int lineNum, int batchSize)
        {
            int batchNum = lineNum / batchSize;
            var path = $@"D:\c_sharp\平行處理練習\平行處理練習\MOCK_{lineNum}.csv";
            var newDirPath = @"D:\c_sharp\平行處理練習\平行處理練習\writeData";
            Directory.CreateDirectory(newDirPath);
            
            var rangePartitioner = Partitioner.Create(0, batchNum);

            Parallel.ForEach(rangePartitioner, range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    int fileIndex = i;
                    var data = CSVLibrary.CSVHelper.Read<User>(path, fileIndex * batchSize + 1, batchSize);
                    var writePath = Path.Combine(newDirPath, $"MOCK_{lineNum}_{fileIndex + 1}.csv");
                    CSVLibrary.CSVHelper.Write<User>(data ,writePath);
                }
            });
        }
    }
}
