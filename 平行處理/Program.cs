// See https://aka.ms/new-console-template for more information


using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;

Stopwatch stopwatch = new Stopwatch();
stopwatch.Start();


var rangePartitioner = Partitioner.Create(0, 10);

Parallel.ForEach(rangePartitioner, range =>
{
    Console.WriteLine(range.Item1 + "," + range.Item2);
});

//await Parallel.ForAsync(0, 10, async (x, y) =>
//{
//    await Task.Delay(1000);
//    Console.WriteLine(x+1 +"任務完成");
//});


//List<Task> list = new List<Task>();
//for(int i=0;i<10;i++)
//{
//    list.Add(Task.Delay(1000));
//}
//await Task.WhenAll(list);
stopwatch.Stop();
Console.WriteLine(stopwatch.ElapsedMilliseconds);

