using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO.MemoryMappedFiles;

namespace 平行處理
{
    public class FastCsvReader
    {
        private readonly string _filePath;
        private long _fileSize;
        private readonly long[] _linePositions;

        public FastCsvReader(string filePath)
        {
            _filePath = filePath;
            _fileSize = new FileInfo(filePath).Length;
            _linePositions = BuildLineIndex();
        }

        private long[] BuildLineIndex()
        {
            var positions = new List<long>();
            positions.Add(0); // 第一行的位置

            using (var mmf = MemoryMappedFile.CreateFromFile(_filePath))
            using (var viewer = mmf.CreateViewAccessor(0, _fileSize))
            {
                byte[] buffer = new byte[4096];
                long position = 0;

                while (position < _fileSize)
                {
                    int bytesToRead = (int)Math.Min(4096, _fileSize - position);
                    viewer.ReadArray(position, buffer, 0, bytesToRead);

                    for (int i = 0; i < bytesToRead; i++)
                    {
                        if (buffer[i] == '\n')
                        {
                            positions.Add(position + i + 1);
                        }
                    }
                    position += bytesToRead;
                }
            }

            return positions.ToArray();
        }

        public string ReadLine(long lineNumber)
        {
            if (lineNumber >= _linePositions.Length)
                throw new ArgumentOutOfRangeException();

            using (var mmf = MemoryMappedFile.CreateFromFile(_filePath))
            using (var viewer = mmf.CreateViewAccessor(0, _fileSize))
            {
                long start = _linePositions[lineNumber];
                long end = (lineNumber + 1 < _linePositions.Length)
                    ? _linePositions[lineNumber + 1] - 1
                    : _fileSize;

                int length = (int)(end - start);
                byte[] buffer = new byte[length];
                viewer.ReadArray(start, buffer, 0, length);

                return Encoding.UTF8.GetString(buffer);
            }
        }
    }
}
