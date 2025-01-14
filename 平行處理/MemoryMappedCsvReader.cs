using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO.MemoryMappedFiles;

namespace 平行處理
{
    public class MemoryMappedCsvReader
    {
        private readonly string _filePath;
        private readonly long _fileSize;
        private MemoryMappedFile _mmf;
        private MemoryMappedViewAccessor _accessor;

        public MemoryMappedCsvReader(string filePath)
        {
            _filePath = filePath;
            _fileSize = new FileInfo(filePath).Length;
            _mmf = MemoryMappedFile.CreateFromFile(filePath);
            _accessor = _mmf.CreateViewAccessor();
        }

        public string ReadAt(long position, int length)
        {
            byte[] buffer = new byte[length];
            _accessor.ReadArray(position, buffer, 0, length);
            return Encoding.UTF8.GetString(buffer);
        }

        public void Dispose()
        {
            _accessor?.Dispose();
            _mmf?.Dispose();
        }
    }
}
