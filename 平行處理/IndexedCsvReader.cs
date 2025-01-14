using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace 平行處理
{
    public class IndexedCsvReader
    {
        private readonly string _filePath;
        private readonly string _indexPath;

        public IndexedCsvReader(string filePath)
        {
            _filePath = filePath;
            _indexPath = filePath + ".idx";
            if (!File.Exists(_indexPath))
            {
                CreateIndex();
            }
        }

        private void CreateIndex()
        {
            using (var reader = new StreamReader(_filePath))
            using (var writer = new BinaryWriter(File.Open(_indexPath, FileMode.Create)))
            {
                long position = 0;
                writer.Write(position);  // 寫入第一行位置

                while (!reader.EndOfStream)
                {
                    char[] buffer = new char[4096];
                    int read = reader.Read(buffer, 0, buffer.Length);

                    for (int i = 0; i < read; i++)
                    {
                        if (buffer[i] == '\n')
                        {
                            position++;
                            writer.Write(position);
                        }
                        position++;
                    }
                }
            }
        }

        public string ReadLine(long lineNumber)
        {
            using (var indexReader = new BinaryReader(File.OpenRead(_indexPath)))
            using (var fileReader = new StreamReader(_filePath))
            {
                indexReader.BaseStream.Seek(lineNumber * 8, SeekOrigin.Begin);  // 8 bytes per long
                long position = indexReader.ReadInt64();

                fileReader.BaseStream.Seek(position, SeekOrigin.Begin);
                return fileReader.ReadLine();
            }
        }
    }
}
