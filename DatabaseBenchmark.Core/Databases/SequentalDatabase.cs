using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DatabaseBenchmark.Core;
using STS.General.Generators;
using STS.General.Data;
using System.IO;
using STS.General.IO;
using System.Drawing;

namespace DatabaseBenchmark.Databases
{
    public class SequentalDatabase : Database
    {
        Persist<KeyValuePair<long, Tick>> Persist;
        Stream Stream;

        private string dbInstanceName;

        public SequentalDatabase()
        {
            Name = "Sequental Database";
            CollectionName = "Database.sequental";
            Category = @"NoSQL\Key-Value Store\";
            Description = "";
            Website = "";
            Color = Color.CadetBlue;

            Requirements = new string[]
            {
                "STS.General.dll"
            };
        }

        public override void Init(int flowCount, long flowRecordCount)
        {
            Persist = new Persist<KeyValuePair<long, Tick>>();
            dbInstanceName = Path.Combine(DataDirectory, CollectionName);

            if (File.Exists(dbInstanceName))
                File.Delete(dbInstanceName);
  
        }

        public override void Write(int flowID, IEnumerable<KeyValuePair<long, Tick>> flow)
        {
            Stream = new OptimizedFileStream(dbInstanceName, FileMode.Create);
            BinaryWriter writer = new BinaryWriter(Stream);
            
            foreach (var kv in flow)
                Persist.Write(writer, kv);

            writer.Close();
        }

        public override IEnumerable<KeyValuePair<long, Tick>> Read()
        {
            Stream = new OptimizedFileStream(dbInstanceName, FileMode.Open);

            using (BinaryReader reader = new BinaryReader(Stream))
            {   
                while(reader.BaseStream.Position < reader.BaseStream.Length)
                yield return Persist.Read(reader);
            }
        }

        public override void Finish()
        {

        }
    }
}
