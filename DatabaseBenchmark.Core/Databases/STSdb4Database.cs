using DatabaseBenchmark.Core;
using STS.General.Generators;
using STSdb4.Database;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Xml.Serialization;
using DatabaseBenchmark.Core.Attributes;
using System.Collections;
using System;

namespace DatabaseBenchmark.Databases
{
    public class STSdb4Database : Database
    {
        private IStorageEngine engine;
        private ITable<long, Tick> table;

        [Category("Settings")]
        [DbParameter]
        public int CacheSize { get; set; }

        [Category("Settings")]
        public bool InMemoryDatabase { get; set; }

        public override string IndexingTechnology
        {
            get { return "WaterfallTree"; }
        }
        public STSdb4Database()
        {
            SyncRoot = new object();

            Name = "STSdb 4.0";
            CollectionName = "table1";
            Category = @"NoSQL\Key-Value Store";
            Description = "STSdb 4.0";
            Website = "http://www.stsdb.com/";
            Color = Color.CornflowerBlue;

            Requirements = new string[] 
            {
                "STSdb4.dll"
            };

            CacheSize = 64;
        }

        public override void Open(int flowCount, long flowRecordCount)
        {
            engine = InMemoryDatabase ? STSdb4.Database.STSdb.FromMemory() : STSdb4.Database.STSdb.FromFile(Path.Combine(DataDirectory, "test.stsdb4"));
            ((StorageEngine)engine).CacheSize = CacheSize;

            table = engine.OpenXTable<long, Tick>(CollectionName);
        }

        public override void Write(int flowID, IEnumerable<KeyValuePair<long, Tick>> flow)
        {
            lock (SyncRoot)
            {
                foreach (var kv in flow)
                    table[kv.Key] = kv.Value;

                engine.Commit();
            }
        }

        public override IEnumerable<KeyValuePair<long, Tick>> Read()
        {
            return engine.OpenXTable<long, Tick>(CollectionName).Forward();
        }

        public override void Close()
        {
            engine.Close();
        }
    }

    public class Table : DatabaseBenchmark.Core.ITable<long, Tick>
    {
        private string name;
        private IDatabase database;
        private STSdb4.Database.ITable<long, Tick> table;

        public string Name
        {
            get { return name; }
        }

        public IDatabase Database
        {
            get { return database; }
        }

        public Table(string name)
        {
            this.name = name;
        }

        public void Write(IEnumerable<KeyValuePair<long, Tick>> records)
        {
            foreach (var record in records)
                table[record.Key] = record.Value;
        }

        public IEnumerable<KeyValuePair<long, Tick>> Read(long from, long to)
        {
            return table.Forward(from, true, to, true);
        }

        public Tick this[long key]
        {
            get
            {
                return table[key];
            }
            set
            {
                table[key] = value;
            }
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public IEnumerator<KeyValuePair<long, Tick>> GetEnumerator()
        {
            return table.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
