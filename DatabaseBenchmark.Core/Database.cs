using STS.General.Generators;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Xml.Serialization;

namespace DatabaseBenchmark.Core
{
    public abstract class Database : IDatabase
    {
        protected object SyncRoot { get; set; }

        public string Name { get; set; }
        public string Category { get; set; }
        public string Description { get; set; }
        public string Website { get; set; }
        public string[] Requirements { get; set; }
        public string CollectionName { get; set; }
        public string DataDirectory { get; set; }
        public string ConnectionString { get; set; }

        [XmlIgnore]
        public Color Color { get; set; }
        
        public abstract void Open(int flowCount, long flowRecordCount);
        public abstract void Write(int flowID, IEnumerable<KeyValuePair<long, Tick>> flow);
        public abstract IEnumerable<KeyValuePair<long, Tick>> Read();
        public abstract void Close();
        
        public virtual string IndexingTechnology
        {
            get { return "None"; }
        }

        [Browsable(false)]
        public virtual long Size
        {
            get { return Directory.GetFiles(DataDirectory, "*.*", SearchOption.AllDirectories).Sum(x => (new FileInfo(x)).Length); }
        }


        public ITable<long, Tick>[] Tables
        {
            get { throw new System.NotImplementedException(); }
        }

        public void Open()
        {
            throw new System.NotImplementedException();
        }

        public ITable<long, Tick> OpenOrCreateTable(string name)
        {
            throw new System.NotImplementedException();
        }

        public void DeleteTable(string name)
        {
            throw new System.NotImplementedException();
        }
    }
}
