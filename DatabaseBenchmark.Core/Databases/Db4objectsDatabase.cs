﻿using DatabaseBenchmark.Core;
using Db4objects.Db4o;
using Db4objects.Db4o.Config;
using Db4objects.Db4o.Query;
using STS.General.Generators;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Xml.Serialization;
using DatabaseBenchmark.Core.Attributes;

namespace DatabaseBenchmark.Databases
{
    public class Db4objectsDatabase : Database
    {
        private IObjectContainer[] sessions;
        private IEmbeddedObjectContainer database;
        private string fileName;

        public override string IndexingTechnology
        {
            get { return "BTree"; }
        }

        public Db4objectsDatabase()
        {
            Name = "Db4objects";
            CollectionName = "database";
            Category = "NoSQL\\Object Databases";
            Description = "Db4objects 8.0";
            Website = "http://www.db4o.com/";
            Color = Color.FromArgb(166, 149, 134);

            Requirements = new string[]
            { 
                "Db4objects.Db4o.dll" 
            };
        }

        public override void Init(int flowCount, long flowRecordCount)
        {
            sessions = new IObjectContainer[flowCount];
            fileName = Path.Combine(DataDirectory, CollectionName + ".db4objects");

            IEmbeddedConfiguration config = Db4oEmbedded.NewConfiguration();
            config.Common.ObjectClass(typeof(long)).ObjectField("Key").Indexed(true);
            database = Db4oEmbedded.OpenFile(config, fileName);

            for (int i = 0; i < flowCount; i++)
            {
                IObjectContainer obj = database.Ext().OpenSession();
                sessions[i] = obj;
            }
        }

        public override void Write(int flowID, IEnumerable<KeyValuePair<long, Tick>> flow)
        {
            using (sessions[flowID])
            {
                foreach (var kv in flow)
                    sessions[flowID].Store(new Row<long, Tick>(kv.Key, kv.Value));

                sessions[flowID].Commit();
            }
        }

        public override IEnumerable<KeyValuePair<long, Tick>> Read()
        {
            IQuery query = database.Ext().OpenSession().Query();

            query.Constrain(typeof(Row<long, Tick>));
            query.Descend("Key").OrderAscending();

            foreach (Row<long, Tick> row in query.Execute())
                yield return new KeyValuePair<long, Tick>(row.Key, row.Record);
        }

        public override void Finish()
        {
            database.Close();
        }

        private class Row<TKey, TRecord>
        {
            public TKey Key;
            public TRecord Record;

            public Row() { }

            public Row(TKey key, TRecord record)
            {
                Key = key;
                Record = record;
            }
        }
    }
}