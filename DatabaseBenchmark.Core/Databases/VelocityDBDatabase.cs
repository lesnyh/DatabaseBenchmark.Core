﻿//using STS.General.Generators;
//using System;
//using DatabaseBenchmark.Core;
//using System.Collections.Generic;
//using System.Drawing;
//using System.Xml.Serialization;
//using VelocityDb;
//using VelocityDb.Collection.BTree;
//using VelocityDb.Session;
//using DatabaseBenchmark.Core.Attributes;

//namespace DatabaseBenchmark.Databases
//{
//    public class VelocityDBDatabase : Core.Database
//    {
//        protected UInt64 mapId;
//        private SessionNoServerShared session;
        
//        public override string IndexingTechnology
//        {
//            get { return "BTree"; }
//        }

//        public VelocityDBDatabase()
//        {
//            Name = "VelocityDB";
//            Category = "NoSQL\\Object Databases";
//            Description = "VelocityDB Standalone Client 3.2 Dec 8, 2013";
//            Website = "http://www.velocitydb.com/";
//            Color = Color.FromArgb(141, 186, 137);

//            Requirements = new string[]
//            { 
//                "VelocityDB.dll"
//            };
//        }

//        public override void Open(int flowCount, long flowRecordCount)
//        {
//            mapId = new UInt64();
//            session = new SessionNoServerShared(DataDirectory);

//            session.BeginUpdate();
//            session.DefaultDatabaseLocation().CompressPages = false;
//            session.RegisterClass(typeof(VelocityTick));
//            session.RegisterClass(typeof(BTreeMapOidShort<long, VelocityTick>));
//        }

//        public override void Write(int flowID, IEnumerable<KeyValuePair<long, Tick>> flow)
//        {
//            Placement place = new Placement((ushort)checked(flowID + 10));
//            Placement velocityRecordPlace = new Placement((ushort)(flowID + 10), 1000, 1, 10000, ushort.MaxValue, false, false, UInt32.MaxValue, false);

//            BTreeMapOidShort<long, VelocityTick> map = new BTreeMapOidShort<long, VelocityTick>(null, session, 1000);

//            mapId = map.Persist(place, session);
//            map.ValuePlacement = velocityRecordPlace;
//            map.TransientBatchSize = 2000;

//            foreach (var kv in flow)
//            {
//                VelocityTick tick = new VelocityTick(kv.Value);
//                map.AddFast(kv.Key, tick);
//            }
//        }

//        public override IEnumerable<KeyValuePair<long, Tick>> Read()
//        {
//            BTreeMapOidShort<long, VelocityTick> map = (BTreeMapOidShort<long, VelocityTick>)session.Open(mapId);

//            foreach (KeyValuePair<long, VelocityTick> pair in map)
//            {
//                yield return new KeyValuePair<long, Tick>(pair.Key, pair.Value.Record);
//            }
//        }

//        public override void Close()
//        {
//            if (session.InTransaction)
//                session.Commit();
//        }

//        public class VelocityTick : OptimizedPersistable
//        {
//            string symbol;
//            DateTime timestamp;
//            double bid;
//            double ask;
//            int bidSize;
//            int askSize;
//            string provider;

//            public VelocityTick(Tick tick)
//            {
//                symbol = tick.Symbol;
//                timestamp = tick.Timestamp;
//                bid = tick.Bid;
//                ask = tick.Ask;
//                askSize = tick.AskSize;
//                bidSize = tick.BidSize;
//                provider = tick.Provider;
//            }

//            public Tick Record
//            {
//                get { return new Tick(symbol, timestamp, bid, ask, bidSize, askSize, provider); }
//            }

//            public override bool AllowOtherTypesOnSamePage
//            {
//                get { return false; }
//            }
//        }
//    }
//}