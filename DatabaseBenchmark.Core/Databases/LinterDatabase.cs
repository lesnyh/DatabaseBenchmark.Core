using System;
using System.Collections.Generic;
using System.Data;
using System.Data.LinterClient;
using System.Xml.Serialization;
using STS.General.Generators;
using STS.General.SQL.Extensions;

namespace DatabaseBenchmark.Databases
{
    public class LinterDatabase : Database
    {
        private IDbConnection initConnection;
        private IDbConnection[] flowConnections;
        private IDbCommand[] flowCommands;

        public override IndexingTechnology IndexingTechnology
        {
            get { return IndexingTechnology.BTree; }
        }

        public LinterDatabase()
        {
            SyncRoot = new object();

            Name = "Linter";
            CollectionName = "table1";
            Category = "SQL";
            Description = "DBMS Linter SQL Server";
            Website = "http://linter.ru/en/";
            Color = System.Drawing.Color.Blue;

            Requirements = new string[]
            { 
                "System.Data.LinterClient.dll"
            };
        }

        public string GetDefaultConnectionString()
        {
            LinterDbConnectionStringBuilder builder = new LinterDbConnectionStringBuilder();
            builder.DataSource = "LOCAL";
            builder.UserID = "SYSTEM";
            builder.Password = "MANAGER";

            return builder.ConnectionString;
        }

        private LinterDbConnection GetConnection()
        {
            LinterDbConnection conn = new LinterDbConnection(ConnectionString);
            conn.Open();

            return conn;
        }

        public override void Init(int flowCount, long flowRecordCount)
        {
            ConnectionString = GetDefaultConnectionString();
            flowConnections = new IDbConnection[flowCount];
            flowCommands = new IDbCommand[flowCount];

            initConnection = GetConnection();
            initConnection.ExecuteNonQuery(CreateTableQuery(CollectionName));

            for (int i = 0; i < flowCount; i++)
            {
                LinterDbConnection connection = GetConnection();
                flowConnections[i] = connection;

                if (connection.State == ConnectionState.Closed)
                    connection.Open();

                flowCommands[i] = CreateCommand(connection);
            }
        }

        public override void Write(int flowID, IEnumerable<KeyValuePair<long, Tick>> flow)
        {
            lock (SyncRoot)
            {
                IDbCommand command = flowCommands[flowID];

                foreach (KeyValuePair<long, Tick> kv in flow)
                {
                    long key = kv.Key;
                    Tick rec = kv.Value;

                    ((IDbDataParameter)command.Parameters[0]).Value = key;
                    ((IDbDataParameter)command.Parameters[1]).Value = rec.Symbol;
                    ((IDbDataParameter)command.Parameters[2]).Value = rec.Timestamp;
                    ((IDbDataParameter)command.Parameters[3]).Value = rec.Bid;
                    ((IDbDataParameter)command.Parameters[4]).Value = rec.Ask;
                    ((IDbDataParameter)command.Parameters[5]).Value = rec.BidSize;
                    ((IDbDataParameter)command.Parameters[6]).Value = rec.AskSize;
                    ((IDbDataParameter)command.Parameters[7]).Value = rec.Provider;

                    command.ExecuteNonQuery();
                }
            }
        }

        public override IEnumerable<KeyValuePair<long, Tick>> Read()
        {
            IDataReader reader = flowConnections[0].ExecuteQuery(String.Format(
                "SELECT * FROM {0} ORDER BY {1};", CollectionName, "ID"));

            foreach (IDataRecord row in reader.Forward())
            {
                long key = row.GetInt64(0);

                Tick tick = new Tick();
                tick.Symbol = row.GetString(1);
                tick.Timestamp = row.GetDateTime(2);
                tick.Bid = row.GetDouble(3);
                tick.Ask = row.GetDouble(4);
                tick.BidSize = row.GetInt32(5);
                tick.AskSize = row.GetInt32(6);
                tick.Provider = row.GetString(7);

                yield return new KeyValuePair<long, Tick>(key, tick);
            }

            reader.Close();
        }

        public override void Finish()
        {
            initConnection.Close();
            for (int i = 0; i < flowConnections.Length; i++)
            {
                flowConnections[i].Close();
            }
        }

        [XmlIgnore]
        public override Dictionary<string, string> Settings
        {
            get
            {
                return null;
            }
        }

        private IDbCommand CreateCommand(IDbConnection connection)
        {
            IDbCommand command = connection.CreateCommand();
            IDbDataParameter key = command.CreateParameter(":ID", DbType.Int64, 0);
            IDbDataParameter symbol = command.CreateParameter(":symbol", DbType.String, 255);
            IDbDataParameter time = command.CreateParameter(":time", DbType.DateTime, 0);
            IDbDataParameter bid = command.CreateParameter(":bid", DbType.Double, 0);
            IDbDataParameter ask = command.CreateParameter(":ask", DbType.Double, 0);
            IDbDataParameter bidSize = command.CreateParameter(":bidSize", DbType.Int32, 0);
            IDbDataParameter askSize = command.CreateParameter(":askSize", DbType.Int32, 0);
            IDbDataParameter provider = command.CreateParameter(":provider", DbType.String, 255);

            command.Parameters.Add(key);
            command.Parameters.Add(symbol);
            command.Parameters.Add(time);
            command.Parameters.Add(bid);
            command.Parameters.Add(ask);
            command.Parameters.Add(bidSize);
            command.Parameters.Add(askSize);
            command.Parameters.Add(provider);

            command.CommandType = CommandType.Text;
            command.CommandText = String.Format(@"
                Merge into {0}
                Using (select :ID (bigint) as id1, :symbol (varchar(255)), :time (date), :bid (double), :ask (double), :bidSize (int), :askSize (int), :provider (varchar(255))) as src
                On {0}.id=src.id1
                WHEN MATCHED THEN
                 UPDATE SET {0}.id=:ID, {0}.Symbol=:symbol, {0}.Time=:time, {0}.Bid=:bid, {0}.Ask=:ask, {0}.BidSize=:bidSize, {0}.AskSize=:askSize, {0}.Provider=:provider
                WHEN NOT MATCHED THEN
                 INSERT ({0}.id, {0}.Symbol, {0}.Time, {0}.Bid, {0}.Ask, {0}.BidSize, {0}.AskSize, {0}.Provider) VALUES (:ID, :symbol, :time, :bid, :ask, :bidSize, :askSize, :provider)",
                CollectionName);

            return command;
        }

        private string CreateTableQuery(string tableName)
        {
            return String.Format(
                "CREATE OR REPLACE TABLE {0} (", tableName) +
                "ID bigint primary key," +
                "Symbol varchar(255)," +
                "Time date," +
                "Bid double," +
                "Ask double," +
                "BidSize int," +
                "AskSize int," +
                "Provider varchar(255))";
        }
    }
}