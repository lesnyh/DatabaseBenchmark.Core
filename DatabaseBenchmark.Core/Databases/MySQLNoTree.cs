using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using STS.General.Generators;
using STS.General.SQL.Extensions;
using DatabaseBenchmark.Core.Attributes;

namespace DatabaseBenchmark.Databases
{
    public class MySQLNoTree : MySQLDatabase
    {
        public MySQLNoTree()
            : base(MySQLStorageEngine.NoTree)
        {
        }

        protected override string CreateTableQuery()
        {
            string query = String.Format("CREATE TABLE `{0}` (", CollectionName) +
                           "`ID` bigint(20) primary key, " +
                           "`Symbol` varchar(255) NOT NULL," +
                           "`Time` datetime NOT NULL," +
                           "`Bid` double NOT NULL," +
                           "`Ask` double NOT NULL," +
                           "`BidSize` int(20) NOT NULL," +
                           "`AskSize` int(20) NOT NULL," +
                           "`Provider` varchar(255) NOT NULL)" +
                           "engine = notree";

            return query;
        }


        public override IEnumerable<KeyValuePair<long, Tick>> Read()
        {
            IDataReader reader = connections.First().ExecuteQuery(String.Format("SELECT * FROM {0};", CollectionName));

            foreach (var row in reader.Forward())
            {
                long key = row.GetInt64(0);

                Tick tick = new Tick
                {
                    Symbol = row.GetString(1),
                    Timestamp = row.GetDateTime(2),
                    Bid = row.GetDouble(3),
                    Ask = row.GetDouble(4),
                    BidSize = row.GetInt32(5),
                    AskSize = row.GetInt32(6),
                    Provider = row.GetString(7)
                };

                yield return new KeyValuePair<long, Tick>(key, tick);
            }
        }



        public override string IndexingTechnology
        {
            get { return "NoTree"; }
        }
    }
}
