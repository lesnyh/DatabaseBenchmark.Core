using System;
using DatabaseBenchmark.Core;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseBenchmark.Databases
{
    public class MySQLTokuDBDatabase : MySQLDatabase
    {
        public MySQLTokuDBDatabase()
            :base(MySQLStorageEngine.TokuDB)
        {
        }

        public override string IndexingTechnology
        {
            get { return "FractalTree"; }
        }

    }
}
