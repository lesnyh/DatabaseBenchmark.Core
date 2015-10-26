using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseBenchmark.Core
{
    public interface ITable<TKey, TRecord> : IEnumerable<KeyValuePair<TKey, TRecord>>
    {
        string Name { get; }
        IDatabase Database { get; }

        void Write(IEnumerable<KeyValuePair<TKey, TRecord>> records);
        IEnumerable<KeyValuePair<TKey, TRecord>> Read(TKey from, TKey to);

        TRecord this[TKey key] { get; set; }
        
        void Close();
    }
}
