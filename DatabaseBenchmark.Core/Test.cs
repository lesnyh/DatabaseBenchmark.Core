using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DatabaseBenchmark.Core.Statistics;

namespace DatabaseBenchmark.Core
{
    public abstract class Test : ITest
    {
        public IDatabase Database { get; protected set; }

        public virtual string Name { get; protected set; }
        public virtual string Description { get; protected set; }
        public virtual string Status { get; protected set; }

        public List<PerformanceWatch> Reports { get; protected set; }
        public PerformanceWatch ActiveReport { get; protected set; }

        public abstract void Start(CancellationToken cancellationToken);
    }
}
