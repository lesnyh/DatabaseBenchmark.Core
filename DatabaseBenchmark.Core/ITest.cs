using DatabaseBenchmark.Core.Statistics;
using System;
using System.Collections.Generic;
using System.Threading;

namespace DatabaseBenchmark.Core
{
    public interface ITest
    {
        event Action<ITest, Exception> OnException;

        string Name { get; }
        string Description { get; }
        string Status { get; }

        IDatabase Database { get; }

        List<PerformanceWatch> Reports { get; }
        PerformanceWatch ActiveReport { get; }

        void Start(CancellationTokenSource cancellationToken);
    }
}
