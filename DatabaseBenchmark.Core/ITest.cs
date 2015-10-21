using DatabaseBenchmark.Core.Statistics;
using System;
using System.Collections.Generic;

namespace DatabaseBenchmark.Core
{
    public interface ITest
    {
        event Action<string, ITest> OnTestMethodCompleted;
        event Action<Exception, ITest> OnException;

        string Name { get; }
        string Description { get; }
        string Status { get; }

        DateTime StartTime { get; }
        DateTime EndTime { get; }

        IDatabase Database { get; }

        List<PerformanceWatch> Reports { get; }
        PerformanceWatch ActiveReport { get; set; }

        void Start();
        void Stop();
    }
}
