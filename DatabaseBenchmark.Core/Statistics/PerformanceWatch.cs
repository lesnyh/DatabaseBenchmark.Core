using DatabaseBenchmark.Core.Benchmarking;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseBenchmark.Core.Statistics
{
    public class PerformanceWatch
    {
        public event Action<PerformanceWatch> OnStart;
        public event Action<PerformanceWatch> OnStop;

        public string Name { get; }

        public SpeedStatistics SpeedStatistics { get; set; }
        public MemoryStatistics MemoryStatistics { get; set; }

        public PerformanceWatch(string name, int step)
        {
            Name = name;

            SpeedStatistics = new SpeedStatistics(Benchmark.INTERVAL_COUNT, step);
            MemoryStatistics = new MemoryStatistics(Benchmark.INTERVAL_COUNT, step);
        }

        public PerformanceWatch()
            : this(String.Empty, 1)
        {
        }

        /// <summary>
        /// Start all statistics.
        /// </summary>
        public void Start()
        {
            SpeedStatistics.Start();
            MemoryStatistics.Start();

            if (OnStart != null)
                OnStart(this);
        }

        /// <summary>
        /// Stop all statistics.
        /// </summary>
        public void Stop()
        {
            SpeedStatistics.Stop();
            MemoryStatistics.Stop();

            if (OnStop != null)
                OnStop(this);
        }

        /// <summary>
        /// Reset all statistics.
        /// </summary>
        public void Reset()
        {
            SpeedStatistics.Reset();
            MemoryStatistics.Reset();
        }

        /// <summary>
        /// Add point to all statistics.
        /// </summary>
        public void Add()
        {
            SpeedStatistics.Add();
            MemoryStatistics.Add();
        }

        public TimeSpan GetElapsedTime()
        {
            return SpeedStatistics.Time;
        }

        public double GetAverageSpeed()
        {
            return SpeedStatistics.Speed;
        }

        public long GetRecordsCount()
        {
            return SpeedStatistics.Count;
        }

        public float GetPeakWorkingSet()
        {
            return MemoryStatistics.PeakWorkingSet;
        }
    }
}
