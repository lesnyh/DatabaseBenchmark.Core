using log4net;
using System;
using DatabaseBenchmark.Core.Exceptions;
using DatabaseBenchmark.Core.Properties;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using DatabaseBenchmark.Core.Statistics;

namespace DatabaseBenchmark.Core.Benchmarking
{
    /// <summary>
    /// Represents a benchmark that executes all of the tests.
    /// </summary>
    public class Benchmark
    {
        public const int INTERVAL_COUNT = 100;

        private ILog Logger;

        public event Action<PerformanceWatch> OnStart;
        public event Action<PerformanceWatch> OnStop;
        public event Action<ITest, Exception> OnException;
      
        public ITest CurrentTest { get; private set; }

        public Benchmark()
        {
            Logger = LogManager.GetLogger(Settings.Default.TestLogger);
        }

        public void ExecuteTests(CancellationTokenSource token, params ITest[] tests)
        {
            foreach (var test in tests)
            {
                CurrentTest = test;

                try
                {
                    CurrentTest.ActiveReport.OnStart += OnStart;
                    CurrentTest.ActiveReport.OnStart += LogOnStart;

                    CurrentTest.ActiveReport.OnStop += OnStop;
                    CurrentTest.ActiveReport.OnStop += LogOnStop;

                    CurrentTest.OnException += OnException;

                    CurrentTest.Start(token);
                }
                catch (Exception exc)
                {
                    Logger.Error("Test execution error...", exc);
                }
            }

            CurrentTest = null;
        }

        /// <summary>
        /// Get moment speed entries of the current database in records/sec for the current running test.
        /// </summary>
        public IEnumerable<KeyValuePair<long, double>> GetMomentSpeeds(int position)
        {
            lock (CurrentTest.ActiveReport)
            {
                var array = CurrentTest.ActiveReport.SpeedStatistics.RecordTime;
                var length = array.Length;

                if (position == 0)
                    position = 1;

                for (; position < length; position++)
                {
                    var records = array[position].Key;
                    var oldRecords = array[position - 1].Key;
                    var currentElapsed = array[position].Value.TotalSeconds;
                    var previousElapsed = array[position - 1].Value.TotalSeconds;

                    var speed = (records - oldRecords) / (currentElapsed - previousElapsed);

                    yield return new KeyValuePair<long, double>(records, speed);
                }
            }
        }

        /// <summary>
        /// Get average speed entries of the current database in records/sec for the current running test.
        /// </summary>
        public IEnumerable<KeyValuePair<long, double>> GetAverageSpeeds(int position)
        {
            lock (CurrentTest.ActiveReport)
            {
                var array = CurrentTest.ActiveReport.SpeedStatistics.RecordTime;
                var count = array.Length;

                if (position == 0)
                    position = 1;

                for (; position < count; position++)
                {
                    var records = array[position].Key;
                    var speed = (records / array[position].Value.TotalSeconds);

                    yield return new KeyValuePair<long, double>(records, speed);
                }
            }
        }

        /// <summary>
        /// Gets  average memory working set entries in bytes for the current running test.
        /// </summary>
        public IEnumerable<KeyValuePair<long, double>> GetMomentWorkingSets(int position)
        {
            lock (CurrentTest.ActiveReport)
            {
                var array = CurrentTest.ActiveReport.MemoryStatistics.MomentWorkingSetStats.ToArray();
                var length = array.Length;

                if (position == 0)
                    position = 1;

                for (; position < length; position++)
                {
                    var records = array[position].Key;
                    var workingSet = array[position].Value;

                    yield return new KeyValuePair<long, double>(records, workingSet);
                }
            }
        }

        private void LogOnStart(PerformanceWatch report)
        {
            Logger.Info(String.Format("{0} started.", report.Name));
        }

        private void LogOnStop(PerformanceWatch report)
        {
            Logger.Info(String.Format("{0} stopped.", report.Name));
        }
    }
}
