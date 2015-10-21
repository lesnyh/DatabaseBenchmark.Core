using log4net;
using System;
using DatabaseBenchmark.Core.Exceptions;
using DatabaseBenchmark.Core.Properties;
using System.Threading;
using System.Collections.Generic;
using System.Linq;

namespace DatabaseBenchmark.Core.Benchmarking
{
    /// <summary>
    /// Represents a benchmark test suite that executes all of the tests.
    /// </summary>
    public class BenchmarkSuite
    {
        public const int INTERVAL_COUNT = 100;

        private ILog Logger;

        public event Action<string, ITest> OnTestMethodCompleted;
        public event Action<Exception, ITest> OnException;

        public ITest CurrentTest { get; private set; }

        public BenchmarkSuite()
        {
            Logger = LogManager.GetLogger(Settings.Default.TestLogger);
        }

        public void ExecuteTests(long flowCount, long recordCount, float randomness, CancellationTokenSource token, params ITest[] tests)
        {
            foreach (var test in tests)
            {
                CurrentTest = test;

                CurrentTest.OnTestMethodCompleted += OnTestMethodCompleted;
                CurrentTest.OnException += OnException;

                CurrentTest.Start();
                CurrentTest.Stop();
            }

            CurrentTest = null;
        }

        /// <summary>
        /// Get moment speed entries of the current database in records/sec.
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
        /// Get average speed entries of the current database in records/sec.
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
        /// Gets  average memory working set entries in bytes.
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
    }
}
