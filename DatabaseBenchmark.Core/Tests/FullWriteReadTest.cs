﻿using DatabaseBenchmark.Core.Exceptions;
using DatabaseBenchmark.Core.Properties;
using DatabaseBenchmark.Core.Statistics;
using log4net;
using STS.General.Generators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DatabaseBenchmark.Core.Tests
{
    public class FullWriteReadTest : Test
    {
        private const int WRITE = 0;
        private const int READ = 1;
        private const int SECONDARY_READ = 2;
        private const int TESTS_COUNT = 3;

        private ILog Logger;
        private CancellationToken Cancellation;
        private string TableName;

        #region ITest Members

        public override string Name
        {
            get { return "Full Write/Read/Secondary Read Test"; }
        }

        public override string Description
        {
            get { return "Performs full write and read test."; }
        }

        #endregion

        public int FlowCount { get; private set; }
        public long RecordCount { get; private set; }

        public float Randomness { get; private set; }

        public long DatabaseSize { get; private set; }
        public long RecordsRead { get; private set; }

        public FullWriteReadTest(IDatabase database, int flowCount, long recordCount, float randomness, CancellationToken cancellation)
        {
            FlowCount = flowCount;
            RecordCount = recordCount;
            Randomness = randomness;

            Cancellation = cancellation;
            Database = database;

            Logger = LogManager.GetLogger(Settings.Default.TestLogger);

            int step = (int)((recordCount) / Benchmark.INTERVAL_COUNT);
            Reports = new List<PerformanceReport>();

            Reports.Add(new PerformanceReport("Full Write", step));
            Reports.Add(new PerformanceReport("Full Read", step));
            Reports.Add(new PerformanceReport("Full Secondary Read", step));
        }

        public FullWriteReadTest()
        {
        }

        #region ITest Methods

        public override void Start(CancellationToken cancellationToken)
        {
            Cancellation = cancellationToken;

            try
            {
                Init();
                Write();
                Read();
                SecondaryRead();
            }
            catch (Exception exc)
            {
                Logger.Error("Test error...", exc);
            }
        }

        #endregion

        #region Test Methods

        private void Init()
        {
            try
            {
                ActiveReport = Reports[WRITE];
                ActiveReport.Start();

                Database.Open();

                Database.DeleteTable(TableName);
                Database.OpenOrCreateTable(TableName);
            }
            finally
            {
                ActiveReport.Stop();
                ActiveReport = null;
            }
        }

        private void Write()
        {
            Task[] tasks = null;

            try
            {
                IEnumerable<KeyValuePair<long, Tick>>[] flows = new IEnumerable<KeyValuePair<long, Tick>>[FlowCount];
                for (int k = 0; k < flows.Length; k++)
                    flows[k] = GetFlow();

                ActiveReport = Reports[WRITE];
                ActiveReport.Start();

                tasks = DoWrite(flows);
                Task.WaitAll(tasks, Cancellation);

                DatabaseSize = Database.Size;
            }
            finally
            {
                ActiveReport.Stop();

                tasks = null;
                ActiveReport = null;
            }
        }

        private void Read()
        {
            Task task = null;

            try
            {
                ActiveReport = Reports[READ];
                ActiveReport.Start();

                task = DoRead();
                task.Wait(Cancellation);

                DatabaseSize = Database.Size;
            }
            finally
            {
                ActiveReport.Stop();

                task = null;
                ActiveReport = null;
            }
        }

        private void SecondaryRead()
        {
            Task task = null;

            try
            {
                ActiveReport = Reports[SECONDARY_READ];
                ActiveReport.Start();

                task = DoRead();
                task.Wait(Cancellation);

                DatabaseSize = Database.Size;
            }
            finally
            {
                ActiveReport.Stop();

                task = null;
                ActiveReport = null;
            }
        }

        public void Finish()
        {
            if (!Cancellation.IsCancellationRequested)
                DatabaseSize = Database.Size;
            else
            {
                DatabaseSize = 0;
                return;
            }

            ActiveReport = Reports[SECONDARY_READ];
            ActiveReport.Start();

            try
            {
                Database.Close();
            }
            finally
            {
                ActiveReport.Stop();
            }
        }

        #endregion

        #region Helper Methods

        private IEnumerable<KeyValuePair<long, Tick>> GetFlow()
        {
            Random random1 = new Random();
            Random random2 = new Random();

            SemiRandomGenerator generator = new SemiRandomGenerator(random1.Next(), random2.Next(), Randomness);

            return TicksGenerator.GetFlow(RecordCount, generator);
        }

        /// <summary>
        /// Wraps a data flow to check cancellation token and accumulate some statistic.
        /// </summary>
        private IEnumerable<KeyValuePair<long, Tick>> Wrap(IEnumerable<KeyValuePair<long, Tick>> flow, IEnumerable<PerformanceReport> statistics, CancellationToken token)
        {
            foreach (var kv in flow)
            {
                if (token.IsCancellationRequested)
                    yield break;

                yield return kv;

                lock (statistics)
                {
                    foreach (var item in statistics)
                        item.Add();
                }
            }
        }

        private Task[] DoWrite(IEnumerable<KeyValuePair<long, Tick>>[] flows)
        {
            Task[] tasks = new Task[flows.Length];

            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Factory.StartNew((Action<object>)((state) =>
                {
                    int index = (int)state;
                    var flow = Wrap(flows[index], Reports, Cancellation);

                    Database.Write(index, flow);

                }), i, Cancellation, TaskCreationOptions.AttachedToParent | TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }

            return tasks;
        }

        private Task DoRead()
        {
            Task task = Task.Factory.StartNew((Action)(() =>
            {
                var flow = Wrap(Database.Read(), Reports, Cancellation);

                long count = 0;
                RecordsRead = 0;

                long previous = long.MinValue;

                foreach (var kv in flow)
                {
                    var key = kv.Key;

                    if (previous > key)
                        throw new KeysNotOrderedException("Keys are not ordered.");

                    previous = key;
                    count++;
                }

                RecordsRead = count;

            }) , Cancellation, TaskCreationOptions.AttachedToParent | TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return task;
        }

        #endregion
    }
}
