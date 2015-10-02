﻿using STS.General.Generators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DatabaseBenchmark.Core.Exceptions;
using DatabaseBenchmark.Core.Statistics;

namespace DatabaseBenchmark.Core.Benchmarking
{
    /// <summary>
    /// Represents a benchmark test session for a single database.
    /// </summary>
    public class BenchmarkSession
    {
        public const int INTERVAL_COUNT = 100; // Gives the maximum number of intervals measured by the statistic.

        public SpeedStatistics[] SpeedStatistics { get; private set; }
        public MemoryStatistics[] MemoryStatistics { get; private set; }

        public TestMethod CurrentMethod { get; private set; }
        public Database Database { get; private set; }

        public long RecordCount { get; private set; }
        public int FlowCount { get; private set; }

        public float Randomness { get; private set; }
        public KeysType KeysType { get; private set; }

        public long DatabaseSize { get; private set; }
        public long RecordsRead { get; private set; }

        public DateTime StartTime { get; private set; }
        public DateTime EndTime { get; private set; }

        private CancellationTokenSource Cancellation;

        public BenchmarkSession(Database database, int flowCount, long recordCount, float randomness, CancellationTokenSource cancellation)
        {
            Database = database;

            FlowCount = flowCount;
            RecordCount = recordCount;
            Randomness = randomness;
            KeysType = Randomness == 0f ? KeysType.Sequential : KeysType.Random;

            // Statistics.
            int length = Enum.GetValues(typeof(TestMethod)).Length - 1;

            SpeedStatistics = new SpeedStatistics[length];
            MemoryStatistics = new MemoryStatistics[length];

            int step = (int)((recordCount) / INTERVAL_COUNT);

            for (int i = 0; i < length; i++)
            {
                SpeedStatistics[i] = new SpeedStatistics(INTERVAL_COUNT);
                SpeedStatistics[i].Step = step;

                MemoryStatistics[i] = new MemoryStatistics(INTERVAL_COUNT);
                MemoryStatistics[i].Step = step;
            }

            Cancellation = cancellation;
        }

        private void StartStatistics(int method)
        {
            SpeedStatistics[method].Start();
            MemoryStatistics[method].Start();
        }

        private void StopStatistics(int method)
        {
            SpeedStatistics[method].Stop();
            MemoryStatistics[method].Stop();
        }

        private void ResetStatistics()
        {
            int length = Enum.GetValues(typeof(TestMethod)).Length - 1;

            for (int i = 0; i < length; i++)
            {
                SpeedStatistics[i].Reset();
                MemoryStatistics[i].Reset();
            }
        }

        #region Test Methods

        public void Init()
        {
            StartTime = DateTime.Now;

            int method = (int)TestMethod.Write;

            try
            {
                StartStatistics(method);

                Database.Init(FlowCount, RecordCount);
            }
            catch (OperationCanceledException)
            {
                ResetStatistics();
            }
            finally
            {
                StopStatistics(method);
            }
        }

        /// <summary>
        /// Execute a write test into the database.
        /// </summary>
        public void Write()
        {
            if (Cancellation.IsCancellationRequested)
                return;

            CurrentMethod = TestMethod.Write;
            int method = (int)CurrentMethod;

            IEnumerable<KeyValuePair<long, Tick>>[] flows = new IEnumerable<KeyValuePair<long, Tick>>[FlowCount];
            for (int k = 0; k < flows.Length; k++)
                flows[k] = GetFlow();

            Task[] tasks = null;

            try
            {
                StartStatistics(method);

                tasks = DoWrite(flows);
                Task.WaitAll(tasks, Cancellation.Token);

                DatabaseSize = Database.Size;
            }
            catch (OperationCanceledException)
            {
                ResetStatistics();
            }
            finally
            {
                CurrentMethod = TestMethod.None;

                StopStatistics(method);
                tasks = null;
            }
        }

        /// <summary>
        /// Execute a read test from the database.
        /// </summary>
        public void Read()
        {
            if (Cancellation.IsCancellationRequested)
                return;

            CurrentMethod = TestMethod.Read;
            int method = (int)CurrentMethod;

            Task task = null;

            try
            {
                StartStatistics(method);

                task = DoRead(TestMethod.Read);
                task.Wait(Cancellation.Token);

                DatabaseSize = Database.Size;
            }
            catch (KeysNotOrderedException)
            {
                ResetStatistics();
            }
            catch (OperationCanceledException)
            {
                ResetStatistics();
            }
            finally
            {
                CurrentMethod = TestMethod.None;

                StopStatistics(method);
                task = null;
            }
        }

        /// <summary>
        /// Execute a secondary read from the database.
        /// </summary>
        public void SecondaryRead()
        {
            if (Cancellation.IsCancellationRequested)
                return;

            CurrentMethod = TestMethod.SecondaryRead;
            int method = (int)CurrentMethod;

            Task task = null;

            try
            {
                StartStatistics(method);

                task = DoRead(TestMethod.SecondaryRead);
                Task.WaitAll(new Task[] { task }, Cancellation.Token);

                DatabaseSize = Database.Size;
            }
            catch (KeysNotOrderedException)
            {
                ResetStatistics();
            }
            catch (OperationCanceledException)
            {
                ResetStatistics();
            }
            finally
            {
                CurrentMethod = TestMethod.None;

                StopStatistics(method);
                task = null;
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

            int method = (int)TestMethod.SecondaryRead;

            StartStatistics(method);

            try
            {
                Database.Finish();
                EndTime = DateTime.Now;
            }
            finally
            {
                StopStatistics(method);
            }
        }

        #endregion

        #region Statistics

        public TimeSpan GetElapsedTime(TestMethod method)
        {
            lock (SpeedStatistics)
            {
                return SpeedStatistics[(int)method].Time;
            }
        }

        public double GetAverageSpeed(TestMethod method)
        {
            lock (SpeedStatistics)
            {
                return SpeedStatistics[(int)method].Speed;
            }
        }

        public long GetRecords(TestMethod method)
        {
            lock (SpeedStatistics)
            {
                return SpeedStatistics[(int)method].Count;
            }
        }

        public float GetPeakWorkingSet(TestMethod method)
        {
            lock (MemoryStatistics)
            {
                return MemoryStatistics[(int)method].PeakWorkingSet;
            }
        }

        /// <summary>
        /// Get average speed entries of the current database in records/sec.
        /// </summary>
        public IEnumerable<KeyValuePair<long, double>> GetAverageSpeeds(TestMethod method, int position)
        {
            lock (SpeedStatistics)
            {
                var array = SpeedStatistics[(int)method].RecordTime;
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
        /// Get moment speed entries of the current database in records/sec.
        /// </summary>
        public IEnumerable<KeyValuePair<long, double>> GetMomentSpeeds(TestMethod method, int position)
        {
            lock (SpeedStatistics)
            {
                var array = SpeedStatistics[(int)method].RecordTime;
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
        /// Gets  average memory working set entries in bytes.
        /// </summary>
        public IEnumerable<KeyValuePair<long, double>> GetMomentWorkingSets(TestMethod method, int position)
        {
            lock (MemoryStatistics)
            {
                var array = MemoryStatistics[(int)method].MomentWorkingSetStats.ToArray();
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

        #endregion

        private IEnumerable<KeyValuePair<long, Tick>> GetFlow()
        {
            Random random1 = new Random();
            Random random2 = new Random();

            SemiRandomGenerator generator = new SemiRandomGenerator(random1.Next(), random2.Next(), Randomness);

            return TicksGenerator.GetFlow(RecordCount, generator);
        }

        #region Database Methods

        /// <summary>
        /// Wraps a data flow to check cancellation token and accumulate some statistic.
        /// </summary>
        private IEnumerable<KeyValuePair<long, Tick>> Wrap(IEnumerable<KeyValuePair<long, Tick>> flow, CancellationToken token, params IStatistic[] statistics)
        {
            foreach (var kv in flow)
            {
                if (token.IsCancellationRequested)
                    yield break;

                yield return kv;

                for (int i = 0; i < statistics.Length; i++)
                {
                    lock (statistics)
                        statistics[i].Add();
                }
            }
        }

        private Task[] DoWrite(IEnumerable<KeyValuePair<long, Tick>>[] flows)
        {
            Task[] tasks = new Task[flows.Length];

            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Factory.StartNew((state) =>
                {
                    int index = (int)state;
                    int method = (int)TestMethod.Write;
                    var flow = Wrap(flows[index], Cancellation.Token, SpeedStatistics[method], MemoryStatistics[method]);

                    Database.Write(index, flow);

                }, i, Cancellation.Token, TaskCreationOptions.AttachedToParent | TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }

            return tasks;
        }

        private Task DoRead(TestMethod method)
        {
            Task task = Task.Factory.StartNew((state) =>
            {
                int methodIndex = (int)state;
                var flow = Wrap(Database.Read(), Cancellation.Token, SpeedStatistics[methodIndex], MemoryStatistics[methodIndex]);

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

            }, (int)method, Cancellation.Token, TaskCreationOptions.AttachedToParent | TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return task;
        }

        #endregion
    }
}
