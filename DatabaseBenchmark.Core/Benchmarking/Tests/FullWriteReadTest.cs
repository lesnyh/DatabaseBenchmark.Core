using DatabaseBenchmark.Core.Exceptions;
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

namespace DatabaseBenchmark.Core.Benchmarking.Tests
{
    public class FullWriteReadTest : ITest
    {
        private const int WRITE = 0;
        private const int READ = 1;
        private const int SECONDARY_READ = 2;
        private const int TESTS_COUNT = 3;

        private CancellationTokenSource Cancellation;
        private ILog Logger;
        private PerformanceWatch CurrentReport;

        #region ITest Members

        public event Action<ITest, string> OnTestMethodCompleted;
        public event Action<ITest, Exception> OnException;

        public string Name
        {
            get { return "Full Write/Read/Secondary Read Test"; }
        }

        public string Description
        {
            get { return "Performs full write and read test."; }
        }

        public string Status { get; }

        public IDatabase Database { get; }

        public List<PerformanceWatch> Reports { get; }

        public PerformanceWatch ActiveReport
        {
            get { return CurrentReport; }
        }

        #endregion

        public int FlowCount { get; private set; }
        public long RecordCount { get; private set; }

        public float Randomness { get; private set; }
        public KeysType KeysType { get; private set; }

        public long DatabaseSize { get; private set; }
        public long RecordsRead { get; private set; }

        public FullWriteReadTest(IDatabase database, int flowCount, long recordCount, float randomness, CancellationTokenSource cancellation)
        {
            FlowCount = flowCount;
            RecordCount = recordCount;
            Randomness = randomness;
            KeysType = Randomness > 0 ? KeysType.Random : KeysType.Sequential;

            Cancellation = cancellation;
            Database = database;

            Logger = LogManager.GetLogger(Settings.Default.TestLogger);

            int step = (int)((recordCount) / Benchmark.INTERVAL_COUNT);
            Reports = new List<PerformanceWatch>();

            Reports.Add(new PerformanceWatch("Full Write", step));
            Reports.Add(new PerformanceWatch("Full Read", step));
            Reports.Add(new PerformanceWatch("Full Secondary Read", step));
        }

        #region ITest Methods

        public void Start(CancellationTokenSource cancellationToken)
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
                if (OnException != null)
                    OnException(this, exc);
            }
        }

        #endregion

        #region Test Methods

        private void Init()
        {
            try
            {
                CurrentReport = Reports[WRITE];
                ActiveReport.Start();

                Database.Init(FlowCount, RecordCount);
            }
            catch (OperationCanceledException)
            {
                ActiveReport.Reset();
            }
            finally
            {
                ActiveReport.Stop();
                CurrentReport = null;
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

                CurrentReport = Reports[WRITE];
                CurrentReport.Start();

                tasks = DoWrite(flows);
                Task.WaitAll(tasks, Cancellation.Token);

                DatabaseSize = Database.Size;
            }
            catch(OperationCanceledException)
            {
                CurrentReport.Reset();
            }
            finally
            {
                CurrentReport.Stop();

                if (OnTestMethodCompleted != null)
                    OnTestMethodCompleted("Write", this);

                tasks = null;
                CurrentReport = null;
            }
        }

        private void Read()
        {
            Task task = null;

            try
            {
                CurrentReport = Reports[READ];
                CurrentReport.Start();

                task = DoRead(TestMethod.Read);
                task.Wait(Cancellation.Token);

                DatabaseSize = Database.Size;
            }
            catch (KeysNotOrderedException )
            {
                CurrentReport.Reset();
            }
            catch (OperationCanceledException)
            {
                CurrentReport.Reset();
            }
            finally
            {
                CurrentReport.Stop();

                if (OnTestMethodCompleted != null)
                    OnTestMethodCompleted("Read", this);

                task = null;
                CurrentReport = null;
            }
        }

        private void SecondaryRead()
        {
            Task task = null;

            try
            {
                CurrentReport = Reports[SECONDARY_READ];
                CurrentReport.Start();

                task = DoRead(TestMethod.Read);
                task.Wait(Cancellation.Token);

                DatabaseSize = Database.Size;
            }
            catch (KeysNotOrderedException)
            {
                CurrentReport.Reset();
            }
            catch (OperationCanceledException)
            {
                CurrentReport.Reset();
            }
            finally
            {
                CurrentReport.Stop();

                if (OnTestMethodCompleted != null)
                    OnTestMethodCompleted("Secondary Read", this);

                task = null;
                CurrentReport = null;
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

            CurrentReport = Reports[SECONDARY_READ];
            CurrentReport.Start();

            try
            {
                Database.Finish();
            }
            finally
            {
                CurrentReport.Stop();
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
        private IEnumerable<KeyValuePair<long, Tick>> Wrap(IEnumerable<KeyValuePair<long, Tick>> flow, IEnumerable<PerformanceWatch> statistics, CancellationToken token)
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
                    var flow = Wrap(flows[index], Reports, Cancellation.Token);

                    Database.Write(index, flow);

                }), i, Cancellation.Token, TaskCreationOptions.AttachedToParent | TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }

            return tasks;
        }

        private Task DoRead(TestMethod method)
        {
            Task task = Task.Factory.StartNew((Action<object>)((state) =>
            {
                int methodIndex = (int)state;
                var flow = Wrap(Database.Read(), Reports, Cancellation.Token);

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

            }), (int)method, Cancellation.Token, TaskCreationOptions.AttachedToParent | TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return task;
        }

        #endregion
    }
}
