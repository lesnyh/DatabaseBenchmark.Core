using log4net;
using System;
using DatabaseBenchmark.Core.Exceptions;
using DatabaseBenchmark.Core.Properties;

namespace DatabaseBenchmark.Core.Benchmarking
{
    /// <summary>
    /// Represents a benchmark test suite that executes all of the tests.
    /// </summary>
    public class BenchmarkSuite
    {
        private ILog Logger;

        public event Action<BenchmarkSession, TestMethod> OnTestMethodCompleted;
        public event Action<Exception, BenchmarkSession> OnException;

        public BenchmarkSession CurrentTest { get; private set; }

        public BenchmarkSuite()
        {
            Logger = LogManager.GetLogger(Settings.Default.TestLogger);
        }

        public void ExecuteInit(BenchmarkSession test)
        {
            CurrentTest = test;
            string databaseName = test.Database.Name;

            try
            {
                Logger.Info("");
                Logger.Info(String.Format("{0} Init() started...", databaseName));

                CurrentTest.Init();

                Logger.Info(String.Format("{0} Init() ended...", databaseName));
            }
            catch (Exception exc)
            {
                Logger.Error(String.Format("{0} Init() failed...", databaseName));
                Logger.Error(String.Format("{0} Init()", databaseName), exc);

                if(OnException != null)
                    OnException(exc, test);
            }
            finally
            {
                CurrentTest = null;
            }
        }

        public void ExecuteWrite(BenchmarkSession test)
        {
            CurrentTest = test;
            string databaseName = test.Database.Name;

            try
            {
                Logger.Info(String.Format("{0} Write() started...", databaseName));
                CurrentTest.Write();
                Logger.Info(String.Format("{0} Write() ended...", databaseName));
            }
            catch (Exception exc)
            {
                Logger.Error(String.Format("{0} Write() failed...", databaseName));
                Logger.Error(String.Format("{0} Write()", databaseName), exc);

                if (OnException != null)
                    OnException(exc, test);
            }
            finally
            {
                CurrentTest = null;

                if(OnTestMethodCompleted != null)
                    OnTestMethodCompleted(test, TestMethod.Write);
            }
        }

        public void ExecuteRead(BenchmarkSession test)
        {
            CurrentTest = test;
            string databaseName = test.Database.Name;

            try
            {
                Logger.Info(String.Format("{0} Read() started...", databaseName));

                CurrentTest.Read();

                Logger.Info(String.Format("Records read: {0}", test.RecordsRead.ToString("N0")));
                Logger.Info(String.Format("{0} Read() ended...", databaseName));
            }
            catch(KeysNotOrderedException exc)
            {
                Logger.Error(String.Format("{0} The database does not return the records ordered by key. The test is invalid!...", databaseName));
                Logger.Error(String.Format("{0} {1}", databaseName, exc.Message));

                if (OnException != null)
                    OnException(exc, test);
            }
            catch (Exception exc)
            {
                Logger.Error(String.Format("{0} Read() failed...", databaseName));
                Logger.Error(String.Format("{0} Read()", databaseName), exc);

                if (OnException != null)
                    OnException(exc, test);
            }
            finally
            {
                CurrentTest = null;

                if(OnTestMethodCompleted != null)
                    OnTestMethodCompleted(test, TestMethod.Read);
            }
        }

        public void ExecuteSecondaryRead(BenchmarkSession test)
        {
            CurrentTest = test;
            string databaseName = test.Database.Name;

            try
            {
                Logger.Info(String.Format("{0} SecondaryRead() started...", databaseName));

                CurrentTest.SecondaryRead();

                Logger.Info(String.Format("Records read: {0}", test.RecordsRead.ToString("N0")));
                Logger.Info(String.Format("{0} SecondaryRead() ended...", databaseName));
            }
            catch (KeysNotOrderedException exc)
            {
                Logger.Error(String.Format("{0} The database does not return the records ordered by key. The test is invalid!...", databaseName));
                Logger.Error(String.Format("{0} Read()", databaseName), exc);

                if (OnException != null)
                    OnException(exc, test);
            }
            catch (Exception exc)
            {
                Logger.Error(String.Format("{0} Secondary Read failed...", databaseName));
                Logger.Error(String.Format("{0} Secondary Read()", databaseName), exc);

                if (OnException != null)
                    OnException(exc, test);
            }
            finally
            {
                CurrentTest = null;

                if(OnTestMethodCompleted != null)
                    OnTestMethodCompleted(test, TestMethod.SecondaryRead);
            }
        }

        public void ExecuteFinish(BenchmarkSession test)
        {
            CurrentTest = test;

            try
            {
                CurrentTest.Finish();
            }
            catch (Exception exc)
            {
                Logger.Error(String.Format("{0} Finish() failed...", test.Database.Name));
                Logger.Error(String.Format("{0} Finish()", test.Database.Name), exc);

                if (OnException != null)
                    OnException(exc, test);
            }
            finally
            {
                CurrentTest = null;
            }
        }
    }
}
