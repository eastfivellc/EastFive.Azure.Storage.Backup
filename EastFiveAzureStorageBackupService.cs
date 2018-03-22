using log4net.Config;
using System;
using System.ServiceProcess;
using System.Timers;

namespace EastFive.Azure.Storage.Backup
{
    public partial class EastFiveAzureStorageBackupService : ServiceBase
    {
        public static readonly log4net.ILog Log;

        private readonly Timer timer;
        private readonly int pollSeconds;

        static EastFiveAzureStorageBackupService()
        {
            XmlConfigurator.Configure();
            Log = log4net.LogManager.GetLogger(typeof(EastFiveAzureStorageBackupService));
        }

        public EastFiveAzureStorageBackupService()
        {
            InitializeComponent();
            try
            {
                pollSeconds = 10;

                timer = new Timer();
                components.Add(timer);
            }
            catch (Exception e)
            {
                Log.Error("error during initialization", e);
                throw;
            }
        }
        protected override void OnStart(string[] args)
        {
            Log.Info("Service started");
            timer.Interval = pollSeconds * 1000;
            timer.AutoReset = true;
            timer.Elapsed += OnTimer;
            timer.Start();
        }

        protected override void OnStop()
        {
            Log.Info("Service stopping...");

            timer.Stop();

            Log.Info("Service stopped");
        }

        internal bool StopCalled()
        {
            return !timer.Enabled;
        }

        public void OnTimer(object sender, ElapsedEventArgs args)
        {
            Log.Info("Ping");
        }
    }
}
