using BlackBarLabs;
using BlackBarLabs.Extensions;
using EastFive.Azure.Storage.Backup.Blob;
using EastFive.Azure.Storage.Backup.Configuration;
using log4net.Config;
using Microsoft.WindowsAzure.Storage;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace EastFive.Azure.Storage.Backup
{
    public partial class EastFiveAzureStorageBackupService : ServiceBase
    {
        public static readonly log4net.ILog Log;

        private readonly System.Timers.Timer timer;
        private readonly TimeSpan wakeUp;
        private readonly string configFile;
        private readonly BackupSettingsLoader loader;
        private readonly FileSystemWatcher watcher;
        private readonly StatusManager manager;

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
                this.wakeUp = Web.Configuration.Settings.GetString(AppSettings.WakeUp,
                    wakeUp => TimeSpan.Parse(wakeUp),
                    why => TimeSpan.FromMinutes(30));

                this.configFile = Web.Configuration.Settings.GetString(AppSettings.ConfigFile,
                    configFile => configFile,
                    why => "backup.json");

                var dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                this.loader = new BackupSettingsLoader(Path.Combine(dir, configFile));
                if (!string.IsNullOrEmpty(this.loader.Error))
                    throw new ArgumentException(this.loader.Error);

                watcher = new FileSystemWatcher(dir, this.configFile);
                watcher.NotifyFilter = NotifyFilters.LastWrite;
                watcher.Changed += loader.OnChanged;
                watcher.EnableRaisingEvents = true;
                components.Add(watcher);

                this.manager = new StatusManager(dir, this.loader);

                timer = new System.Timers.Timer();
                components.Add(timer);

                ServicePointManager.UseNagleAlgorithm = false;
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
            timer.Interval = wakeUp.TotalMilliseconds;
            timer.AutoReset = true;
            timer.Elapsed += OnTimer;
            timer.Start();
        }

        protected override void OnStop()
        {
            Log.Info("Service stopping...");

            timer.Stop();
            if (manager.Status.running.Any())
            {
                var maxWait = DateTime.UtcNow + TimeSpan.FromMinutes(5);
                do
                {
                    Thread.Sleep((int)TimeSpan.FromSeconds(30).TotalMilliseconds);
                    if (!manager.Status.running.Any())
                        break;
                    Log.Info("work still running, waiting...");
                } while (DateTime.UtcNow < maxWait);
            }

            Log.Info("Service stopped");
        }

        private bool StopCalled()
        {
            return !timer.Enabled;
        }

        public void OnTimer(object sender, ElapsedEventArgs args)
        {
            Log.Info("WakeUp");
            this.manager.OnWakeUp(
                (defaults, action) => RunServicesAsync(defaults, action, 
                    () => new string[] { },
                    errors => errors));
        }

        private async Task<TResult> RunServicesAsync<TResult>(ServiceDefaults defaults, BackupAction action, Func<TResult> onNoServices, Func<string[], TResult> onCompleted)
        {
            if (!action.services.Any())
            {
                Log.Info($"no services for {action.uniqueId}");
                return onNoServices();
            }

            if (!CloudStorageAccount.TryParse(action.sourceConnectionString, out CloudStorageAccount sourceAccount))
                return onCompleted(new[] { $"bad SOURCE connection string for {action.uniqueId}" });
            if (!CloudStorageAccount.TryParse(action.recurringSchedule.targetConnectionString, out CloudStorageAccount targetAccount))
                return onCompleted(new[] { $"bad TARGET connection string for {action.uniqueId}" });

            var errors = new string[] { };
            // Blobs
            if (action.services.Contains(StorageService.Blob))
            {
                Log.Info($"copying blobs for {action.uniqueId}...");
                var sourceClient = sourceAccount.CreateCloudBlobClient();
                var targetClient = targetAccount.CreateCloudBlobClient();
                var err = await await sourceClient.FindAllContainersAsync<Task<string[]>>(
                    async sourceContainers => 
                    {
                        var stats = await sourceContainers
                            .Select(sourceContainer => sourceContainer.CopyContainerAsync(targetClient, defaults.blob, StopCalled))
                            .WhenAllAsync();
                        return stats
                            .SelectMany(s => s.Value.errors)
                            .ToArray();
                    },
                    why => new[] { why }.ToTask());
                errors = errors.Concat(err).ToArray();
            }
            return onCompleted(errors);
        }
    }
}
