using BlackBarLabs;
using BlackBarLabs.Extensions;
using EastFive.Azure.Storage.Backup.Blob;
using EastFive.Azure.Storage.Backup.Configuration;
using EastFive.Azure.Storage.Backup.Table;
using log4net.Config;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Diagnostics;
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
        private bool fastStart;

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
                Log.Info("Initializing...");

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
            fastStart = true;
            timer.Interval = TimeSpan.FromSeconds(15).TotalMilliseconds;
            timer.AutoReset = false;
            timer.Elapsed += OnTimer;
            timer.Start();

            AppDomain.CurrentDomain.UnhandledException += ExceptionHandler;
        }

        protected override void OnStop()
        {
            Log.Info("Service stopping...");

            timer.Stop();
            if (manager.Status.running.Any())
            {
                var maxWaitLocal = DateTime.Now + TimeSpan.FromMinutes(5);
                do
                {
                    Thread.Sleep((int)TimeSpan.FromSeconds(15).TotalMilliseconds);
                    if (!manager.Status.running.Any())
                        break;
                    Log.Info("work still running, waiting...");
                } while (DateTime.Now < maxWaitLocal);
            }

            Log.Info("Service stopped");
        }

        private void ExceptionHandler(object sender, UnhandledExceptionEventArgs e)
        {
            if (e.ExceptionObject is Exception ex)
                Log.Error($"Unhandled exception! Process terminating: {e.IsTerminating}", ex);
        }

        private bool StopCalled()
        {
            return !timer.Enabled;
        }

        public async void OnTimer(object sender, ElapsedEventArgs args)
        {
            if (fastStart)
            {
                fastStart = false;
                timer.Interval = wakeUp.TotalMilliseconds;
                timer.AutoReset = true;
                timer.Start();
            }

            Log.Info("WakeUp");
            var logMessage = await this.manager.OnWakeUp(
                () => "a backup is already running".ToTask(),
                async (serviceSettings, action, schedule, onCompleted, onStopped) =>
                {
                    return await RunServicesAsync(serviceSettings, action, schedule,
                        errors =>
                        {
                            onCompleted(errors);
                            return $"finished all work for {schedule.tag}. Detail: {(errors.Any() ? errors.Join(",") : "no errors")}";
                        },
                        errors =>
                        {
                            onStopped(errors);
                            return $"stopped all work for {schedule.tag}. Detail: {(errors.Any() ? errors.Join(",") : "no errors")}";
                        });
                },
                () => "not yet time to backup".ToTask(),
                () => "unable to find any backup configuration".ToTask());
            Log.Info(logMessage);
        }

        private async Task<TResult> RunServicesAsync<TResult>(ServiceSettings serviceSettings, BackupAction action, RecurringSchedule schedule, Func<string[], TResult> onCompleted, Func<string[],TResult> onStopped)
        {
            if (!CloudStorageAccount.TryParse(action.sourceConnectionString, out CloudStorageAccount sourceAccount))
                return onCompleted(new[] { $"bad SOURCE connection string for {action.tag}" });
            if (!CloudStorageAccount.TryParse(schedule.targetConnectionString, out CloudStorageAccount targetAccount))
                return onCompleted(new[] { $"bad TARGET connection string for {schedule.tag}" });

            var watch = new Stopwatch();
            watch.Start();
            var errors = new string[] { };

            // Table
            if (action.services.Contains(StorageService.Table))
            {
                if (StopCalled())
                    return onStopped(errors);

                Log.Info($"copying tables for {action.tag} to {schedule.tag}...");
                var sourceClient = sourceAccount.CreateCloudTableClient();
                var targetClient = targetAccount.CreateCloudTableClient();
                var err = await await sourceClient.FindAllTablesAsync(
                    async sourceTables =>
                    {
                        var stats = await sourceTables
                            .Select(sourceTable => sourceTable.CopyTableAsync(targetClient, serviceSettings.table, StopCalled))
                            .WhenAllAsync(serviceSettings.table.maxTableConcurrency);
                        return stats
                            .SelectMany(s => s.Value.errors)
                            .ToArray();
                    },
                    why => new[] { why }.ToTask());
                errors = errors.Concat(err).ToArray();
                Log.Info($"finished copying tables for {action.tag} in {watch.Elapsed}");
            }

            // Blob
            if (action.services.Contains(StorageService.Blob))
            {
                if (StopCalled())
                    return onStopped(errors);

                Log.Info($"copying blobs for {action.tag} to {schedule.tag}...");
                var sourceClient = sourceAccount.CreateCloudBlobClient();
                var targetClient = targetAccount.CreateCloudBlobClient();
                var err = await await sourceClient.FindAllContainersAsync<Task<string[]>>(
                    async sourceContainers => 
                    {
                        var stats = await sourceContainers
                            .Select(sourceContainer => sourceContainer.CopyContainerAsync(targetClient, serviceSettings.blob, StopCalled))
                            .WhenAllAsync(serviceSettings.blob.maxContainerConcurrency);
                        return stats
                            .SelectMany(s => s.Value.errors)
                            .ToArray();
                    },
                    why => new[] { why }.ToTask());
                errors = errors.Concat(err).ToArray();
                Log.Info($"finished copying blobs for {action.tag} in {watch.Elapsed}");
            }

            return StopCalled() ? onStopped(errors) : onCompleted(errors);
        }
    }
}
