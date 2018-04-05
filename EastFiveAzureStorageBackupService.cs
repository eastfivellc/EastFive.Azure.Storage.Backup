using BlackBarLabs.Extensions;
using EastFive.Azure.Storage.Backup.Blob;
using EastFive.Azure.Storage.Backup.Configuration;
using EastFive.Azure.Storage.Backup.Table;
using log4net.Config;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
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
        private readonly StatusManager scheduler;
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

                this.scheduler = new StatusManager(dir, this.loader);

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
            if (scheduler.Status.running.Any())
            {
                var maxWaitLocal = DateTime.Now + TimeSpan.FromMinutes(5);
                do
                {
                    Thread.Sleep((int)TimeSpan.FromSeconds(15).TotalMilliseconds);
                    if (!scheduler.Status.running.Any())
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
            Log.Info("WakeUp");
            if (fastStart)
            {
                fastStart = false;
                timer.Interval = wakeUp.TotalMilliseconds;
                timer.AutoReset = true;
                timer.Start();
            }

            KeyValuePair<string, bool> result;
            var nowLocal = DateTime.Now;
            do
            {
                result = await this.scheduler.CheckForWork(nowLocal,
                () => "a backup is already running".PairWithValue(false).ToTask(),
                async (serviceSettings, action, schedule, onCompleted, onStopped) =>
                {
                    Log.Info($"copying from {action.tag} to {schedule.tag}...");
                    var watch = new Stopwatch();
                    watch.Start();
                    return await RunServicesAsync(serviceSettings, action, schedule,
                        errors =>
                        {
                            var msg = $"all work done for {schedule.tag} in {watch.Elapsed}. Detail: {(errors.Any() ? errors.Join(",") : "no errors")}";
                            if (StopCalled())
                            {
                                onStopped(errors);
                                return msg.PairWithValue(false);
                            }

                            onCompleted(errors);
                            return msg.PairWithValue(true);
                        });
                },
                () => "not yet time to backup".PairWithValue(false).ToTask(),
                () => "unable to find any backup configuration".PairWithValue(false).ToTask());
                Log.Info(result.Key);
            } while (result.Value);
        }

        private async Task<TResult> RunServicesAsync<TResult>(ServiceSettings serviceSettings, BackupAction action, RecurringSchedule schedule, Func<string[], TResult> onCompleted)
        {
            if (action.services == null || !action.services.Any())
                return onCompleted(new string[] { $"no services for {action.tag}"});
            if (!action.GetSourceAccount(out CloudStorageAccount sourceAccount))
                return onCompleted(new[] { $"bad SOURCE connection string for {action.tag}" });
            if (!schedule.GetTargetAccount(out CloudStorageAccount targetAccount))
                return onCompleted(new[] { $"bad TARGET connection string for {schedule.tag}" });

            var errors = new string[] { };
            if (action.services.Contains(StorageService.Table))
                errors = errors
                    .Concat(await serviceSettings.table.CopyAccountAsync(sourceAccount, targetAccount, StopCalled, msgs => msgs))
                    .ToArray();

            if (action.services.Contains(StorageService.Blob))
                errors = errors
                    .Concat(await serviceSettings.blob.CopyAccountAsync(sourceAccount, targetAccount, StopCalled, msgs => msgs))
                    .ToArray();

            return onCompleted(errors);
        }
    }
}
