using EastFive.Linq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EastFive.Azure.Storage.Backup.Configuration
{
    public class StatusManager
    {
        private readonly string statusPath;
        private readonly BackupSettingsLoader settings;
        private readonly ReaderWriterLockSlim mutex = new ReaderWriterLockSlim();
        private ActionStatus status;

        public StatusManager(string path, BackupSettingsLoader settings)
        {
            this.statusPath = Path.Combine(path, "status.json");
            this.settings = settings;
            this.status = ActionStatus.GetDefault();

            // Initialize since nothing is running when we start up
            ResetStatus();
        }

        public ActionStatus Status
        {
            get
            {
                mutex.EnterReadLock();
                try
                {
                    return status;
                }
                finally
                {
                    mutex.ExitReadLock();
                }
            }
        }

        public void OnWakeUp(Func<ServiceDefaults,BackupAction,Task<string[]>> work)
        {
            var value = Load();
            var timeOfDay = DateTime.UtcNow.TimeOfDay;
            if (timeOfDay < value.resetAt)
                value = ResetStatus();

            
            GetNextAction(
                value,
                () => ActionStatus.GetDefault(),
                action =>
                {
                    EastFiveAzureStorageBackupService.Log.Info($"ready to run {action.uniqueId}");
                    if (!Save((v, save) =>
                        {
                            if (v.running.Contains(action.uniqueId))
                                return false;
                            save(v.ConcatRunning(action.uniqueId));
                            return true;
                        },
                        why => false)
                    )
                        return ActionStatus.GetDefault();

                    var watch = new Stopwatch();
                    watch.Start();
                    return OnActionCompleted(
                        action.uniqueId,
                        work(settings.Settings.Value.serviceDefaults, action)
                            .GetAwaiter()
                            .GetResult(),
                        watch.Elapsed);
                },
                () => ActionStatus.GetDefault());
        }

        private ActionStatus OnActionCompleted(Guid completed, string[] errors, TimeSpan duration)
        {
            EastFiveAzureStorageBackupService.Log.Info($"finished work for {completed} in {duration}");
            return Save(
                (v,save) => save(v.ConcatCompleted(completed, errors)),
                why => ActionStatus.GetDefault());
        }

        private ActionStatus ResetStatus()
        {
            return Save(
                (v, save) => save(ActionStatus.GetDefault()),
                why => ActionStatus.GetDefault());
        }

        private TResult Save<TResult>(Func<ActionStatus,Func<ActionStatus,ActionStatus>,TResult> onExchange, Func<string,TResult> onFailure)
        {
            mutex.EnterWriteLock();
            try
            {
                return onExchange(status, 
                    update =>
                    {
                        EastFiveAzureStorageBackupService.Log.Info(
                            $"saving new status, running: {update.running.FirstOrDefault()}, completed: {update.completed.Length}, resetAt: {update.resetAt}");
                        File.WriteAllText(statusPath, JsonConvert.SerializeObject(update, Formatting.Indented));
                        status = update;
                        return status;
                    });
            }
            catch (Exception e)
            {
                EastFiveAzureStorageBackupService.Log.Error("unable to save status", e);
                return onFailure(e.Message);
            }
            finally
            {
                mutex.ExitWriteLock();
            }
        }

        private ActionStatus Load()
        {
            mutex.EnterWriteLock();
            try
            {
                var text = File.ReadAllText(statusPath);
                status = JsonConvert.DeserializeObject<ActionStatus>(text, new JsonSerializerSettings()
                {
                    Converters = new List<JsonConverter>
                    {
                        new Newtonsoft.Json.Converters.StringEnumConverter()
                    }
                });
                return status;
            }
            catch (Exception e)
            {
                EastFiveAzureStorageBackupService.Log.Error($"Error loading status from path {statusPath}", e);
                return status;
            }
            finally
            {
                mutex.ExitWriteLock();
            }
        }

        private TResult GetNextAction<TResult>(ActionStatus value, Func<TResult> onRunning, Func<BackupAction, TResult> onNext, Func<TResult> onNothing)
        {
            if (value.running.Any())
                return onRunning();

            if (!settings.Settings.HasValue)
                return onNothing();

            var utcNow = DateTime.UtcNow;
            var dayOfWeek = utcNow.DayOfWeek;
            var action = settings.Settings.Value.actions
                .Where(a => a.recurringSchedule.daysOfWeek.Contains(dayOfWeek) &&
                    utcNow.TimeOfDay > a.recurringSchedule.timeUtc &&
                    !value.completed.Contains(a.uniqueId))
                .OrderBy(a => a.recurringSchedule.timeUtc)
                .Take(1)
                .ToArray();
            return action.Length == 1 ? onNext(action[0]) : onNothing();
        }
    }
}
