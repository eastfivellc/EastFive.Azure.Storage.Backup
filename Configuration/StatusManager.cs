using BlackBarLabs.Extensions;
using EastFive.Linq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

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

        public TResult OnWakeUp<TResult>(Func<TResult> onAlreadyRunning, Func<ServiceDefaults,BackupAction,RecurringSchedule,Func<string[],ActionStatus>,TResult> onNext, Func<TResult> onNothingToDo, Func<TResult> onMissingConfiguration)
        {
            var value = Load();
            var timeOfDay = DateTime.UtcNow.TimeOfDay;
            if (timeOfDay < value.resetAt)
                value = ResetStatus();

            return GetNextAction(
                value,
                onAlreadyRunning,
                (serviceDefaults,action,schedule) =>
                {
                    if (!Save((v, save) =>
                        {
                            if (v.running.Contains(schedule.uniqueId) || v.completed.Contains(schedule.uniqueId))
                                return false;
                            save(v.ConcatRunning(schedule.uniqueId));
                            return true;
                        },
                        why => false)
                    )
                        return onAlreadyRunning();
                    
                    return onNext(serviceDefaults, action, schedule,
                        (errors) => OnActionCompleted(schedule.uniqueId, errors));
                },
                onNothingToDo,
                onMissingConfiguration);
        }

        private TResult GetNextAction<TResult>(ActionStatus value, Func<TResult> onAlreadyRunning, Func<ServiceDefaults,BackupAction,RecurringSchedule,TResult> onNext, Func<TResult> onNothingToDo, Func<TResult> onMissingConfiguration)
        {
            if (!settings.Settings.HasValue)
                return onMissingConfiguration();

            if (value.running.Any())
                return onAlreadyRunning();

            var utcNow = DateTime.UtcNow;
            var ready = settings.Settings.Value.actions
                .SelectMany(a => a.GetActiveSchedules(utcNow)
                    .Select(s => a.PairWithValue(s)))
                .OrderBy(pair => pair.Value.timeUtc)
                .Take(1)
                .ToArray();
            
            return ready.Length == 1 ? onNext(settings.Settings.Value.serviceDefaults, ready[0].Key, ready[0].Value) : onNothingToDo();
        }

        private ActionStatus OnActionCompleted(Guid completed, string[] errors)
        {
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
    }
}
