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
            var ex = Load(
                ActionStatus.GetDefault,
                v => default(Exception),
                e => e);
            if (default(Exception) != ex)
                throw ex;

            ex = Save((v, save) =>
            {
                // Clear any left over running status at startup, but preserve our completed ones.
                if (v.SomethingRunning())
                    save(v.ClearRunning(new string[] { }));
                return default(Exception);
            },
            e => e);
            if (default(Exception) != ex)
                throw ex;
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

        public TResult CheckForWork<TResult>(DateTime nowLocal, Func<TResult> onAlreadyRunning, Func<ServiceSettings,BackupAction,RecurringSchedule,Func<string[],ActionStatus>,Func<string[],ActionStatus>,TResult> onNext, Func<TResult> onNothingToDo, Func<TResult> onMissingConfiguration)
        {
            // The status may have changed since the last check
            var value = GetCurrentStatus(nowLocal);

            return GetNextAction(nowLocal, value,
                onAlreadyRunning,
                (serviceSettings,action,schedule) =>
                {
                    if (!Save((v, save) =>
                        {
                            // Inside the lock, we can check for sure whether it's already been started
                            if (v.HasStarted(schedule.uniqueId))
                                return false;
                            save(v.UpdateWithRunning(schedule.uniqueId));
                            return true;
                        },
                        e => false)
                    )
                        return onAlreadyRunning();
                    
                    return onNext(serviceSettings, action, schedule,
                        (errors) => OnActionCompleted(schedule.uniqueId, errors),
                        OnActionStopped);
                },
                onNothingToDo,
                onMissingConfiguration);
        }

        private TResult GetNextAction<TResult>(DateTime nowLocal, ActionStatus value, Func<TResult> onAlreadyRunning, Func<ServiceSettings,BackupAction,RecurringSchedule,TResult> onNext, Func<TResult> onNothingToDo, Func<TResult> onMissingConfiguration)
        {
            if (!settings.Settings.HasValue || settings.Settings.Value.actions == null)
                return onMissingConfiguration();

            if (value.running.Any())
                return onAlreadyRunning();

            var ready = settings.Settings.Value.actions
                .SelectMany(a => a.GetActiveSchedules(nowLocal)
                    .Select(s => a.PairWithValue(s)))
                .Where(pair => !value.completed.Contains(pair.Value.uniqueId))
                .OrderBy(pair => pair.Value.timeLocal)
                .Take(1)
                .ToArray();
            return ready.Length == 1 ? onNext(settings.Settings.Value.serviceSettings, ready[0].Key, ready[0].Value) : onNothingToDo();
        }

        private ActionStatus OnActionCompleted(Guid completed, string[] errors)
        {
            return Save(
                (v,save) => save(v.UpdateWithCompleted(completed, errors)),
                e => status);
        }

        private ActionStatus OnActionStopped(string[] errors)
        {
            return Save(
                (v, save) => save(v.ClearRunning(errors)),
                e => status);
        }

        private ActionStatus GetCurrentStatus(DateTime nowLocal)
        {
            var value = Load(
                ActionStatus.GetDefault,
                v => v,
                e => status);
            if (value.PastEndOfDay(nowLocal))
            {
                return Save(
                    (v, save) =>
                    {
                        // Inside the lock, we can check for sure whether it is time to start the next day's schedule.
                        if (v.PastEndOfDay(nowLocal))
                            return save(ActionStatus.GetDefault());
                        return v;
                    },
                    e => status);
            }
            return value;
        }

        private TResult Save<TResult>(Func<ActionStatus,Func<ActionStatus,ActionStatus>,TResult> onExchange, Func<Exception,TResult> onFailure)
        {
            mutex.EnterWriteLock();
            try
            {
                return onExchange(status, 
                    update =>
                    {
                        EastFiveAzureStorageBackupService.Log.Info(
                            $"saving new status, running: {update.running.FirstOrDefault()}, completed: {update.completed.Length}, resetAt: {update.resetAtLocal}");
                        File.WriteAllText(statusPath, JsonConvert.SerializeObject(update, Formatting.Indented));
                        status = update;
                        return status;
                    });
            }
            catch (Exception e)
            {
                EastFiveAzureStorageBackupService.Log.Error("unable to save status", e);
                return onFailure(e);
            }
            finally
            {
                mutex.ExitWriteLock();
            }
        }

        private TResult Load<TResult>(Func<ActionStatus> getInitialValue, Func<ActionStatus,TResult> onLoad, Func<Exception, TResult> onFailure)
        {
            mutex.EnterWriteLock();
            try
            {
                if (!File.Exists(statusPath))
                {
                    status = getInitialValue();
                    return onLoad(status);
                }

                var text = File.ReadAllText(statusPath);
                status = JsonConvert.DeserializeObject<ActionStatus>(text, new JsonSerializerSettings()
                {
                    Converters = new List<JsonConverter>
                    {
                        new Newtonsoft.Json.Converters.StringEnumConverter()
                    }
                });
                return onLoad(status);
            }
            catch (Exception e)
            {
                EastFiveAzureStorageBackupService.Log.Error($"Error loading status from path {statusPath}", e);
                return onFailure(e);
            }
            finally
            {
                mutex.ExitWriteLock();
            }
        }
    }
}
