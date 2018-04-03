using System;
using System.Linq;

namespace EastFive.Azure.Storage.Backup.Configuration
{
    public struct ActionStatus
    {
        public DateTime resetAtLocal;
        public Guid[] running;
        public Guid[] completed;
        public string[] errors;

        public static ActionStatus GetDefault()
        {
            return new ActionStatus
            {
                resetAtLocal = DateTime.Now.Date + TimeSpan.FromDays(1),
                running = new Guid[] { },
                completed = new Guid[] { },
                errors = new string[] { }
            };
        }

        public ActionStatus UpdateWithRunning(Guid running)
        {
            return new ActionStatus
            {
                resetAtLocal = this.resetAtLocal,
                running = this.running.Concat(new[] { running }).ToArray(),
                completed = this.completed.Except(new [] { running }).ToArray(),
                errors = this.errors
            };
        }

        public ActionStatus UpdateWithCompleted(Guid completed, string[] errors)
        {
            return new ActionStatus
            {
                resetAtLocal = this.resetAtLocal,
                running = this.running.Except(new[] { completed }).ToArray(),
                completed = this.completed.Concat(new [] { completed }).ToArray(),
                errors = errors.Any() ? this.errors.Concat(errors).ToArray() : this.errors
            };
        }

        public ActionStatus ClearRunning(string[] errors)
        {
            return new ActionStatus
            {
                resetAtLocal = this.resetAtLocal,
                running = new Guid[] { },
                completed = this.completed,
                errors = errors.Any() ? this.errors.Concat(errors).ToArray() : this.errors
            };
        }

        public bool PastEndOfDay(DateTime nowLocal)
        {
            // If something is running, the day isn't over yet.
            return !SomethingRunning() && nowLocal >= resetAtLocal;
        }

        public bool HasStarted(Guid uniqueId)
        {
            return running.Contains(uniqueId) || completed.Contains(uniqueId);
        }

        public bool SomethingRunning()
        {
            return running.Any();
        }
    }
}
