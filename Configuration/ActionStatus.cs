using System;
using System.Linq;

namespace EastFive.Azure.Storage.Backup.Configuration
{
    public struct ActionStatus
    {
        public TimeSpan resetAt;
        public Guid[] running;
        public Guid[] completed;
        public string[] errors;

        public static ActionStatus GetDefault()
        {
            return new ActionStatus
            {
                resetAt = DateTime.UtcNow.TimeOfDay,
                running = new Guid[] { },
                completed = new Guid[] { },
                errors = new string[] { }
            };
        }

        public ActionStatus ConcatRunning(Guid running)
        {
            return new ActionStatus
            {
                resetAt = this.resetAt,
                running = this.running.Concat(new[] { running }).ToArray(),
                completed = this.completed.Except(new [] { running }).ToArray(),
                errors = this.errors
            };
        }

        public ActionStatus ConcatCompleted(Guid completed, string[] errors)
        {
            return new ActionStatus
            {
                resetAt = this.resetAt,
                running = this.running.Except(new[] { completed }).ToArray(),
                completed = this.completed.Concat(new [] { completed }).ToArray(),
                errors = this.errors.Concat(errors).ToArray()
            };
        }
    }
}
