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

        public ActionStatus ConcatRunning(Guid running)
        {
            return new ActionStatus
            {
                resetAtLocal = this.resetAtLocal,
                running = this.running.Concat(new[] { running }).ToArray(),
                completed = this.completed.Except(new [] { running }).ToArray(),
                errors = this.errors
            };
        }

        public ActionStatus ConcatCompleted(Guid completed, string[] errors)
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
    }
}
