using EastFive.Azure.Storage.Backup.Blob;
using System;
using System.Linq;

namespace EastFive.Azure.Storage.Backup.Configuration
{
    public struct RecurringSchedule
    {
        public Guid uniqueId;
        public string tag;
        public string targetConnectionString;
        public DayOfWeek[] daysOfWeek;
        public TimeSpan timeUtc;

        public bool IsActive(DateTime asOfDate)
        {
            return daysOfWeek.Contains(asOfDate.DayOfWeek) && asOfDate.TimeOfDay > timeUtc;
        }
    }

    public struct BackupAction
    {
        public string tag;
        public string sourceConnectionString;
        public StorageService[] services;
        public RecurringSchedule[] recurringSchedules;

        public RecurringSchedule[] GetActiveSchedules(DateTime asOfDate)
        {
            if (!services.Any())
                return new RecurringSchedule[] { };

            return recurringSchedules
                .Where(x => x.IsActive(asOfDate))
                .ToArray();
        }
    }

    public struct ServiceDefaults
    {
        public BlobCopyOptions blob;
    }

    public struct BackupSettings
    {
        public ServiceDefaults serviceDefaults;
        public BackupAction[] actions;
    }
}
