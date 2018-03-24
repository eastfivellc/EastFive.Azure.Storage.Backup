using EastFive.Azure.Storage.Backup.Blob;
using System;

namespace EastFive.Azure.Storage.Backup.Configuration
{
    public struct RecurringSchedule
    {
        public string targetConnectionString;
        public DayOfWeek[] daysOfWeek;
        public TimeSpan timeUtc;
    }

    public struct BackupAction
    {
        public Guid uniqueId;
        public string tag;
        public string sourceConnectionString;
        public StorageService[] services;
        public RecurringSchedule recurringSchedule;
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
