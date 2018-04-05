using EastFive.Azure.Storage.Backup.Blob;
using EastFive.Azure.Storage.Backup.Table;
using Microsoft.WindowsAzure.Storage;
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
        public TimeSpan timeLocal;

        public bool IsActive(DateTime asOfDateLocal)
        {
            return daysOfWeek.Contains(asOfDateLocal.DayOfWeek) && asOfDateLocal.TimeOfDay > timeLocal;
        }

        public bool GetTargetAccount(out CloudStorageAccount targetAccount)
        {
            return CloudStorageAccount.TryParse(targetConnectionString, out targetAccount);
        }
    }

    public struct BackupAction
    {
        public string tag;
        public string sourceConnectionString;
        public StorageService[] services;
        public RecurringSchedule[] recurringSchedules;

        public RecurringSchedule[] GetActiveSchedules(DateTime asOfDateLocal)
        {
            if (recurringSchedules == null || services == null || !services.Any())
                return new RecurringSchedule[] { };

            return recurringSchedules
                .Where(x => x.IsActive(asOfDateLocal))
                .ToArray();
        }

        public bool GetSourceAccount(out CloudStorageAccount sourceAccount)
        {
            return CloudStorageAccount.TryParse(sourceConnectionString, out sourceAccount);
        }
    }

    public struct ServiceSettings
    {
        public static readonly TimeSpan defaultBackoff = TimeSpan.FromSeconds(4);
        public const int defaultMaxAttempts = 10;
        public static readonly TimeSpan defaultRequestTimeout = TimeSpan.FromSeconds(90);

        public TableCopyOptions table;
        public BlobCopyOptions blob;
    }

    public struct BackupSettings
    {
        public ServiceSettings serviceSettings;
        public BackupAction[] actions;
    }
}
