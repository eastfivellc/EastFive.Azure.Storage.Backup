using EastFive.Azure.Storage.Backup.Configuration;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;

namespace EastFive.Azure.Storage.Backup.Table
{
    public struct TableCopyOptions
    {
        public static readonly TableRequestOptions requestOptions =
            new TableRequestOptions
            {
                ServerTimeout = ServiceSettings.defaultRequestTimeout,
                RetryPolicy = new ExponentialRetry(ServiceSettings.defaultBackoff, ServiceSettings.defaultMaxAttempts)
            };

        // can be empty to indicate all tables
        public string[] includedTables;
        // if includedTables is empty, this can be used to omit certain ones
        public string[] excludedTables;
        public int maxTableConcurrency;
        public int maxSegmentDownloadConcurrencyPerTable;
        public int maxRowUploadConcurrencyPerTable;
        // can be empty to indicate all keys
        public string[] partitionKeys;
        public int copyRetries;

        public bool ShouldCopy(string tableName)
        {
            return (excludedTables == null || !excludedTables.Contains(tableName, StringComparer.OrdinalIgnoreCase)) &&
                   (includedTables == null || !includedTables.Any() || includedTables.Contains(tableName, StringComparer.OrdinalIgnoreCase));
        }
    }
}
