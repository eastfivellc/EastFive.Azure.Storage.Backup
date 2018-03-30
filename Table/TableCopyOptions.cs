using EastFive.Azure.Storage.Backup.Configuration;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using System;

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

        public int maxTableConcurrency;
        public int maxRowConcurrencyPerTable;
        public int copyRetries;
    }
}
