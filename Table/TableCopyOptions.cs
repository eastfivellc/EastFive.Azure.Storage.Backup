using EastFive.Azure.Storage.Backup.Configuration;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;

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
        public int maxSegmentDownloadConcurrencyPerTable;
        public int maxRowUploadConcurrencyPerTable;
        // can be empty to indicate all keys
        public string[] partitionKeys; 
        public int copyRetries;
    }
}
