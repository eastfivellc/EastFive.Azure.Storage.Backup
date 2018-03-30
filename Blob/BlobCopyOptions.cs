using EastFive.Azure.Storage.Backup.Configuration;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;

namespace EastFive.Azure.Storage.Backup.Blob
{
    public struct BlobCopyOptions
    {
        public static readonly BlobRequestOptions requestOptions =
            new BlobRequestOptions
            {
                ServerTimeout = ServiceSettings.defaultRequestTimeout,
                RetryPolicy = new ExponentialRetry(ServiceSettings.defaultBackoff, ServiceSettings.defaultMaxAttempts)
            };

        public TimeSpan accessPeriod;
        public int maxContainerConcurrency;
        public int maxBlobConcurrencyPerContainer;
        public TimeSpan minIntervalCheckCopy;
        public TimeSpan maxIntervalCheckCopy;
        public TimeSpan maxTotalWaitForCopy;
        public int copyRetries;
    }
}
