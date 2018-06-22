using EastFive.Azure.Storage.Backup.Configuration;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Linq;

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

        // can be empty to indicate all containers
        public string[] includedContainers;
        // if includedContainers is empty, this can be used to omit certain ones
        public string[] excludedContainers;
        public TimeSpan accessPeriod;
        public int maxContainerConcurrency;
        public int maxBlobConcurrencyPerContainer;
        public TimeSpan minIntervalCheckCopy;
        public TimeSpan maxIntervalCheckCopy;
        public TimeSpan maxTotalWaitForCopy;
        public int copyRetries;

        public bool ShouldCopy(string containerName)
        {
            return (excludedContainers == null || !excludedContainers.Contains(containerName, StringComparer.OrdinalIgnoreCase)) &&
                   (includedContainers == null || !includedContainers.Any() || includedContainers.Contains(containerName, StringComparer.OrdinalIgnoreCase));
        }
    }
}
