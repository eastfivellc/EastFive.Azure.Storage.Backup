using System;

namespace EastFive.Azure.Storage.Backup.Blob
{
    public struct BlobCopyOptions
    {
        public TimeSpan accessPeriod;
        public int maxConcurrency;
        public TimeSpan minIntervalCheckCopy;
        public TimeSpan maxIntervalCheckCopy;
        public TimeSpan maxTotalWaitForCopy;
        public int copyRetries;

        public static readonly BlobCopyOptions Default = new BlobCopyOptions
        {
            accessPeriod = TimeSpan.FromMinutes(60),
            maxConcurrency = 200,
            minIntervalCheckCopy = TimeSpan.FromSeconds(1),
            maxIntervalCheckCopy = TimeSpan.FromSeconds(15),
            maxTotalWaitForCopy = TimeSpan.FromMinutes(5),
            copyRetries = 5
        };
    }
}
