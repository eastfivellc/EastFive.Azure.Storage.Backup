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
    }
}
