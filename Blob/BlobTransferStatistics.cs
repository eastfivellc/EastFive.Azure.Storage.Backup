using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EastFive.Azure.Storage.Backup.Blob
{
    public struct BlobTransferStatistics
    {
        public const int logFrequency = 50_000;

        public IntervalCalculator calc;
        public string[] errors;
        public int successes;
        public KeyValuePair<CloudBlob, TransferStatus>[] retries;
        public KeyValuePair<CloudBlob, TransferStatus>[] failures;

        public void LogProgress(TimeSpan lower, TimeSpan higher, Action<string> onLog, int batchSize = 5_000)  // blob segment retrieval is 5K
        {
            var remainder = (successes + retries.Length + failures.Length) % logFrequency;
            if (remainder < batchSize)
                onLog($"successes: {successes}, retries: {retries.Length}, failures: {failures.Length}, next interval: {calc.GetNextInterval(lower, higher)}, errors: {errors.Length}/{errors.Distinct().Join(",")}");
        }

        public static readonly BlobTransferStatistics Default = new BlobTransferStatistics
        {
            calc = IntervalCalculator.Default,
            errors = new string[] { },
            successes = 0,
            retries = new KeyValuePair<CloudBlob, TransferStatus>[] { },
            failures = new KeyValuePair<CloudBlob, TransferStatus>[] { }
        };

        public BlobTransferStatistics Concat(BlobTransferStatistics stats)
        {
            return new BlobTransferStatistics
            {
                calc = this.calc.Concat(stats.calc),
                errors = stats.errors.Any() ? this.errors.Concat(stats.errors).ToArray() : this.errors,
                successes = this.successes + stats.successes,
                retries = stats.retries.Any() ? this.retries.Concat(stats.retries).ToArray() : this.retries,
                failures = stats.failures.Any() ? this.failures.Concat(stats.failures).ToArray() : this.failures
            };
        }

        public BlobTransferStatistics Concat(string[] errors)
        {
            return new BlobTransferStatistics
            {
                calc = this.calc,
                errors = errors.Any() ? this.errors.Concat(errors).ToArray() : this.errors,
                successes = this.successes,
                retries = this.retries,
                failures = this.failures
            };
        }
    }
}
