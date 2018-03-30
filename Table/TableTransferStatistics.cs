using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EastFive.Azure.Storage.Backup.Table
{
    public struct TableTransferStatistics
    {
        public const int logFrequency = 50_000;

        public string[] errors;
        public int successes;
        public KeyValuePair<DynamicTableEntity, TransferStatus>[] retries;

        public void LogProgress(Action<string> onLog, int batchSize = 1_000)  // row segment retrieval is 1K
        {
            var remainder = (successes + retries.Length) % logFrequency;
            if (remainder < batchSize)
                onLog($"successes: {successes}, retries: {retries.Length}, errors: {errors.Length}/{errors.Distinct().Join(",")}");
        }

        public static readonly TableTransferStatistics Default = new TableTransferStatistics
        {
            errors = new string[] { },
            successes = 0,
            retries = new KeyValuePair<DynamicTableEntity, TransferStatus>[] { }
        };

        public TableTransferStatistics Concat(TableTransferStatistics stats)
        {
            return new TableTransferStatistics
            {
                errors = stats.errors.Any() ? this.errors.Concat(stats.errors).ToArray() : this.errors,
                successes = this.successes + stats.successes,
                retries = stats.retries.Any() ? this.retries.Concat(stats.retries).ToArray() : this.retries
            };
        }

        public TableTransferStatistics Concat(string[] errors)
        {
            return new TableTransferStatistics
            {
                errors = errors.Any() ? this.errors.Concat(errors).ToArray() : this.errors,
                successes = this.successes,
                retries = this.retries
            };
        }
    }
}
