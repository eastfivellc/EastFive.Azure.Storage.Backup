using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Linq;

namespace EastFive.Azure.Storage.Backup.Blob
{
    public struct BlobTransferStatistics
    {
        public IntervalCalculator calc;
        public string[] errors;
        public int successes;
        public KeyValuePair<CloudBlob, TransferStatus>[] retries;
        public KeyValuePair<CloudBlob, TransferStatus>[] failures;

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
                errors = this.errors.Concat(stats.errors).ToArray(),
                successes = this.successes + stats.successes,
                retries = this.retries.Concat(stats.retries).ToArray(),
                failures = this.failures.Concat(stats.failures).ToArray()
            };
        }

        public BlobTransferStatistics Concat(string[] errors)
        {
            return new BlobTransferStatistics
            {
                calc = this.calc,
                errors = this.errors.Concat(errors).ToArray(),
                successes = this.successes,
                retries = this.retries,
                failures = this.failures
            };
        }
    }
}
