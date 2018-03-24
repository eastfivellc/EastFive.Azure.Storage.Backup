using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using BlackBarLabs.Extensions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;

namespace BlackBarLabs.Persistence.Azure.StorageTables.Backups
{
    public enum TableStatus
    {
        CopySuccessful,
        Running,
        ShouldRetry,
        Failed
    }

    public struct TableStatistics
    {
        public string[] errors;
        public int successes;
        public KeyValuePair<CloudTable, TableStatus>[] retries;
        public KeyValuePair<CloudTable, TableStatus>[] failures;

        public static readonly TableStatistics Default = new TableStatistics
        {
            errors = new string[] { },
            successes = 0,
            retries = new KeyValuePair<CloudTable, TableStatus>[] { },
            failures = new KeyValuePair<CloudTable, TableStatus>[] { }
        };

        public TableStatistics Concat(TableStatistics stats)
        {
            return new TableStatistics
            {
                errors = this.errors.Concat(stats.errors).ToArray(),
                successes = this.successes + stats.successes,
                retries = this.retries.Concat(stats.retries).ToArray(),
                failures = this.failures.Concat(stats.failures).ToArray()
            };
        }

        public TableStatistics Concat(string[] errors)
        {
            return new TableStatistics
            {
                errors = this.errors.Concat(errors).ToArray(),
                successes = this.successes,
                retries = this.retries,
                failures = this.failures
            };
        }
    }

    public struct TableCopyOptions
    {
        public TimeSpan accessPeriod;
        public int maxBatch;
        public int maxConcurrency;
        public TimeSpan checkCopyCompleteAfter;
        public TimeSpan maxWaitForCopyComplete;
        public int copyRetries;

        public static readonly BlobCopyOptions Default = new BlobCopyOptions
        {
            accessPeriod = TimeSpan.FromMinutes(60),
            maxBatch = 100_000,
            maxConcurrency = 200,
            checkCopyCompleteAfter = TimeSpan.FromSeconds(10),
            maxWaitForCopyComplete = TimeSpan.FromMinutes(5),
            copyRetries = 5
        };
    }

    public static class StorageTable 
    {
        //private struct BlobAccess
        //{
        //    public string key;
        //    public DateTime expiresUtc;
        //}

        //ok
        private static readonly TableRequestOptions RetryOptions =
            new TableRequestOptions
            {
                ServerTimeout = TimeSpan.FromSeconds(90),
                RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(10), 5)
            };

        private static readonly AccessCondition EmptyCondition = AccessCondition.GenerateEmptyCondition();

        //ok
        private static OperationContext CreateContext()
        {
            return new OperationContext();
        }

        //ok
        public static async Task<TResult> FindAllTablesAsync<TResult>(this CloudTableClient sourceClient, Func<CloudTable[], TResult> onSuccess, Func<string,TResult> onFailure)
        {
            var context = CreateContext();
            TableContinuationToken token = null;
            var tables = new List<CloudTable>();
            while (true)
            {
                try
                {
                    var segment = await sourceClient.ListTablesSegmentedAsync(null,
                        null, token, RetryOptions, context);
                    var results = segment.Results.ToArray();
                    tables.AddRange(results);
                    token = segment.ContinuationToken;
                    if (null == token)
                        return onSuccess(tables.ToArray());
                }
                catch (Exception e)
                {
                    return onFailure($"Exception listing all tables, Detail: {e.Message}");
                }
            }
        }

        public static async Task<KeyValuePair<string, TableStatistics>> CopyTableAsync(this CloudTable sourceTable, CloudTableClient targetClient, DateTime whenStartedUtc, TableCopyOptions copyOptions)
        {
            var targetTableName = sourceTable.Name;

            //return await await sourceContainer.CreateOrUpdateTargetContainerForCopyAsync(targetClient, targetContainerName,
            //    async (targetContainer, findExistingAsync, renewAccessAsync, releaseAccessAsync) =>
            //    {
            //        try
            //        {
            //            var existingTargetBlobs = await findExistingAsync();
            //            var pair = default(BlobContinuationToken).PairWithValue(ContainerStatistics.Default.Concat(existingTargetBlobs.Key));
            //            BlobAccess access = default(BlobAccess);
            //            Func<Task<BlobAccess>> renewWhenExpiredAsync =
            //                async () =>
            //                {
            //                    if (access.expiresUtc < DateTime.UtcNow + TimeSpan.FromMinutes(5))
            //                    {
            //                        access = await renewAccessAsync(copyOptions.accessPeriod);
            //                        await Task.Delay(TimeSpan.FromSeconds(10));  // let settle in so first copy will be ok
            //                    }
            //                    return access;
            //                };
            //            while (true)
            //            {
            //                pair = await await sourceContainer.FindNextBlobSegmentAsync<Task<KeyValuePair<BlobContinuationToken,ContainerStatistics>>>(pair.Key,
            //                    async (token, blobs) =>
            //                    {
            //                        var stats = await blobs.CopyBlobsWithContainerKeyAsync(targetContainer, existingTargetBlobs.Value, copyOptions.checkCopyCompleteAfter, copyOptions.maxWaitForCopyComplete, renewWhenExpiredAsync, copyOptions.maxBatch, copyOptions.maxConcurrency);
            //                        blobs = null;
            //                        return token.PairWithValue(pair.Value.Concat(stats));
            //                    },
            //                    why => default(BlobContinuationToken).PairWithValue(pair.Value.Concat(new[] { why })).ToTask());
            //                if (default(BlobContinuationToken) == pair.Key)
            //                {
            //                    if (pair.Value.retries.Any())
            //                    {
            //                        var copyRetries = copyOptions.copyRetries;
            //                        existingTargetBlobs = await findExistingAsync();
            //                        pair = default(BlobContinuationToken).PairWithValue(pair.Value.Concat(existingTargetBlobs.Key));
            //                        while (copyRetries-- > 0)
            //                        {
            //                            var stats = await pair.Value.retries
            //                                .Select(x => x.Key)
            //                                .ToArray()
            //                                .CopyBlobsWithContainerKeyAsync(targetContainer, existingTargetBlobs.Value, copyOptions.checkCopyCompleteAfter, copyOptions.maxWaitForCopyComplete, renewWhenExpiredAsync, copyOptions.maxBatch, copyOptions.maxConcurrency);
            //                            pair = default(BlobContinuationToken).PairWithValue(new ContainerStatistics
            //                            {
            //                                errors = pair.Value.errors.Concat(stats.errors).ToArray(),
            //                                successes = pair.Value.successes + stats.successes,
            //                                retries = stats.retries,
            //                                failures = pair.Value.failures.Concat(stats.failures).ToArray()
            //                            });
            //                            if (!pair.Value.retries.Any())
            //                                break;
            //                        }
            //                    }
            //                    return sourceContainer.Name.PairWithValue(pair.Value);
            //                }
            //            }
            //        }
            //        catch (Exception e)
            //        {
            //            return sourceContainer.Name.PairWithValue(ContainerStatistics.Default.Concat(new [] { $"Exception copying container, Detail: {e.Message}" }));
            //        }
            //        finally
            //        {
            //            await releaseAccessAsync();
            //        }
            //    },
            //    why => sourceContainer.Name.PairWithValue(ContainerStatistics.Default.Concat(new[] { why })).ToTask());
        }

        //private static string GetNameForCopy(this CloudBlobContainer sourceContainer, DateTime date)
        //{
        //    return $"{date:yyyyMMddHHmmss}-{sourceContainer.ServiceClient.Credentials.AccountName}-{sourceContainer.Name}";
        //}

        private static async Task<TResult> CreateOrUpdateTargetContainerForCopyAsync<TResult>(this CloudBlobContainer sourceContainer,
           CloudBlobClient blobClient, string targetContainerName,
           Func<CloudBlobContainer, Func<Task<KeyValuePair<string[],CloudBlob[]>>>, Func<TimeSpan, Task<BlobAccess>>, Func<Task>, TResult> onSuccess, Func<string,TResult> onFailure)
        {
            try
            {
                var targetContainer = blobClient.GetContainerReference(targetContainerName);
                var context = CreateContext();
                var exists = await targetContainer.ExistsAsync(RetryOptions, context);
                if (!exists)
                {
                    var createPermissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, RetryOptions, context);
                    await targetContainer.CreateAsync(createPermissions.PublicAccess, RetryOptions, context);

                    var metadataModified = false;
                    foreach (var item in sourceContainer.Metadata)
                    {
                        if (!targetContainer.Metadata.ContainsKey(item.Key) || targetContainer.Metadata[item.Key] != item.Value)
                        {
                            targetContainer.Metadata[item.Key] = item.Value;
                            metadataModified = true;
                        }
                    }
                    if (metadataModified)
                        await targetContainer.SetMetadataAsync(EmptyCondition, RetryOptions, context);
                }
                var keyName = $"{sourceContainer.ServiceClient.Credentials.AccountName}-{targetContainerName}-access";
                return onSuccess(targetContainer,
                    () =>
                    {
                        return targetContainer.FindAllBlobsAsync(
                            blobs => new string[] { }.PairWithValue(blobs),
                            (why, partialBlobList) => new[] { why }.PairWithValue(partialBlobList));
                    },
                    async (sourceAccessWindow) =>
                    {
                        var renewContext = CreateContext();
                        var permissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, RetryOptions, renewContext);
                        permissions.SharedAccessPolicies.Clear();
                        var access = new BlobAccess
                        {
                            key = keyName,
                            expiresUtc = DateTime.UtcNow.Add(sourceAccessWindow)
                        };
                        permissions.SharedAccessPolicies.Add(access.key, new SharedAccessBlobPolicy
                        {
                            SharedAccessExpiryTime = access.expiresUtc,
                            Permissions = SharedAccessBlobPermissions.Read
                        });
                        await sourceContainer.SetPermissionsAsync(permissions, EmptyCondition, RetryOptions, renewContext);
                        return access;
                    },
                    async () =>
                    {
                        var releaseContext = CreateContext();
                        var permissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, RetryOptions, releaseContext);
                        permissions.SharedAccessPolicies.Clear();
                        await sourceContainer.SetPermissionsAsync(permissions, EmptyCondition, RetryOptions, releaseContext);
                    });
            }
            catch (Exception e)
            {
                return onFailure($"Exception preparing container for copy, Detail: {e.Message}");
            }
        }

        private static async Task<TResult> FindAllBlobsAsync<TResult>(this CloudBlobContainer container, Func<CloudBlob[], TResult> onSuccess, Func<string, CloudBlob[], TResult> onFailure)
        {
            var context = CreateContext();
            BlobContinuationToken token = null;
            var blobs = new List<IListBlobItem>();
            while (true)
            {
                try
                {
                    var segment = await container.ListBlobsSegmentedAsync(null, true,
                        BlobListingDetails.UncommittedBlobs, null, token, RetryOptions, context);
                    var results = segment.Results.ToArray();
                    blobs.AddRange(results);
                    token = segment.ContinuationToken;
                    if (null == token)
                        return onSuccess(blobs.Cast<CloudBlob>().ToArray());
                }
                catch (Exception e)
                {
                    return onFailure($"Exception listing all blobs, Detail: {e.Message}", blobs.Cast<CloudBlob>().ToArray());
                }
            }
        }

        private static async Task<TResult> FindNextBlobSegmentAsync<TResult>(this CloudBlobContainer container, BlobContinuationToken token, Func<BlobContinuationToken, CloudBlob[], TResult> onSuccess, Func<string,TResult> onFailure)
        {
            var context = CreateContext();
            try
            {
                var segment = await container.ListBlobsSegmentedAsync(null, true,
                    BlobListingDetails.UncommittedBlobs, null, token, RetryOptions, context);
                var results = segment.Results.Cast<CloudBlob>().ToArray();
                token = segment.ContinuationToken;
                return onSuccess(token, results);
            }
            catch (Exception e)
            {
                //var inner = e;
                //while (inner.InnerException != null)
                //    inner = inner.InnerException;

                //var status = inner.Message.Contains("could not finish the operation within specified timeout") ? BlobStatus.ShouldRetry : BlobStatus.Failed;
               
                return onFailure($"Exception listing next blob segment, Detail: {e.Message}");
            }
        }

        private static async Task<ContainerStatistics> CopyBlobsWithContainerKeyAsync(this CloudBlob[] sourceBlobs, CloudBlobContainer targetContainer, CloudBlob[] existingTargetBlobs, TimeSpan checkCopyCompleteAfter, TimeSpan maxWaitForCopyComplete, Func<Task<BlobAccess>> renewAccessAsync, int maxBatch, int maxConcurrency)
        {
            return await sourceBlobs
                .Select((x, index) => new { x, index })
                .GroupBy(x => x.index / maxBatch, y => y.x)
                .Aggregate(
                    ContainerStatistics.Default.ToTask(),
                    async (statsTask, group) =>
                    {
                        var stats = await statsTask;
                        var access = await renewAccessAsync();
                        var items = await group
                            .ToArray()
                            .Select(blob => blob.StartCopyAndWaitForCompletionAsync(targetContainer, access.key, existingTargetBlobs, checkCopyCompleteAfter, maxWaitForCopyComplete))
                            .WhenAllAsync(maxConcurrency);
                        return new ContainerStatistics
                        {
                            errors = stats.errors,
                            successes = stats.successes + items.Count(pair => pair.Value == BlobStatus.CopySuccessful),
                            retries = stats.retries.Concat(items.Where(pair => pair.Value == BlobStatus.ShouldRetry)).ToArray(),
                            failures = stats.failures.Concat(items.Where(pair => pair.Value == BlobStatus.Failed)).ToArray(),
                        };
                    });
        }

        private static async Task<KeyValuePair<CloudBlob,BlobStatus>> StartCopyAndWaitForCompletionAsync(this CloudBlob sourceBlob, CloudBlobContainer targetContainer, string accessKey, CloudBlob[] existingTargetBlobs, TimeSpan checkCopyCompleteAfter, TimeSpan maxWaitForCopyComplete)
        {
            return await await sourceBlob.StartBackgroundCopyAsync(targetContainer, accessKey, existingTargetBlobs,
                async (started, progressAsync) =>
                {
                    try
                    {
                        var waitUntil = DateTime.UtcNow + maxWaitForCopyComplete;
                        while (true)
                        {
                            if (started)
                            {
                                if (waitUntil < DateTime.UtcNow)
                                    return sourceBlob.PairWithValue(BlobStatus.ShouldRetry);
                                await Task.Delay(checkCopyCompleteAfter);
                            }
                            var state = await progressAsync();
                            if (BlobStatus.Running == state)
                                continue;
                            return sourceBlob.PairWithValue(state);
                        }
                    }
                    catch (Exception e)
                    {
                        var inner = e;
                        while (inner.InnerException != null)
                            inner = inner.InnerException;

                        // This catches when our shared access key has expired after the copy has begun
                        var status = inner.Message.Contains("could not finish the operation within specified timeout") ? BlobStatus.ShouldRetry : BlobStatus.Failed;
                        return sourceBlob.PairWithValue(status);
                    }
                });
        }

        private static async Task<TResult> StartBackgroundCopyAsync<TResult>(this CloudBlob sourceBlob, CloudBlobContainer targetContainer, string accessKey, CloudBlob[] existingTargetBlobs,
            Func<bool, Func<Task<BlobStatus>>, TResult> onSuccess) // started, progressAsync
        {
            var started = true;
            var notStarted = !started;
            var target = existingTargetBlobs.FirstOrDefault(tb => tb.Name == sourceBlob.Name);
            if (null != target && target.Properties.ContentMD5 == sourceBlob.Properties.ContentMD5 && target.Properties.Length == sourceBlob.Properties.Length)
                return onSuccess(notStarted, () => BlobStatus.CopySuccessful.ToTask());

            target = targetContainer.GetReference(sourceBlob.BlobType, sourceBlob.Name);
            var sas = sourceBlob.GetSharedAccessSignature(null, accessKey);
            try
            {
                await target.StartCopyAsync(new Uri(sourceBlob.Uri + sas), EmptyCondition, EmptyCondition, RetryOptions, CreateContext());
                return onSuccess(started,
                    async () =>
                    {
                        var blob = (await targetContainer.GetBlobReferenceFromServerAsync(sourceBlob.Name, AccessCondition.GenerateEmptyCondition(), RetryOptions, CreateContext())) as CloudBlob;
                        var copyStatus = blob.CopyState?.Status ?? CopyStatus.Invalid;
                        if (CopyStatus.Success == copyStatus)
                            return BlobStatus.CopySuccessful;
                        if (CopyStatus.Pending == copyStatus)
                            return BlobStatus.Running;
                        // This catches when the shared access key expired before the blob finished copying
                        if (CopyStatus.Failed == copyStatus && blob.CopyState.StatusDescription.Contains("Copy failed when reading the source"))
                            return BlobStatus.ShouldRetry;
                        return BlobStatus.Failed;
                    });
            }
            catch (Exception e)
            {
                var inner = e;
                while (inner.InnerException != null)
                    inner = inner.InnerException;

                // This catches when our shared access key has expired before requesting the copy to start
                var webException = inner as System.Net.WebException;
                var blobStatus = (webException != null && webException.Status == WebExceptionStatus.ProtocolError) || 
                    inner.Message.Contains("could not finish the operation within specified timeout") ? BlobStatus.ShouldRetry : BlobStatus.Failed;
                return onSuccess(notStarted, () => blobStatus.ToTask());
            }
        }
    }
}
