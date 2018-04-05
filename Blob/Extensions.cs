using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using BlackBarLabs;
using BlackBarLabs.Extensions;
using EastFive.Azure.Storage.Backup.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace EastFive.Azure.Storage.Backup.Blob
{
    public static class Extensions
    {
        private struct BlobAccess
        {
            public string error;
            public string key;
            public DateTime expiresUtc;
        }

        private struct SparseCloudBlob
        {
            public string contentMD5;
            public long length;
        }

        private static readonly AccessCondition EmptyCondition = AccessCondition.GenerateEmptyCondition();

        public static async Task<TResult> CopyAccountAsync<TResult>(this BlobCopyOptions options, CloudStorageAccount sourceAccount, CloudStorageAccount targetAccount, Func<bool> stopCalled, Func<string[], TResult> onCompleted)
        {
            var sourceClient = sourceAccount.CreateCloudBlobClient();
            var targetClient = targetAccount.CreateCloudBlobClient();
            return onCompleted(await await sourceClient.FindAllContainersAsync(stopCalled,
                async sourceContainers =>
                {
                    var stats = await sourceContainers
                        .Select(sourceContainer => sourceContainer.CopyContainerAsync(targetClient, options, stopCalled))
                        .WhenAllAsync(options.maxContainerConcurrency);
                    return stats
                        .SelectMany(s => s.Value.errors)
                        .ToArray();
                },
                why => new[] { why }.ToTask()));
        }

        private static async Task<TResult> FindAllContainersAsync<TResult>(this CloudBlobClient sourceClient, Func<bool> stopCalled, Func<CloudBlobContainer[], TResult> onSuccess, Func<string,TResult> onFailure)
        {
            var context = new OperationContext();
            BlobContinuationToken token = null;
            var containers = new List<CloudBlobContainer>();
            while (true)
            {
                if (stopCalled())
                    return onFailure($"listing containers stopped on {sourceClient.Credentials.AccountName}");
                try
                {
                    var segment = await sourceClient.ListContainersSegmentedAsync(null, ContainerListingDetails.All, 
                        null, token, BlobCopyOptions.requestOptions, context);
                    var results = segment.Results.ToArray();
                    containers.AddRange(results);
                    token = segment.ContinuationToken;
                    if (null == token)
                        return onSuccess(containers.ToArray());
                }
                catch (Exception e)
                {
                    return onFailure($"Exception listing all containers, Detail: {e.Message}");
                }
            }
        }

        private static async Task<KeyValuePair<string, BlobTransferStatistics>> CopyContainerAsync(this CloudBlobContainer sourceContainer, CloudBlobClient targetClient, BlobCopyOptions copyOptions, Func<bool> stopCalled)
        {
            var targetContainerName = sourceContainer.Name;
            if (stopCalled())
                return sourceContainer.Name.PairWithValue(BlobTransferStatistics.Default.Concat(new[] { $"copy stopped on {targetContainerName}" }));

            return await await sourceContainer.CreateIfNotExistTargetContainerForCopyAsync(targetClient, targetContainerName,
                async (targetContainer, findExistingAsync, renewAccessAsync, releaseAccessAsync) =>
                {
                    EastFiveAzureStorageBackupService.Log.Info($"starting {targetContainerName}");
                    try
                    {
                        var existingTargetBlobs = await findExistingAsync(stopCalled);
                        EastFiveAzureStorageBackupService.Log.Info($"{existingTargetBlobs.Value.Count} blobs already backed up for {targetContainerName}");
                        var pair = default(BlobContinuationToken).PairWithValue(BlobTransferStatistics.Default.Concat(existingTargetBlobs.Key));
                        BlobAccess access = default(BlobAccess);
                        Func<Task<BlobAccess>> renewWhenExpiredAsync =
                            async () =>
                            {
                                if (access.expiresUtc < DateTime.UtcNow + TimeSpan.FromMinutes(5))
                                {
                                    access = await renewAccessAsync(copyOptions.accessPeriod);
                                    await Task.Delay(TimeSpan.FromSeconds(10));  // let settle in so first copy will be ok
                                }
                                return access;
                            };
                        while (true)
                        {
                            if (stopCalled())
                                return sourceContainer.Name.PairWithValue(pair.Value.Concat(new[] { $"copy stopped on {sourceContainer.Name}" }));

                            pair = await await sourceContainer.FindNextBlobSegmentAsync<Task<KeyValuePair<BlobContinuationToken,BlobTransferStatistics>>>(pair.Key,
                                async (token, blobs) =>
                                {
                                    var stats = await blobs.CopyBlobsWithContainerKeyAsync(targetContainer, existingTargetBlobs.Value, 
                                        () => pair.Value.calc.GetNextInterval(copyOptions.minIntervalCheckCopy, copyOptions.maxIntervalCheckCopy), 
                                        copyOptions.maxTotalWaitForCopy, renewWhenExpiredAsync, copyOptions.maxBlobConcurrencyPerContainer, stopCalled);
                                    blobs = null;
                                    return token.PairWithValue(pair.Value.Concat(stats));
                                },
                                why => default(BlobContinuationToken).PairWithValue(pair.Value.Concat(new[] { why })).ToTask());
                            pair.Value.LogProgress(copyOptions.minIntervalCheckCopy, copyOptions.maxIntervalCheckCopy,
                                msg => EastFiveAzureStorageBackupService.Log.Info($"(progress) {targetContainerName} -> {msg}"));

                            if (default(BlobContinuationToken) == pair.Key)
                            {
                                if (pair.Value.retries.Any())
                                {
                                    EastFiveAzureStorageBackupService.Log.Info($"retrying {targetContainerName}");
                                    var copyRetries = copyOptions.copyRetries;
                                    existingTargetBlobs = await findExistingAsync(stopCalled);
                                    EastFiveAzureStorageBackupService.Log.Info($"{existingTargetBlobs.Value.Count} blobs already backed up for {targetContainerName}");
                                    pair = default(BlobContinuationToken).PairWithValue(pair.Value.Concat(existingTargetBlobs.Key)); // just copies errors
                                    var checkCopyCompleteAfter = pair.Value.calc.GetNextInterval(copyOptions.minIntervalCheckCopy, copyOptions.maxIntervalCheckCopy);
                                    while (copyRetries-- > 0)
                                    {
                                        if (stopCalled())
                                        {
                                            pair = default(BlobContinuationToken).PairWithValue(pair.Value.Concat(new[] { $"copy stopped on {sourceContainer.Name}" }));
                                            break;
                                        }
                                        var stats = await pair.Value.retries
                                            .Select(x => x.Key)
                                            .ToArray()
                                            .CopyBlobsWithContainerKeyAsync(targetContainer, existingTargetBlobs.Value, 
                                                () => pair.Value.calc.GetNextInterval(copyOptions.minIntervalCheckCopy, copyOptions.maxIntervalCheckCopy), 
                                                copyOptions.maxTotalWaitForCopy, renewWhenExpiredAsync, copyOptions.maxBlobConcurrencyPerContainer, stopCalled);
                                        pair = default(BlobContinuationToken).PairWithValue(new BlobTransferStatistics
                                        {
                                            calc = pair.Value.calc.Concat(stats.calc),
                                            errors = pair.Value.errors.Concat(stats.errors).ToArray(),
                                            successes = pair.Value.successes + stats.successes,
                                            retries = stats.retries,
                                            failures = pair.Value.failures.Concat(stats.failures).ToArray()
                                        });
                                        if (!pair.Value.retries.Any())
                                            break;
                                    }
                                }
                                pair.Value.LogProgress(copyOptions.minIntervalCheckCopy, copyOptions.maxIntervalCheckCopy,
                                    msg => EastFiveAzureStorageBackupService.Log.Info($"finished {targetContainerName} -> {msg}"),
                                    BlobTransferStatistics.logFrequency);
                                return sourceContainer.Name.PairWithValue(pair.Value);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        return sourceContainer.Name.PairWithValue(BlobTransferStatistics.Default.Concat(new [] { $"Exception copying container, Detail: {e.Message}" }));
                    }
                    finally
                    {
                        await releaseAccessAsync();
                        EastFiveAzureStorageBackupService.Log.Info($"done with {targetContainerName}");
                    }
                },
                why => sourceContainer.Name.PairWithValue(BlobTransferStatistics.Default.Concat(new[] { why })).ToTask());
        }

        private static async Task<TResult> CreateIfNotExistTargetContainerForCopyAsync<TResult>(this CloudBlobContainer sourceContainer,
           CloudBlobClient targetClient, string targetContainerName,
           Func<CloudBlobContainer, Func<Func<bool>, Task<KeyValuePair<string[],IDictionary<string, SparseCloudBlob>>>>, Func<TimeSpan, Task<BlobAccess>>, Func<Task>, TResult> onSuccess, Func<string,TResult> onFailure)
        {
            try
            {
                var targetContainer = targetClient.GetContainerReference(targetContainerName);
                var context = new OperationContext();
                var exists = await targetContainer.ExistsAsync(BlobCopyOptions.requestOptions, context);
                if (!exists)
                {
                    var createPermissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, BlobCopyOptions.requestOptions, context);
                    await targetContainer.CreateAsync(createPermissions.PublicAccess, BlobCopyOptions.requestOptions, context);

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
                        await targetContainer.SetMetadataAsync(EmptyCondition, BlobCopyOptions.requestOptions, context);
                }
                var keyName = $"{sourceContainer.ServiceClient.Credentials.AccountName}-{targetContainerName}-access";
                return onSuccess(targetContainer,
                    (stopCalled) =>
                    {
                        return targetContainer.FindAllBlobsAsync(stopCalled,
                            blobs => new string[] { }.PairWithValue(blobs),
                            (why, partialBlobList) => new[] { why }.PairWithValue(partialBlobList));
                    },
                    async (sourceAccessWindow) =>
                    {
                        try
                        {
                            var renewContext = new OperationContext();
                            var permissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, BlobCopyOptions.requestOptions, renewContext);
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
                            await sourceContainer.SetPermissionsAsync(permissions, EmptyCondition, BlobCopyOptions.requestOptions, renewContext);
                            return access;
                        }
                        catch (Exception e)
                        {
                            return new BlobAccess { error = $"Error renewing access policy on container, Detail: {e.Message}" };
                        }
                    },
                    async () =>
                    {
                        try
                        {
                            var releaseContext = new OperationContext();
                            var permissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, BlobCopyOptions.requestOptions, releaseContext);
                            permissions.SharedAccessPolicies.Clear();
                            await sourceContainer.SetPermissionsAsync(permissions, EmptyCondition, BlobCopyOptions.requestOptions, releaseContext);
                        }
                        catch (Exception e)
                        {
                            // Failure here is no big deal as the container will still be usable
                            return;
                        }
                    });
            }
            catch (Exception e)
            {
                return onFailure($"Exception preparing container for copy, Detail: {e.Message}");
            }
        }

        private static async Task<TResult> FindAllBlobsAsync<TResult>(this CloudBlobContainer targetContainer, Func<bool> stopCalled, Func<IDictionary<string,SparseCloudBlob>, TResult> onSuccess, Func<string, IDictionary<string, SparseCloudBlob>, TResult> onFailure)
        {
            var context = new OperationContext();
            BlobContinuationToken token = null;
            var dict = new Dictionary<string,SparseCloudBlob>(5000);
            var retryTimes = ServiceSettings.defaultMaxAttempts;
            while (true)
            {
                try
                {
                    if (stopCalled())
                        return onFailure($"copy stopped on {targetContainer.Name}", dict);

                    var segment = await targetContainer.ListBlobsSegmentedAsync(null, true,
                        BlobListingDetails.UncommittedBlobs, null, token, BlobCopyOptions.requestOptions, context);
                    foreach (CloudBlob blob in segment.Results)
                    {
                        dict[blob.Name] = new SparseCloudBlob
                        {
                            contentMD5 = blob.Properties.ContentMD5,
                            length = blob.Properties.Length
                        };
                    }
                    token = segment.ContinuationToken;
                    if (null == token)
                        return onSuccess(dict);
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("could not finish the operation within specified timeout") && --retryTimes > 0)
                    {
                        await Task.Delay(ServiceSettings.defaultBackoff);
                        continue;
                    }
                    return onFailure($"Exception listing all blobs, Detail: {e.Message}", dict);
                }
            }
        }

        private static async Task<TResult> FindNextBlobSegmentAsync<TResult>(this CloudBlobContainer sourceContainer, BlobContinuationToken token, Func<BlobContinuationToken, CloudBlob[], TResult> onSuccess, Func<string,TResult> onFailure)
        {
            var context = new OperationContext();
            var retryTimes = ServiceSettings.defaultMaxAttempts;
            while (true)
            {
                try
                {
                    var segment = await sourceContainer.ListBlobsSegmentedAsync(null, true,
                        BlobListingDetails.UncommittedBlobs, null, token, BlobCopyOptions.requestOptions, context);
                    var results = segment.Results.Cast<CloudBlob>().ToArray();
                    token = segment.ContinuationToken;
                    return onSuccess(token, results);
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("could not finish the operation within specified timeout") && --retryTimes > 0)
                    {
                        await Task.Delay(ServiceSettings.defaultBackoff);
                        continue;
                    }
                    return onFailure($"Exception listing next blob segment, Detail: {e.Message}");
                }
            }
        }

        private static async Task<BlobTransferStatistics> CopyBlobsWithContainerKeyAsync(this CloudBlob[] sourceBlobs, CloudBlobContainer targetContainer, IDictionary<string, SparseCloudBlob> existingTargetBlobs, Func<TimeSpan> getCheckInterval, TimeSpan maxTotalWaitForCopy, Func<Task<BlobAccess>> renewAccessAsync, int maxConcurrency, Func<bool> stopCalled)
        {
            var access = await renewAccessAsync();
            if (!string.IsNullOrEmpty(access.error))
            {
                return BlobTransferStatistics.Default.Concat(new[] { access.error });
            }
            var checkInterval = getCheckInterval();
            var items = await sourceBlobs
                .Select(blob => blob.StartCopyAndWaitForCompletionAsync(targetContainer, access.key, existingTargetBlobs, checkInterval, maxTotalWaitForCopy, stopCalled))
                .WhenAllAsync(maxConcurrency);

            return new BlobTransferStatistics
            {
                calc = IntervalCalculator.Default.Concat(checkInterval, items.Select(x => x.Item3).ToArray()),
                errors = new string[] { },
                successes = items.Count(item => item.Item2 == TransferStatus.CopySuccessful),
                retries = items.Where(item => item.Item2 == TransferStatus.ShouldRetry).Select(item => item.Item1.PairWithValue(item.Item2)).ToArray(),
                failures = items.Where(item => item.Item2 == TransferStatus.Failed).Select(item => item.Item1.PairWithValue(item.Item2)).ToArray()
            };
        }

        private static async Task<Tuple<CloudBlob,TransferStatus,int>> StartCopyAndWaitForCompletionAsync(this CloudBlob sourceBlob, CloudBlobContainer targetContainer, string accessKey, IDictionary<string, SparseCloudBlob> existingTargetBlobs, TimeSpan checkInterval, TimeSpan maxTotalWaitForCopy, Func<bool> stopCalled)
        {
            return await await sourceBlob.StartBackgroundCopyAsync(targetContainer, accessKey, existingTargetBlobs,
                async (started, progressAsync) =>
                {
                    try
                    {
                        var waitUntilLocal = DateTime.Now + maxTotalWaitForCopy;
                        var cycles = 0;
                        while (true)
                        {
                            if (started)
                            {
                                if (waitUntilLocal < DateTime.Now || stopCalled())
                                    return new Tuple<CloudBlob,TransferStatus,int>(sourceBlob,TransferStatus.ShouldRetry,cycles);
                                await Task.Delay(checkInterval);
                                cycles++;
                            }
                            var status = await progressAsync();
                            if (TransferStatus.Running == status)
                                continue;
                            return new Tuple<CloudBlob, TransferStatus, int>(sourceBlob, status, cycles);
                        }
                    }
                    catch (Exception e)
                    {
                        var inner = e;
                        while (inner.InnerException != null)
                            inner = inner.InnerException;

                        // This catches when our shared access key has expired after the copy has begun
                        var status = inner.Message.Contains("could not finish the operation within specified timeout") ? TransferStatus.ShouldRetry : TransferStatus.Failed;
                        return new Tuple<CloudBlob, TransferStatus, int>(sourceBlob, status, 3); // 3 is arbitrary here just to make these failures more weighty
                    }
                });
        }

        private static async Task<TResult> StartBackgroundCopyAsync<TResult>(this CloudBlob sourceBlob, CloudBlobContainer targetContainer, string accessKey, IDictionary<string, SparseCloudBlob> existingTargetBlobs,
            Func<bool, Func<Task<TransferStatus>>, TResult> onSuccess) // started, progressAsync
        {
            var started = true;
            var notStarted = !started;
            if (existingTargetBlobs.TryGetValue(sourceBlob.Name, out SparseCloudBlob existingTarget) &&
                existingTarget.contentMD5 == sourceBlob.Properties.ContentMD5 &&
                existingTarget.length == sourceBlob.Properties.Length)
            {
                return onSuccess(notStarted, () => TransferStatus.CopySuccessful.ToTask());
            }

            var target = targetContainer.GetReference(sourceBlob.BlobType, sourceBlob.Name);
            var sas = sourceBlob.GetSharedAccessSignature(null, accessKey);
            try
            {
                await target.StartCopyAsync(new Uri(sourceBlob.Uri + sas), EmptyCondition, EmptyCondition, BlobCopyOptions.requestOptions, new OperationContext());
                return onSuccess(started,
                    async () =>
                    {
                        var blob = (await targetContainer.GetBlobReferenceFromServerAsync(sourceBlob.Name, AccessCondition.GenerateEmptyCondition(), BlobCopyOptions.requestOptions, new OperationContext())) as CloudBlob;
                        var copyStatus = blob.CopyState?.Status ?? CopyStatus.Invalid;
                        if (CopyStatus.Success == copyStatus)
                            return TransferStatus.CopySuccessful;
                        if (CopyStatus.Pending == copyStatus)
                            return TransferStatus.Running;
                        // This catches when the shared access key expired before the blob finished copying
                        if (CopyStatus.Failed == copyStatus && blob.CopyState.StatusDescription.Contains("Copy failed when reading the source"))
                            return TransferStatus.ShouldRetry;
                        return TransferStatus.Failed;
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
                    inner.Message.Contains("could not finish the operation within specified timeout") ? TransferStatus.ShouldRetry : TransferStatus.Failed;
                return onSuccess(notStarted, () => blobStatus.ToTask());
            }
        }

        public static CloudBlob GetReference(this CloudBlobContainer container, BlobType type, string name)
        {
            switch (type)
            {
                case BlobType.BlockBlob:
                    return container.GetBlockBlobReference(name);
                case BlobType.PageBlob:
                    return container.GetPageBlobReference(name);
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
