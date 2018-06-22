using BlackBarLabs;
using BlackBarLabs.Extensions;
using EastFive.Azure.Storage.Backup.Configuration;
using EastFive.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace EastFive.Azure.Storage.Backup.Table
{
    public static class Extensions
    {
        private struct SparseEntity
        {
            public DateTimeOffset timeStamp;
            public string partitionKey;
        }

        private struct SegmentedQuery
        {
            public TableContinuationToken token;
            public TableQuery<DynamicTableEntity> query;

            public static SegmentedQuery Default = new SegmentedQuery
            {
                token = default(TableContinuationToken),
                query = new TableQuery<DynamicTableEntity>()
            };

            public static SegmentedQuery[] GetForPartitionKeys(string[] partitionKeys)
            {
                return partitionKeys
                .Select(
                    key => new SegmentedQuery
                    {
                        token = default(TableContinuationToken),
                        query = new TableQuery<DynamicTableEntity>()
                            .Where(TableQuery.GenerateFilterCondition("PartitionKey",QueryComparisons.Equal,key))
                    })
                .ToArray();
            }

            public SegmentedQuery UpdateToken(TableContinuationToken token)
            {
                return new SegmentedQuery
                {
                    token = token,
                    query = this.query
                };
            }
        }

        private struct SegmentedQueryResult
        {
            public SegmentedQuery[] details;
            public DynamicTableEntity[] rows;
        }

        public static async Task<TResult> CopyAccountAsync<TResult>(this TableCopyOptions options, CloudStorageAccount sourceAccount, CloudStorageAccount targetAccount, Func<bool> stopCalled, Func<string[], TResult> onCompleted)
        {
            var sourceClient = sourceAccount.CreateCloudTableClient();
            var targetClient = targetAccount.CreateCloudTableClient();
            return onCompleted(await await sourceClient.FindAllTablesAsync(stopCalled,
                async sourceTables =>
                {
                    var stats = await sourceTables
                        .Where(sourceTable => options.ShouldCopy(sourceTable.Name))
                        .Select(sourceTable => sourceTable.CopyTableAsync(targetClient, options, stopCalled))
                        .WhenAllAsync(options.maxTableConcurrency);
                    return stats
                        .SelectMany(s => s.Value.errors)
                        .ToArray();
                },
                why => new[] { why }.ToTask()));
        }

        private static async Task<TResult> FindAllTablesAsync<TResult>(this CloudTableClient sourceClient, Func<bool> stopCalled, Func<CloudTable[], TResult> onSuccess, Func<string, TResult> onFailure)
        {
            var context = new OperationContext();
            TableContinuationToken token = null;
            var tables = new List<CloudTable>();
            while (true)
            {
                if (stopCalled())
                    return onFailure($"listing tables stopped on {sourceClient.Credentials.AccountName}");
                try
                {
                    var segment = await sourceClient.ListTablesSegmentedAsync(null,
                        null, token, TableCopyOptions.requestOptions, context);
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

        private static async Task<KeyValuePair<string, TableTransferStatistics>> CopyTableAsync(this CloudTable sourceTable, CloudTableClient targetClient, TableCopyOptions copyOptions, Func<bool> stopCalled)
        {
            var targetTableName = sourceTable.Name;
            if (stopCalled())
                return sourceTable.Name.PairWithValue(TableTransferStatistics.Default.Concat(new[] { $"copy stopped on {targetTableName}" }));

            return await await sourceTable.CreateIfNotExistTargetTableForCopyAsync(targetClient, targetTableName, copyOptions.partitionKeys, copyOptions.maxSegmentDownloadConcurrencyPerTable,
                async (targetTable, findExistingAsync, getNextSegmentAsync) =>
                {
                    EastFiveAzureStorageBackupService.Log.Info($"starting {targetTableName}");
                    try
                    {
                        var existingTargetRows = await findExistingAsync(stopCalled);
                        EastFiveAzureStorageBackupService.Log.Info($"{existingTargetRows.Value.Count} entities already backed up for {targetTableName}");
                        var pair = new SegmentedQuery[] { }.PairWithValue(TableTransferStatistics.Default.Concat(existingTargetRows.Key));  // just copies errors
                        while (true)
                        {
                            if (stopCalled())
                                return sourceTable.Name.PairWithValue(pair.Value);

                            var nextSourceRows = await getNextSegmentAsync(pair.Key, stopCalled);
                            pair = nextSourceRows.Value.details
                                .PairWithValue(pair.Value.Concat(nextSourceRows.Key) // just copies errors
                                    .Concat(await nextSourceRows.Value.rows.CopyRowsAsync(targetTable, existingTargetRows.Value, copyOptions.maxRowUploadConcurrencyPerTable)));  // gets result of work

                            pair.Value.LogProgress(
                                msg => EastFiveAzureStorageBackupService.Log.Info($"(progress) {targetTableName} -> {msg}"));

                            if (!pair.Key.Any()) // wait until all the tokens are gone
                            {
                                if (pair.Value.retries.Any())
                                {
                                    EastFiveAzureStorageBackupService.Log.Info($"retrying {targetTableName}");
                                    var copyRetries = copyOptions.copyRetries;
                                    existingTargetRows = await findExistingAsync(stopCalled);
                                    EastFiveAzureStorageBackupService.Log.Info($"{existingTargetRows.Value.Count} entities already backed up for {targetTableName}");
                                    pair = new SegmentedQuery[] { }.PairWithValue(pair.Value.Concat(existingTargetRows.Key));  // just copies errors
                                    while (copyRetries-- > 0)
                                    {
                                        if (stopCalled())
                                            break;

                                        var stats = await pair.Value.retries
                                            .Select(x => x.Key)
                                            .ToArray()
                                            .CopyRowsAsync(targetTable, existingTargetRows.Value, copyOptions.maxRowUploadConcurrencyPerTable);
                                        pair = new SegmentedQuery[] { }.PairWithValue(new TableTransferStatistics
                                        {
                                            errors = pair.Value.errors.Concat(stats.errors).ToArray(),
                                            successes = pair.Value.successes + stats.successes,
                                            retries = stats.retries
                                        });
                                        if (!pair.Value.retries.Any())
                                            break;
                                    }
                                }
                                pair.Value.LogProgress(
                                    msg => EastFiveAzureStorageBackupService.Log.Info($"finished {targetTableName} -> {msg}"),
                                    TableTransferStatistics.logFrequency);
                                return sourceTable.Name.PairWithValue(pair.Value);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        return sourceTable.Name.PairWithValue(TableTransferStatistics.Default.Concat(new[] { $"Exception copying table, Detail: {e.Message}" }));
                    }
                },
                why => sourceTable.Name.PairWithValue(TableTransferStatistics.Default.Concat(new[] { why })).ToTask());
        }

        private static async Task<TResult> CreateIfNotExistTargetTableForCopyAsync<TResult>(this CloudTable sourceTable,
            CloudTableClient targetClient, string targetTableName, string[] partitionKeys, int numberOfSegments,
            Func<CloudTable, Func<Func<bool>,Task<KeyValuePair<string[], IDictionary<string, SparseEntity>>>>, Func<SegmentedQuery[], Func<bool>, Task<KeyValuePair<string[],SegmentedQueryResult>>>, TResult> onSuccess,
            Func<string, TResult> onFailure)
        {
            try
            {
                var targetTable = targetClient.GetTableReference(targetTableName);
                var context = new OperationContext();
                var exists = await targetTable.ExistsAsync(TableCopyOptions.requestOptions, context);
                if (!exists)
                {
                    await targetTable.CreateAsync(TableCopyOptions.requestOptions, context);
                    var createPermissions = await sourceTable.GetPermissionsAsync(TableCopyOptions.requestOptions, context);
                    await targetTable.SetPermissionsAsync(createPermissions, TableCopyOptions.requestOptions, context);
                }
                return onSuccess(targetTable,
                    (stopCalled) =>
                    {
                        if (partitionKeys != null && partitionKeys.Length > 0)
                            return targetTable.FindAllRowsByPartitionKeysAsync(partitionKeys, stopCalled,
                                rows => new string[] { }.PairWithValue(rows),
                                (why, partialRowList) => new[] { why }.PairWithValue(partialRowList));
                        else
                            return targetTable.FindAllRowsByQueryAsync(
                                new TableQuery<DynamicTableEntity>().Select(new string[] { "PartitionKey" }), // we are not interested in the properties of the target so don't download them
                                stopCalled,
                                rows => new string[] { }.PairWithValue((IDictionary<string,SparseEntity>)rows.ToDictionary()),
                                (why, partialRowList) => new[] { why }.PairWithValue((IDictionary<string, SparseEntity>)partialRowList.ToDictionary()));
                    },
                    (details, stopCalled) =>
                    {
                        if (partitionKeys != null && partitionKeys.Length > 0)
                        {
                            var innerDetails = details.Any() ? details : SegmentedQuery.GetForPartitionKeys(partitionKeys);  // init the run
                            return sourceTable.FindNextTableSegmentByPartitionKeysAsync(innerDetails, numberOfSegments, stopCalled,
                                result => new string[] { }.PairWithValue(result),
                                (why, result) => new[] { why }.PairWithValue(result));
                        }
                        else
                        {
                            var innerDetails = details.Any() ? details.First() : SegmentedQuery.Default;  // init the run
                            return sourceTable.FindNextTableSegmentByQueryAsync(innerDetails, numberOfSegments, stopCalled,
                              (result) => new string[] { }.PairWithValue(result),
                              (why, result) => new[] { why }.PairWithValue(result));
                        }
                    });
            }
            catch (Exception e)
            {
                return onFailure($"Exception preparing table for copy, Detail: {e.Message}");
            }
        }

        private static async Task<TResult> FindNextTableSegmentByPartitionKeysAsync<TResult>(this CloudTable sourceTable, SegmentedQuery[] details, int numberOfSegments, Func<bool> stopCalled, Func<SegmentedQueryResult, TResult> onSuccess, Func<string, SegmentedQueryResult, TResult> onFailure)
        {
            var errorsWithResults = await details
                .Select(
                    detail =>
                    {
                        return sourceTable.FindNextTableSegmentByQueryAsync(detail, numberOfSegments, stopCalled,
                            x => string.Empty.PairWithValue(x),
                            (why, x) => why.PairWithValue(x));
                    })
                .WhenAllAsync();

            var error = errorsWithResults
                .Select(x => x.Key)
                .Where(x => !string.IsNullOrEmpty(x))
                .Join(",");
            var result = new SegmentedQueryResult
            {
                details = errorsWithResults
                    .SelectMany(x => x.Value.details)
                    .ToArray(),
                rows = errorsWithResults
                    .SelectMany(x => x.Value.rows)
                    .ToArray()
            };
            if (string.IsNullOrEmpty(error))
                return onSuccess(result);

            return onFailure(error, result);
        }

        private static async Task<TResult> FindNextTableSegmentByQueryAsync<TResult>(this CloudTable sourceTable, SegmentedQuery detail, int numberOfSegments, Func<bool> stopCalled,
            Func<SegmentedQueryResult, TResult> onSuccess, Func<string, SegmentedQueryResult, TResult> onFailure)
        {
            var context = new OperationContext();
            var list = new List<DynamicTableEntity>(numberOfSegments * 1000);
            var retryTimes = ServiceSettings.defaultMaxAttempts;
            while (true)
            {
                try
                {
                    if (stopCalled())
                        return onFailure($"stopped finding segments for {sourceTable.Name}", new SegmentedQueryResult
                            {
                                details = new SegmentedQuery[] { },
                                rows = list.ToArray()
                            });

                    var segment = await sourceTable.ExecuteQuerySegmentedAsync(detail.query, detail.token, TableCopyOptions.requestOptions, context);
                    list.AddRange(segment.Results);
                    detail = detail.UpdateToken(segment.ContinuationToken);
                    if (detail.token == null)
                        return onSuccess(new SegmentedQueryResult
                        {
                            details = new SegmentedQuery[] { },
                            rows = list.ToArray()
                        });
                    if (--numberOfSegments < 1)
                        return onSuccess(new SegmentedQueryResult
                        {
                            details = new[] { detail },
                            rows = list.ToArray()
                        });
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("could not finish the operation within specified timeout") && --retryTimes > 0)
                    {
                        await Task.Delay(ServiceSettings.defaultBackoff);
                        continue;
                    }
                    return onFailure($"Exception retrieving next table segment, Detail: {e.Message}", new SegmentedQueryResult
                        {
                            details = new SegmentedQuery[] { },
                            rows = list.ToArray()
                        });
                }
            }
        }

        private static async Task<TResult> FindAllRowsByPartitionKeysAsync<TResult>(this CloudTable targetTable, string[] partitionKeys, Func<bool> stopCalled, Func<IDictionary<string, SparseEntity>, TResult> onSuccess, Func<string, IDictionary<string, SparseEntity>, TResult> onFailure)
        {
            // Splits request by partition key for parallel download
            var errorsWithResults = await partitionKeys
                .Select(
                    key =>
                    {
                        var query = new TableQuery<DynamicTableEntity>()
                            .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, key))
                            .Select(new string[] { "PartitionKey" }); // we are not interested in the properties of the target so don't download them

                        return targetTable.FindAllRowsByQueryAsync(query, stopCalled,
                            rows => string.Empty.PairWithValue(rows),
                            (why, rows) => why.PairWithValue(rows));
                    })
                .WhenAllAsync();

            var error = errorsWithResults
                .Select(x => x.Key)
                .Where(x => !string.IsNullOrEmpty(x))
                .Join(",");
            var results = errorsWithResults
                .SelectMany(x => x.Value)
                .ToDictionary();
            if (string.IsNullOrEmpty(error))
                return onSuccess(results);

            return onFailure(error, results);
        }

        private static async Task<TResult> FindAllRowsByQueryAsync<TResult>(this CloudTable targetTable, TableQuery<DynamicTableEntity> query, Func<bool> stopCalled, Func<IEnumerable<KeyValuePair<string,SparseEntity>>, TResult> onSuccess, Func<string, IEnumerable<KeyValuePair<string, SparseEntity>>, TResult> onFailure)
        {
            var context = new OperationContext();
            TableContinuationToken token = null;
            var list = new List<KeyValuePair<string, SparseEntity>>(1000);
            var retryTimes = ServiceSettings.defaultMaxAttempts;
            while (true)
            {
                try
                {
                    if (stopCalled())
                        return onFailure($"stopped listing all rows for {targetTable.Name}", list);

                    var segment = await targetTable.ExecuteQuerySegmentedAsync(query, token, TableCopyOptions.requestOptions, context);
                    segment.Results.ForEach(
                        row => list.Add(row.RowKey.PairWithValue(
                            new SparseEntity
                            {
                                timeStamp = row.Timestamp,
                                partitionKey = row.PartitionKey
                            })));
                    token = segment.ContinuationToken;
                    if (null == token)
                        return onSuccess(list);
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("could not finish the operation within specified timeout") && --retryTimes > 0)
                    {
                        await Task.Delay(ServiceSettings.defaultBackoff);
                        continue;
                    }
                    return onFailure($"Exception listing all rows, Detail: {e.Message}", list);
                }
            }
        }

        private static async Task<TableTransferStatistics> CopyRowsAsync(this DynamicTableEntity[] sourceRows, CloudTable targetTable, IDictionary<string, SparseEntity> existingTargetRows, int maxRowUpload)
        {
            var toCopy = sourceRows
                .Where(row => !existingTargetRows.TryGetValue(row.RowKey, out SparseEntity existing) ||
                    existing.timeStamp < row.Timestamp || existing.partitionKey != row.PartitionKey)
                .ToArray();
            var stats = new TableTransferStatistics
            {
                errors = new string[] { },
                successes = sourceRows.Length - toCopy.Length,
                retries = new KeyValuePair<DynamicTableEntity, TransferStatus>[] { }
            };
            if (toCopy.Length == 0)
                return stats;

            return await toCopy
                .GroupBy(row => row.PartitionKey)
                .SelectMany(group => group
                    .ToArray()
                    .Select((x, index) => new { x, index })
                    .GroupBy(x => x.index / maxRowUpload, y => y.x))
                .Aggregate(stats.ToTask(),
                    async (aggrTask, group) =>
                    {
                        var innerResult = await group.ToArray().BatchInsertOrReplaceForSamePartitionKeyAsync(targetTable);
                        var innerStats = await aggrTask;  // awaiting after insert to get more parallelism
                        return innerStats.Concat(new TableTransferStatistics
                        {
                            errors = innerResult.Key,
                            successes = innerResult.Value.Count(item => item.Value == TransferStatus.CopySuccessful),
                            retries = innerResult.Value.Where(item => item.Value == TransferStatus.ShouldRetry).ToArray()
                        });
                    });
        }

        private static async Task<KeyValuePair<string[],KeyValuePair<DynamicTableEntity,TransferStatus>[]>> BatchInsertOrReplaceForSamePartitionKeyAsync(this DynamicTableEntity[] rows, CloudTable targetTable)
        {
            try
            {
                // batch operations are limited to 100 rows and 4MB of data
                var cmd = new TableBatchOperation();
                foreach (var row in rows)
                    cmd.Add(TableOperation.InsertOrReplace(row));

                var batch = await targetTable.ExecuteBatchAsync(cmd, TableCopyOptions.requestOptions, new OperationContext());
                return new string[] { }.PairWithValue(
                            batch.Select(x => new KeyValuePair<DynamicTableEntity, TransferStatus>(x.Result as DynamicTableEntity, Convert(x.HttpStatusCode)))
                                .ToArray());
            }
            catch (Exception e)
            {
                return new[] { e.Message }.PairWithValue(
                            rows.Select(x => new KeyValuePair<DynamicTableEntity, TransferStatus>(x, TransferStatus.ShouldRetry))
                                .ToArray());
            }
        }

        private static TransferStatus Convert(int statusCode)
        {
            switch (statusCode)
            {
                case (int)HttpStatusCode.NoContent:
                    return TransferStatus.CopySuccessful;
                default:
                    return TransferStatus.ShouldRetry;
            }
        }
    }
}
