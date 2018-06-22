# EastFive.Azure.Storage.Backup
A service that periodically schedules a data copy (tables and blobs) from one Azure storage account to another.  

Table data is downloaded to the service and re-uploaded.  Blob data is not downloaded, but copied between storage accounts by Azure.

Using the same target account for backups minimizes transactions because after the initial copy, then only deltas are copied.

To quickly recover a system where the storage account has become unusable, consider simply changing the connection string to point to the backup. (And, then switch the backup service config so that the target is the new source.)

## Getting Started
1. Clone this repo and the prerequisites side by side into a directory.
2. Build each individually with Visual Studio.  
3. Edit the configuration file `backup.json`.
4. Install the service.

## Prerequisites
Clone each into a common directory, say `c:\backup.`
* [BlackBarLabs.Core](https://github.com/blackbarlabs/BlackBarLabs.Core) - c# helpers
* [BlackBarLabs.Security](https://github.com/blackbarlabs/BlackBarLabs.Security) - token helpers
* [BlackBarLabs.Web](https://github.com/blackbarlabs/BlackBarLabs.Web) - config helpers
* [EastFive.Azure.Storage.Backup](https://github.com/eastfivellc/EastFive.Azure.Storage.Backup) - this repo

## Installing
From an administrative command window, type:
```
c:\windows\microsoft.net\Framework\v4.0.30319\InstallUtil.exe c:\backup\EastFive.Azure.Storage.Backup\bin\Release\EastFive.Azure.Storage.Backup.exe
```
Open services.msc, and start **EastFive.Azure.Storage.Backup.Service**

Note: To uninstall, add the `/u` option.

## Configuration File 
[backup.json](backup.json) is in the root folder, and once the service is started, a file watcher is used to check it for changes so that you don't have to restart the service.

`actions` is an array of backups where you specify the source and target along with the day of week and time of day for the copy to occur.

Example:
```
{
    "actions": [
        {
            "tag": "production",
            "sourceConnectionString": "",
            "services": [
                "Table",
                "Blob"
            ],
            "serviceSettings" = {},  /* <-- described below */
            "recurringSchedules": [
                {
                    "uniqueId": "F2EEBD42-534E-4028-BAC1-7E0558796268",
                    "tag": "my backup",
                    "targetConnectionString": "",
                    "daysOfWeek": [
                        "Sunday",
                        "Monday",
                        "Tuesday",
                        "Wednesday",
                        "Thursday",
                        "Friday",
                        "Saturday"
                    ],
                    "timeLocal": "21:00:00"
                }
            ]
        }
    ]
}
```

`recurringSchedules` is an array where you can setup several targets which backup on different days or more than once on the same day.

These options let you customize the table and blob transfers:

Table options
* `includedTables` - Specific table names to copy or empty to indicate all tables. Default is empty.
* `excludedTables` - If includedTables is empty (meaning all), then use this to omit certain tables. Default is empty.
* `maxTableConcurrency` - The max number of tables to copy concurrently.  Higher uses more bandwidth and cpu.  Default is 5.
* `maxSegmentDownloadConcurrencyPerTable` - The max number of table segments to process at a time.  Each segment is about 1000 rows and higher uses more memory.  Default is 25.
* `maxRowUploadConcurrencyPerTable` - The max number of rows to insert or replace at a time.  Azure limits this to 100, but you can choose lower if the payload is too large.  Default is 100.
* `partitionKeys` - The array of partition key strings, or empty for all partition keys, to copy.  Specifying this can help large tables by retrieving in parallel by partition key.  Default is empty.
* `copyRetries` - The number of times to retry a copy if there were any failures.  Default is 5.

Blob options
* `includedContainers` - Specific container names to copy or empty to indicate all containers. Default is empty.
* `excludedContainers` - If includedContainers is empty (meaning all), then use this to omit certain containers. Default is empty.
* `accessPeriod` - The time span after which a new container access window is requested. Default is 1 hour.
* `maxContainerConcurrency` - The max number of containers to copy concurrently.  Higher uses more bandwidth and cpu.  Default is 2.
* `maxBlobConcurrencyPerContainer` - The max number of blobs on which to initiate a background copy concurrently.  Default is 200.
* `minIntervalToCheckCopy` - The min wait time after starting a copy and checking whether it has completed.  Default is 1 second.
* `maxIntervalToCheckCopy` - The max wait time after starting a copy and checking whether it has completed.  Default is 15 seconds.
* `maxTotalWaitForCopy` - The max wait time for a blob copy to complete before setting it aside and retrying later.  Default is 5 minutes.
* `copyRetries` - The number of times to retry a copy if there were any failures.  Default is 5.

Note: The actual wait time between starting a blob copy and checking whether it has completed is automatically adjusted and may go up or down between the given limits as network performance changes.

Example:
```
"serviceSettings": {
    "table": {
        "maxTableConcurrency": 5,
        "maxRowUploadConcurrencyPerTable": 100,
        "maxSegmentDownloadConcurrencyPerTable": 25,
        "partitionKeys": [],
        "copyRetries": 5
    },
    "blob": {
        "accessPeriod": "01:00:00",
        "maxContainerConcurrency": 2,
        "maxBlobConcurrencyPerContainer": 200,
        "minIntervalCheckCopy": "00:00:01",
        "maxIntervalCheckCopy": "00:00:15",
        "maxTotalWaitForCopy": "00:05:00",
        "copyRetries": 5
    }
},
```
