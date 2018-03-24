namespace EastFive.Azure.Storage.Backup
{
    public enum TransferStatus
    {
        CopySuccessful,
        Running,
        ShouldRetry,
        Failed
    }

    public enum StorageService
    {
        Table,
        Blob
    }
}
