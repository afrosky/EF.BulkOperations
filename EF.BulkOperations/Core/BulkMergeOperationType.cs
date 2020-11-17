namespace EF.BulkOperations.Core
{
    public enum BulkMergeOperationType
    {
        None = 0,
        Insert = 1 << 0,
        Update = 1 << 1,
        Delete = 1 << 2
    }
}
