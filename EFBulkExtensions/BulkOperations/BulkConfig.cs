namespace EFBulkExtensions.BulkOperations
{
    using System;
    using System.Data.SqlClient;
    using System.Linq.Expressions;

    /// <summary>
    /// This class regroups bulk operation available settings.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    public class BulkConfig<TEntity>
        where TEntity : class
    {
        public BulkConfig()
        {
            MergeWithHoldLock = true;
            UseTempDb = true;
        }

        /// <summary>
        /// Gets or sets a value indicating whether identity of inserted entities should be returned.
        /// </summary>
        public bool SetOutputIdentity { get; set; }

        /// <summary>
        /// The columns to use as identifer. By default, primary key columns are taken.
        /// </summary>
        public Expression<Func<TEntity, object>> IdentifierColumns { get; set; }

        /// <summary>
        /// The included columns. By default, all columns are included.
        /// </summary>
        public Expression<Func<TEntity, object>> IncludedColumns { get; set; }

        /// <summary>
        /// Bitwise flag that specifies one or more options to use with an instance of System.Data.SqlClient.SqlBulkCopy.
        /// </summary>
        public SqlBulkCopyOptions SqlBulkCopyOptions { get; set; }

        /// <summary>
        /// Number of rows in each batch. At the end of each batch, the rows in the batch are sent to the server.
        /// </summary>
        public int SqlBulkCopyBatchSize { get; set; }

        /// <summary>
        /// Defines the number of rows to be processed before generating a notification event.
        /// </summary>
        public int? SqlBulkCopyNotifyAfter { get; set; }

        /// <summary>
        /// Number of seconds for the operation to complete before it times out.
        /// </summary>
        public int? SqlBulkCopyTimeout { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether System.Data.SqlClient.SqlBulkCopy object streams data from an System.Data.IDataReader object.
        /// </summary>
        public bool SqlBulkCopyEnableStreaming { get; set; }

        /// <summary>
        /// An action to be executed while bulk operation is in progress (useful for long process and display loading status).
        /// </summary>
        public Action<decimal> SqlBulkCopyProgressEventHandler { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether merge operation uses HOLD LOCK.
        /// </summary>
        public bool MergeWithHoldLock { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether merge operation uses HOLD LOCK.
        /// </summary>
        public bool IsBulkResultEnabled { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the use of tempDB is enabled.
        /// </summary>
        public bool UseTempDb { get; set; }

        /// <summary>
        /// Gets the bulk operation results.
        /// </summary>
        public BulkResult BulkResult { get; internal set; }
    }
}
