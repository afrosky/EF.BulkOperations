namespace EFBulkExtensions.BulkOperations
{
    using System.Collections.Generic;
    using System.Data.Entity;

    /// <summary>
    /// This interface represents a bulk operation.
    /// </summary>
    public interface IBulkOperation
    {
        /// <summary>
        /// Executes the bulk operation.
        /// </summary>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="context">The database context.</param>
        /// <param name="entities">The collection of entites.</param>
        /// <param name="operationType">The bulk operation type.</param>
        /// <param name="config">The bulk operation configuration.</param>
        void Execute<TEntity>(
            DbContext context,
            IEnumerable<TEntity> entities,
            BulkMergeOperationType operationType,
            BulkConfig<TEntity> config)
            where TEntity : class;
    }
}
