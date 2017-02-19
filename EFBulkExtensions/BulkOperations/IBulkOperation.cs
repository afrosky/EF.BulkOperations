namespace EFBulkExtensions.BulkOperations
{
    using System;
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
        /// <param name="settings">The bulk operation settings factory.</param>
        /// <returns>The number of affected entities.</returns>
        long Execute<TEntity>(DbContext context, IEnumerable<TEntity> collection, Action<BulkOperationSettings<TEntity>> settingsFactory = null)
            where TEntity : class;
    }
}
