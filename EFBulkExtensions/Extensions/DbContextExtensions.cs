namespace EFBulkExtensions.Extensions
{
    using System.Collections.Generic;
    using System.Data.Entity;
    using BulkOperations;

    /// <summary>
    /// This class contains extension methods on <see cref="System.Data.Entity.DbContext"/>.
    /// </summary>
    public static class DbContextExtensions
    {
        /// <summary>
        /// Inserts a list of <typeparamref name="TEntity"/>.
        /// </summary>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="context">The database context.</param>
        /// <param name="entities">The list of entities to insert.</param>
        /// <param name="config">The bulk operation configuration.</param>
        public static void BulkInsert<TEntity>(
            this DbContext context,
            IEnumerable<TEntity> entities,
            BulkConfig<TEntity> config = null)
            where TEntity : class
        {
            BulkOperations.BulkInsert.New.Execute(
                context, entities, BulkMergeOperationType.Insert, config ?? new BulkConfig<TEntity>());
        }

        /// <summary>
        /// Updates a list of <typeparamref name="TEntity"/>.
        /// </summary>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="context">The database context.</param>
        /// <param name="entities">The list of entities to update.</param>
        /// <param name="config">The bulk operation configuration.</param>
        public static void BulkUpdate<TEntity>(
            this DbContext context,
            IEnumerable<TEntity> entities,
            BulkConfig<TEntity> config = null)
            where TEntity : class
        {
            BulkOperations.BulkUpdate.New.Execute(
                context, entities, BulkMergeOperationType.Update, config ?? new BulkConfig<TEntity>());
        }

        /// <summary>
        /// Inserts a list of <typeparamref name="TEntity"/>.
        /// </summary>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="context">The database context.</param>
        /// <param name="entities">The list of entities to delete.</param>
        /// <param name="config">The bulk operation configuration.</param>
        public static void BulkDelete<TEntity>(
            this DbContext context,
            IEnumerable<TEntity> entities,
            BulkConfig<TEntity> config = null)
            where TEntity : class
        {
            BulkOperations.BulkDelete.New.Execute(
                context, entities, BulkMergeOperationType.Delete, config ?? new BulkConfig<TEntity>());
        }

        /// <summary>
        /// Merges a list of <typeparamref name="TEntity"/>.
        /// </summary>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="context">The database context.</param>
        /// <param name="entities">The list of entities to merge.</param>
        /// <param name="operationType">The operation type.</param>
        /// <param name="config">The bulk operation configuration.</param>
        public static void BulkMerge<TEntity>(
            this DbContext context,
            IEnumerable<TEntity> entities,
            BulkMergeOperationType operationType,
            BulkConfig<TEntity> config = null)
            where TEntity : class
        {
            BulkOperations.BulkMerge.New.Execute(
                context, entities, operationType, config ?? new BulkConfig<TEntity>());
        }

        internal static DbContextTransaction InternalTransaction(
            this DbContext context)
        {
            DbContextTransaction transaction = null;

            if (context.Database.CurrentTransaction == null)
            {
                transaction = context.Database.BeginTransaction();
            }

            return transaction;
        }
    }
}
