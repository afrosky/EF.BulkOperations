namespace EFBulkExtensions.BulkOperations
{
    using System.Collections.Generic;
    using System.Data.Entity;
    using Extensions;

    public abstract class BulkOperationBase : IBulkOperation
    {
        public void Execute<TEntity>(
            DbContext context,
            IEnumerable<TEntity> entities,
            BulkMergeOperationType operationType,
            BulkConfig<TEntity> config)
            where TEntity : class
        {
            BulkTableInfo<TEntity> tableInfo = new BulkTableInfo<TEntity>(context, entities, config, operationType);

            // Creates inner transaction for the scope of the operation if the context doesn't have one.
            var transaction = context.InternalTransaction();

            try
            {
                ExecuteCommand(context, entities, tableInfo);

                //Commit if internal transaction exists.
                transaction?.Commit();
            }
            finally
            {
                transaction?.Dispose();
            }
        }

        protected abstract void ExecuteCommand<TEntity>(
            DbContext context, IEnumerable<TEntity> entities, BulkTableInfo<TEntity> tableInfo)
            where TEntity : class;
    }
}
