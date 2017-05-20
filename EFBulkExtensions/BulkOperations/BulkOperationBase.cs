using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core;
using System.Linq;
using EFBulkExtensions.Extensions;

namespace EFBulkExtensions.BulkOperations
{
    public abstract class BulkOperationBase : IBulkOperation
    {
        public int Execute<TEntity>(DbContext context, IEnumerable<TEntity> collection, Action<BulkOperationSettings<TEntity>> settingsFactory = null)
            where TEntity : class
        {
            if (!context.ContainsTable<TEntity>())
            {
                throw new EntityException(@"The specified entity type is not recognized as a DbContext type.");
            }

            var affectedRows = default(int);
            var settings = new BulkOperationSettings<TEntity>();

            if (!collection.Any())
            {
                return affectedRows;
            }

            // Retrieve bulk operation settings
            if (settingsFactory != null)
            {
                settingsFactory.Invoke(settings);
            }

            // Creates inner transaction for the scope of the operation if the context doesn't have one.
            var transaction = context.InternalTransaction();

            try
            {
                affectedRows = ExecuteCommand(context, collection, settings);

                //Commit if internal transaction exists.
                transaction?.Commit();
                return affectedRows;
            }
            catch (Exception)
            {
                //Rollback if internal transaction exists.
                transaction?.Dispose();
                throw;
            }
        }

        protected abstract int ExecuteCommand<TEntity>(DbContext context, IEnumerable<TEntity> collection, BulkOperationSettings<TEntity> settings)
            where TEntity : class;
    }
}
