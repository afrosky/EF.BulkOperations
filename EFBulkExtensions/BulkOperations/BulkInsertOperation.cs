namespace EFBulkExtensions.BulkOperations
{
    using System;
    using System.Collections.Generic;
    using System.Data.Entity;
    using System.Data.Entity.Core;
    using System.Linq;
    using System.Linq.Expressions;
    using Generators;
    using Extensions;
    using System.Data.SqlClient;
    using EntityFramework.MappingAPI;
    using Helpers;

    public class BulkInsertOperation : BulkOperationBase
    {
        public static BulkInsertOperation New
        {
            get
            {
                return new BulkInsertOperation();
            }
        }

        private BulkInsertOperation()
        {
        }

        protected override int ExecuteCommand<TEntity>(DbContext context, IEnumerable<TEntity> collection, BulkOperationSettings<TEntity> settings)
        {
            var tmpTableName = context.RandomTableName<TEntity>();
            var entities = collection.ToList();
            var database = context.Database;

            // Retrieve included columns definition
            var includedColumnsDef = settings.IncludedColumns != null
                ? context.GetTableColumns<TEntity>(ExpressionHelper.GetPropertyNames(settings.IncludedColumns.Body))
                : context.GetTableColumns<TEntity>().Where(c => !c.IsIdentity);

            // Retrieve identity column definition
            var identityColumnDef = context.GetTableIdentityColumn<TEntity>();

            if (includedColumnsDef.Any(c => identityColumnDef?.PropertyName == c.PropertyName))
            {
                throw new EntityException(@"Included columns can't contain identity column.");
            }

            // Convert entity collection into a DataTable
            var dataTable = context.ToDataTable(entities, includedColumnsDef);

            // Return generated ids for bulk inserted elements
            if (settings.IsIdentityOutputEnabled)
            {
                if (identityColumnDef == null)
                {
                    throw new EntityException(@"Entity doesn't contain any identity column. Set BulkOperationSettings.IsIdentityOutputEnabled = false.");
                }

                // Create temporary table to store values to insert
                var command = SqlGenerator.BuildCreateTempTable<TEntity>(context, tmpTableName, includedColumnsDef);
                database.ExecuteSqlCommand(command);

                // Bulk inset data to temporary temporary table
                context.BulkCopy(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

                // Create temporary table to store inserted identities
                var tmpOutputTableName = context.RandomTableName<TEntity>();
                command = SqlGenerator.BuildCreateTempTable<TEntity>(context, tmpOutputTableName, new List<IPropertyMap> { identityColumnDef });
                database.ExecuteSqlCommand(command);

                // Copy data from temporary table to destination table with id output to another temporary table
                command = SqlGenerator.BuildInsertIntoOutput<TEntity>(context, tmpOutputTableName, tmpTableName, identityColumnDef, includedColumnsDef);
                database.ExecuteSqlCommand(command);

                // Remove temporary output
                command = SqlGenerator.BuildDropTable(tmpTableName);
                database.ExecuteSqlCommand(command);

                //Load generated IDs from temporary output table into the entities.
                command = SqlGenerator.BuildSelect(tmpOutputTableName, new List<IPropertyMap> { identityColumnDef }, OrderByColumns: new List<IPropertyMap> { identityColumnDef });
                var identities = database.SqlQuery<long>(command).ToList();

                // Remove temporary output
                command = SqlGenerator.BuildDropTable(tmpOutputTableName);
                database.ExecuteSqlCommand(command);

                // Update entities identity
                context.UpdateEntitiesIdentity(identities, identityColumnDef, entities);
            }
            else
            {
                //Bulk insert data to destination table
                context.BulkCopy(dataTable, context.GetTableName<TEntity>(), SqlBulkCopyOptions.Default);
            }

            return dataTable.Rows.Count;
        }
    }
}
