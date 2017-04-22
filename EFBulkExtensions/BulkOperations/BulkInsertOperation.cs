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

            // Retrieve identifier columns definition
            var identifierColumnsDef = settings.IdentifierColumns != null
                ? context.GetTableColumns<TEntity>(ExpressionHelper.GetPropertyNames(settings.IdentifierColumns.Body))
                : context.GetTablePrimaryKeys<TEntity>();

            // Retrieve included columns definition
            var includedColumnsDef = settings.IncludedColumns != null
                ? context.GetTableColumns<TEntity>(ExpressionHelper.GetPropertyNames(settings.IncludedColumns.Body))
                : context.GetTableColumns<TEntity>();

            // Retrieve identity column definition
            var identityColumnDef = context.GetTableIdentityColumn<TEntity>();

            // Convert entity collection into a DataTable
            var dataTable = context.ToDataTable(entities, includedColumnsDef);

            // Create temporary table to store values to insert
            var command = SqlGenerator.BuildCreateTempTable<TEntity>(context, tmpTableName, includedColumnsDef);
            database.ExecuteSqlCommand(command);

            // Bulk inset data to temporary temporary table
            context.BulkCopy(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

            if (settings.IsIdentityOutputEnabled && identityColumnDef != null)
            {
                // Create temporary table to store inserted identities
                var tmpOutputTableName = context.RandomTableName<TEntity>();
                command = SqlGenerator.BuildCreateTempTable<TEntity>(context, tmpOutputTableName, new List<IPropertyMap> { identityColumnDef });
                database.ExecuteSqlCommand(command);

                // Copy data from temporary table to destination table with id output to another temporary table
                command = SqlGenerator.BuildMergeIntoInsertOutput(
                    tmpTableName,
                    context.GetTableName<TEntity>(),
                    tmpOutputTableName,
                    identifierColumnsDef,
                    includedColumnsDef.Where(c => !c.IsIdentity),
                    new List<IPropertyMap> { identityColumnDef })
                    .EndCommand();

                database.ExecuteSqlCommand(command);

                //Load generated IDs from temporary output table into the entities.
                command = SqlGenerator.BuildSelect(tmpOutputTableName, new List<IPropertyMap> { identityColumnDef }).EndCommand();
                var identities = database.SqlQuery<long>(command).ToList();

                // Update entities identity
                context.UpdateEntitiesIdentity(identities, identityColumnDef, entities);

                // Remove temporary output
                command = SqlGenerator.BuildDropTable(tmpOutputTableName);
                database.ExecuteSqlCommand(command);
            }
            else
            {
                // Copy data from temporary table to destination table with id output to another temporary table
                command = SqlGenerator.BuildMergeIntoInsert(
                    tmpTableName,
                    context.GetTableName<TEntity>(),
                    identifierColumnsDef,
                    includedColumnsDef.Where(c => !c.IsIdentity))
                    .EndCommand();

                database.ExecuteSqlCommand(command);
            }    

            // Remove temporary output
            command = SqlGenerator.BuildDropTable(tmpTableName);
            database.ExecuteSqlCommand(command);

            return dataTable.Rows.Count;
        }
    }
}
