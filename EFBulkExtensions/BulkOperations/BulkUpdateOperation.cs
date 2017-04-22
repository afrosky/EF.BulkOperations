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
    using Helpers;

    public class BulkUpdateOperation : BulkOperationBase
    {
        public static BulkUpdateOperation New
        {
            get
            {
                return new BulkUpdateOperation();
            }
        }

        private BulkUpdateOperation()
        {
        }

        protected override int ExecuteCommand<TEntity>(DbContext context, IEnumerable<TEntity> collection, BulkOperationSettings<TEntity> settings)
        {
            var tmpTableName = context.RandomTableName<TEntity>();
            var entities = collection.ToList();
            var database = context.Database;
            var affectedRows = 0;

            // Retrieve included columns definition
            var includedColumnsDef = settings.IncludedColumns != null
                ? context.GetTableColumns<TEntity>(ExpressionHelper.GetPropertyNames(settings.IncludedColumns.Body))
                : context.GetTableColumns<TEntity>();

            // Retrieve identifier columns definition
            var identifierColumnsDef = settings.IdentifierColumns != null
                ? context.GetTableColumns<TEntity>(ExpressionHelper.GetPropertyNames(settings.IdentifierColumns.Body))
                : context.GetTablePrimaryKeys<TEntity>();

            // Merge identifier columns with included columns
            includedColumnsDef = includedColumnsDef.Union(identifierColumnsDef);

            // Convert entity collection into a DataTable
            var dataTable = context.ToDataTable(entities, includedColumnsDef);

            // Create temporary table to store values to update
            var command = SqlGenerator.BuildCreateTempTable<TEntity>(context, tmpTableName, includedColumnsDef);
            database.ExecuteSqlCommand(command);

            // Bulk inset data to temporary temporary table
            context.BulkCopy(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

            // Copy data from temporary table to destination table
            command = SqlGenerator
                .BuildMergeIntoUpdate(
                    tmpTableName,
                    context.GetTableName<TEntity>(),
                    identifierColumnsDef,
                    includedColumnsDef.Where(c => !c.IsIdentity && !identifierColumnsDef.Any(i => i.ColumnName == c.ColumnName)))
                    .EndCommand();

            affectedRows = database.ExecuteSqlCommand(command);

            // Remove temporary output
            command = SqlGenerator.BuildDropTable(tmpTableName);
            database.ExecuteSqlCommand(command);

            return affectedRows;
        }
    }
}
