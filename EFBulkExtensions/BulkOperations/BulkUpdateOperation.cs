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

        protected override long ExecuteCommand<TEntity>(DbContext context, IEnumerable<TEntity> collection, BulkOperationSettings<TEntity> settings)
        {
            var tmpTableName = context.RandomTableName<TEntity>();
            var entities = collection.ToList();
            var database = context.Database;
            var affectedRows = 0;

            // Retrieve included columns definition
            var includedColumnsDef = settings.IncludedColumns != null
                ? context.GetTableColumns<TEntity>(((NewExpression)settings.IncludedColumns.Body).Members.Select(m => m.Name))
                : context.GetTableColumns<TEntity>();

            // Retrieve primary keys column definition
            var pksColumnDef = context.GetTablePrimaryKeys<TEntity>();

            if (!pksColumnDef.All(pk => includedColumnsDef.Any(c => pk.PropertyName == c.PropertyName)))
            {
                throw new EntityException(@"Included columns must contain primary key columns.");
            }

            // Convert entity collection into a DataTable
            var dataTable = context.ToDataTable(entities, includedColumnsDef);

            // Create temporary table to store values to update
            var command = SqlGenerator.BuildCreateTempTable<TEntity>(context, tmpTableName, includedColumnsDef);
            database.ExecuteSqlCommand(command);

            // Bulk inset data to temporary temporary table
            context.BulkCopy(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

            // Copy data from temporary table to destination table
            command = SqlGenerator
                .BuildMergeIntoUpdate<TEntity>(tmpTableName, context.GetTableName<TEntity>(), pksColumnDef, includedColumnsDef.Where(c => !c.IsPk && !c.IsIdentity));

            affectedRows = database.ExecuteSqlCommand(command);

            // Remove temporary output
            command = SqlGenerator.BuildDropTable(tmpTableName);
            database.ExecuteSqlCommand(command);

            return affectedRows;
        }
    }
}
