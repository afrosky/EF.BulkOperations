namespace EFBulkExtensions.BulkOperations
{
    using System.Collections.Generic;
    using System.Data.Entity;
    using System.Data.SqlClient;
    using System.Linq;
    using Extensions;
    using Generators;

    public class BulkDeleteOperation : BulkOperationBase
    {
        public static BulkDeleteOperation New
        {
            get
            {
                return new BulkDeleteOperation();
            }
        }

        private BulkDeleteOperation()
        {
        }

        protected override long ExecuteCommand<TEntity>(DbContext context, IEnumerable<TEntity> collection, BulkOperationSettings<TEntity> settings)
        {
            var tmpTableName = context.RandomTableName<TEntity>();
            var entities = collection.ToList();
            var database = context.Database;
            var affectedRows = 0;

            // Retrieve primary keys column definition
            var pksColumnDef = context.GetTablePrimaryKeys<TEntity>();

            // Convert entity collection into a DataTable
            var dataTable = context.ToDataTable(entities, pksColumnDef);

            // Create temporary table to store values to update
            var command = SqlGenerator.BuildCreateTempTable<TEntity>(context, tmpTableName, pksColumnDef);
            database.ExecuteSqlCommand(command);

            // Bulk inset data to temporary temporary table
            context.BulkCopy(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

            // Copy data from temporary table to destination table
            command = SqlGenerator
                .BuildMergeIntoDelete(tmpTableName, context.GetTableName<TEntity>(), pksColumnDef);

            affectedRows = database.ExecuteSqlCommand(command);

            // Remove temporary output
            command = SqlGenerator.BuildDropTable(tmpTableName);
            database.ExecuteSqlCommand(command);

            return affectedRows;
        }
    }
}
