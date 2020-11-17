namespace EF.BulkOperations.Core
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Entity;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Reflection;
    using EntityFramework.Metadata;
    using Extensions;
    using FastMember;

    internal static class SqlBulkOperation
    {
        internal static void Insert<TEntity>(DbContext context, IEnumerable<TEntity> entities, BulkTableInfo<TEntity> tableInfo)
            where TEntity : class
        {
            Insert(context, tableInfo.FullTableName, entities, tableInfo.OperationIncludedColumns, tableInfo.Config);
        }

        internal static int Merge<TEntity>(
            DbContext context, IEnumerable<TEntity> entities, BulkTableInfo<TEntity> tableInfo)
            where TEntity : class
        {
            bool isTempTableCreated = false;
            bool isTempOutputTableCreated = false;

            bool keepIdentity = tableInfo.KeepIdentity;

            try
            {
                // Create temporary table to store values to insert
                context.Database.ExecuteSqlCommand(
                    SqlQueryBuilder.CreateTableCopy(
                        tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo.TempTableIncludedColumns));

                isTempTableCreated = true;

                if (tableInfo.Config.SetOutputIdentity || tableInfo.Config.IsBulkResultEnabled)
                {
                    // Create temporary output table to store result values
                    context.Database.ExecuteSqlCommand(
                        SqlQueryBuilder.CreateOutputTableCopy(
                            tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo.TempOutputTableIncludedColumns));

                    isTempOutputTableCreated = true;
                }

                // Insert data into temp table
                Insert(context, tableInfo.FullTempTableName, entities, tableInfo.TempTableIncludedColumns, tableInfo.Config);

                if (keepIdentity && tableInfo.HasIdentity)
                {
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, true));
                }

                context.Database.ExecuteSqlCommand(SqlQueryBuilder.MergeTable(tableInfo));

                LoadOutputData(context, entities, tableInfo);
            }
            finally
            {
                if (isTempOutputTableCreated)
                {
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
                }

                if (isTempTableCreated)
                {
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));
                }

                if (keepIdentity && tableInfo.HasIdentity)
                {
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, false));
                }
            }

            return 0;
        }

        private static void Insert<TEntity>(
            DbContext context,
            string targetTable,
            IEnumerable<TEntity> entities,
            IEnumerable<IPropertyMap> includedColumns,
            BulkConfig<TEntity> config)
            where TEntity : class
        {
            var dataTable = GetDataTable(includedColumns, entities);

            BulkCopy(context, targetTable, dataTable, config);
        }

        private static DataTable GetDataTable<TEntity>(IEnumerable<IPropertyMap> columns, IEnumerable<TEntity> entities)
                where TEntity : class
        {
            var dataTable = new DataTable();

            // Create columns
            var entityProperties = typeof(TEntity).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            var dataColumns = columns
                .Select(c => new DataColumn(c.ColumnName, Nullable.GetUnderlyingType(c.Type) ?? c.Type));

            dataTable.Columns.AddRange(dataColumns.ToArray());

            // Create rows
            foreach (var item in entities)
            {
                var values = new List<object>();

                foreach (var c in columns)
                {
                    var prop = entityProperties.SingleOrDefault(pi => pi.Name == c.PropertyName);

                    if (prop != null)
                    {
                        values.Add(prop.GetValue(item, null));
                    }
                }

                dataTable.Rows.Add(values.ToArray());
            }

            return dataTable;
        }

        private static void BulkCopy<TEntity>(
            DbContext context, string destinationTableName, DataTable dataTable, BulkConfig<TEntity> config)
            where TEntity : class
        {
            var sqlConnection = (SqlConnection)context.Database.Connection;
            var ctxTransaction = (SqlTransaction)context.Database.CurrentTransaction.UnderlyingTransaction;
            var sqlBulkCopyOptions = config.SqlBulkCopyOptions;

            using (var sqlBulkCopy = new SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, ctxTransaction))
            {
                sqlBulkCopy.DestinationTableName = destinationTableName;
                sqlBulkCopy.BatchSize = config.SqlBulkCopyBatchSize;
                sqlBulkCopy.NotifyAfter = config.SqlBulkCopyNotifyAfter ?? config.SqlBulkCopyBatchSize;
                sqlBulkCopy.SqlRowsCopied += (sender, e) =>
                {
                    if (config.SqlBulkCopyProgressEventHandler != null)
                    {
                        config.SqlBulkCopyProgressEventHandler(GetProgress(dataTable.Rows.Count, e.RowsCopied, config.SqlBulkCopyNotifyAfter));
                    }
                };
                sqlBulkCopy.BulkCopyTimeout = config.SqlBulkCopyTimeout ?? sqlBulkCopy.BulkCopyTimeout;
                sqlBulkCopy.EnableStreaming = config.SqlBulkCopyEnableStreaming;

                foreach (var dataTableColumn in dataTable.Columns)
                {
                    sqlBulkCopy.ColumnMappings.Add(dataTableColumn.ToString(), dataTableColumn.ToString());
                }

                sqlBulkCopy.WriteToServer(dataTable);
            }
        }

        private static void LoadOutputData<TEntity>(DbContext context, IEnumerable<TEntity> entities, BulkTableInfo<TEntity> tableInfo)
            where TEntity : class
        {
            if (tableInfo.Config.SetOutputIdentity || tableInfo.Config.IsBulkResultEnabled)
            {
                var mergeOutputResult = context.Database.SqlQuery(
                    SqlQueryBuilder.SelectFromTable(tableInfo.FullTempOutputTableName, OrderByColumns: tableInfo.TempOutputTableIncludedColumns));

                if (tableInfo.Config.SetOutputIdentity && tableInfo.HasIdentity)
                {
                    var accessor = TypeAccessor.Create(typeof(TEntity));
                    string identityPropertyName = tableInfo.IdentityColumn.PropertyName;
                    string identityColumnName = tableInfo.IdentityColumn.PropertyName;

                    for (int i = 0; i < entities.Count(); i++)
                    {
                        var resultRow = mergeOutputResult.ElementAt(i);

                        if ((string)resultRow[SqlQueryBuilder.OutputActionColumnAlias] == SqlQueryBuilder.OutputInsertActionValue)
                        {
                            var entity = entities.ElementAt(i);
                            accessor[entity, identityPropertyName] = resultRow[identityColumnName];
                        }
                    }
                }

                if (tableInfo.Config.IsBulkResultEnabled)
                {
                    var mergeStats = SqlQueryBuilder.ExtractMergeStats(mergeOutputResult);

                    if (tableInfo.OperationType == BulkMergeOperationType.Insert)
                    {
                        mergeStats.Updated = 0;
                    }

                    tableInfo.Config.BulkResult = mergeStats;
                }
            }
        }

        private static decimal GetProgress(int entitiesCount, long rowsCopied, int? notifierAfter = 0)
        {
            var currentCount = rowsCopied;

            if (entitiesCount - rowsCopied < notifierAfter)
            {
                currentCount = entitiesCount;
            }

            return (decimal)(Math.Floor(currentCount * 10000D / entitiesCount) / 10000);
        }
    }
}
