namespace EFBulkExtensions.BulkOperations
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Text.RegularExpressions;
    using EntityFramework.Metadata;

    internal static class SqlQueryBuilder
    {
        public const string OutputActionColumnAlias = "Action";
        public const string OutputInsertActionValue = "INSERT";
        public const string OutputUpdateActionValue = "UPDATE";
        public const string OutputDeleteActionValue = "DELETE";

        private const string OutputActionColumnName = "$action";

        internal static string CreateTableCopy(
            string sourceTable,
            string targetTable,
            IEnumerable<IPropertyMap> columns)
        {
            var query = new StringBuilder();

            query.AppendFormat("{0}SELECT TOP 0 {1}", Environment.NewLine, JoinColumns(columns, leftTableAlias: "T"));
            query.AppendFormat("{0}INTO {1} FROM {2} AS S", Environment.NewLine, targetTable, sourceTable);
            query.AppendFormat("{0}LEFT JOIN {1} AS T ON 1 = 0;", Environment.NewLine, sourceTable); // removes Identity constraint

            // Good to know: inverse T and S in left join for columns to be nullable

            return query.ToString();
        }

        internal static string CreateOutputTableCopy(
            string sourceTable,
            string targetTable,
            IEnumerable<IPropertyMap> columns)
        {
            var query = new StringBuilder();

            var outputColumns = new List<string> {
                string.Format("{0} = CAST('' as nvarchar(10))", OutputActionColumnAlias),
                JoinColumns(columns, leftTableAlias: "T")
            }.Where(c => !string.IsNullOrEmpty(c));

            query.AppendFormat("{0}SELECT TOP 0 {1}", Environment.NewLine, string.Join(", ", outputColumns));
            query.AppendFormat("{0}INTO {1} FROM {2} AS S", Environment.NewLine, targetTable, sourceTable);
            query.AppendFormat("{0}LEFT JOIN {1} AS T ON 1 = 0;", Environment.NewLine, sourceTable); // removes Identity constraint

            return query.ToString();
        }

        internal static string MergeTable<TEntity>(BulkTableInfo<TEntity> tableInfo)
            where TEntity : class
        {
            var operationType = tableInfo.OperationType;

            var sourceTable = tableInfo.Config.SetOutputIdentity
                ? $"(SELECT TOP {tableInfo.EntityCount} * FROM {tableInfo.FullTempTableName} ORDER BY {JoinColumns(tableInfo.EntityMap.Pks)})"
                : tableInfo.FullTempTableName;

            var query = new StringBuilder();

            query.AppendFormat("{0}DECLARE @dummy bit;{0}", Environment.NewLine); // dummy variable for fake update
            query.AppendFormat("{0}MERGE {1} {2} AS T", Environment.NewLine, tableInfo.FullTableName, tableInfo.Config.MergeWithHoldLock ? "WITH (HOLDLOCK)" : string.Empty);
            query.AppendFormat("{0}USING {1} AS S", Environment.NewLine, sourceTable);
            query.AppendFormat("{0}ON {1}", Environment.NewLine, JoinColumns(tableInfo.IdentifierColumns, " AND ", "T", "S"));

            if (operationType.HasFlag(BulkMergeOperationType.Insert))
            {
                query.AppendFormat("{0}WHEN NOT MATCHED BY TARGET THEN INSERT ({1})", Environment.NewLine, JoinColumns(tableInfo.OperationIncludedColumns));
                query.AppendFormat("{0}VALUES ({1})", Environment.NewLine, JoinColumns(tableInfo.OperationIncludedColumns, leftTableAlias: "S"));

                if (operationType == BulkMergeOperationType.Insert)
                {
                    query.AppendFormat(
                    "{0}WHEN MATCHED THEN UPDATE SET @dummy = @dummy", Environment.NewLine);
                }
            }

            if (operationType.HasFlag(BulkMergeOperationType.Update))
            {
                var updateColumns = new List<string>
                {
                    "@dummy = @dummy",
                    JoinColumns(tableInfo.OperationIncludedColumns.Where(p => !p.IsIdentity), leftTableAlias: "T", rightTableAlias: "S")
                }.Where(c => !string.IsNullOrEmpty(c));

                query.AppendFormat(
                    "{0}WHEN MATCHED THEN UPDATE SET {1}", Environment.NewLine, string.Join(", ", updateColumns));
            }

            if (operationType.HasFlag(BulkMergeOperationType.Update | BulkMergeOperationType.Delete))
            {
                query.AppendFormat("{0}WHEN NOT MATCHED BY SOURCE THEN DELETE", Environment.NewLine);
            }
            else if (operationType.HasFlag(BulkMergeOperationType.Delete))
            {
                query.AppendFormat("{0}WHEN MATCHED THEN DELETE", Environment.NewLine);
            }

            if (tableInfo.Config.SetOutputIdentity || tableInfo.Config.IsBulkResultEnabled)
            {
                var outputColumFormat = "CASE WHEN $action = 'INSERT' OR $action = 'UPDATE' THEN INSERTED.[{0}] WHEN $action = 'DELETE' THEN DELETED.[{0}] END AS [{0}]";

                var outputColumns = new List<string> { OutputActionColumnName }
                    .Concat(tableInfo.TempOutputTableIncludedColumns.Select(p => string.Format(outputColumFormat, p.ColumnName)))
                    .Where(c => !string.IsNullOrEmpty(c));

                query.AppendFormat("{0}OUTPUT {1}", Environment.NewLine, string.Join(", ", outputColumns));
                query.AppendFormat("{0}INTO {1}", Environment.NewLine, tableInfo.FullTempOutputTableName);
            }

            query.Append(";");

            return query.ToString();
        }

        internal static BulkResult ExtractMergeStats(IEnumerable<IDictionary<string, object>> mergeOutputResult)
        {
            var mergeStats = mergeOutputResult.Aggregate(new BulkResult(), (acc, current) =>
            {
                string action = current[OutputActionColumnAlias] as string;

                switch (action)
                {
                    case OutputInsertActionValue:
                        acc.Inserted++;
                        break;

                    case OutputUpdateActionValue:
                        acc.Updated++;
                        break;

                    case OutputDeleteActionValue:
                        acc.Deleted++;
                        break;
                }

                return acc;
            });

            return mergeStats;
        }

        internal static string SelectFromTable(
            string tableName,
            IEnumerable<IPropertyMap> columns = null,
            IEnumerable<IPropertyMap> OrderByColumns = null)
        {
            var command = new StringBuilder();

            var joinedColumns = columns != null && columns.Any()
                ? JoinColumns(columns)
                : "*";

            command.AppendFormat("{0}SELECT {1}", Environment.NewLine, joinedColumns);
            command.AppendFormat("{0}FROM {1}", Environment.NewLine, tableName);

            if (OrderByColumns != null && OrderByColumns.Any())
            {
                command.AppendFormat("{0}ORDER BY {1}", Environment.NewLine, JoinColumns(OrderByColumns));
            }

            return command.ToString();
        }

        internal static string DropTable(string tableName)
        {
            var query = $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName}";
            return query;
        }

        internal static string JoinColumns(
            IEnumerable<IPropertyMap> columnMappins,
            string separator = ", ",
            string leftTableAlias = null,
            string rightTableAlias = null)
        {
            var joinedColumns = columnMappins.Aggregate(string.Empty, (acc, c) =>
            {
                var result = string.Empty;

                var leftColumn = !string.IsNullOrEmpty(leftTableAlias)
                    ? $"{leftTableAlias}.[{c.ColumnName}]"
                    : $"[{c.ColumnName}]";

                if (!string.IsNullOrEmpty(rightTableAlias))
                {
                    var rightColumn = $"{rightTableAlias}.[{c.ColumnName}]";

                    result = $"{leftColumn} = {rightColumn}";

                    var regex = new Regex("^(and|or)$", RegexOptions.IgnoreCase);

                    if (!c.IsRequired && regex.IsMatch(separator.Trim()))
                    {
                        result = $"({result} OR ({leftColumn} IS NULL AND {rightColumn} IS NULL))";
                    }
                }
                else
                {
                    result = leftColumn;
                }

                acc += !string.IsNullOrEmpty(acc) ? string.Format("{0}{1}", separator, result) : result;

                return acc;
            });

            return joinedColumns;
        }

        internal static string SetIdentityInsert(string tableName, bool identityInsert)
        {
            string activationValue = identityInsert ? "ON" : "OFF";
            var q = $"SET IDENTITY_INSERT {tableName} {activationValue};";
            return q;
        }
    }
}
