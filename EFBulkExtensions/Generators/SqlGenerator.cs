namespace EFBulkExtensions.Generators
{
    using System;
    using System.Collections.Generic;
    using System.Data.Entity;
    using System.Linq;
    using System.Text;
    using EntityFramework.MappingAPI;
    using Extensions;

    public static class SqlGenerator
    {
        private const string Source = "Source";

        private const string Target = "Target";

        internal static string BuildCreateTempTable<TEntity>(DbContext context, string tableName, IEnumerable<IPropertyMap> columns)
            where TEntity : class
        {
            var command = new StringBuilder();

            command.Append($"{Environment.NewLine}CREATE TABLE {tableName}(");

            var primitiveTypes = context.GetPrimitiveType<TEntity>();

            var parameters = columns
                .Select(c => $"[{c.ColumnName}] {GetSchemaType(c, primitiveTypes[c.ColumnName])}");

            command.Append(string.Join(", ", parameters));
            command.Append(");");

            return command.ToString();
        }

        internal static string BuildInsertIntoOutput<TEntity>(
            DbContext context,
            string tmpOutputTableName,
            string tmpTableName,
            IPropertyMap identityColumn,
            IEnumerable<IPropertyMap> columns)
            where TEntity : class
        {
            var command = new StringBuilder();
            var fullTableName = context.GetTableName<TEntity>();

            command.Append(BuildInsertInto(fullTableName, columns));
            command.Append($"{Environment.NewLine}OUTPUT INSERTED.{identityColumn.ColumnName}");
            command.Append($"{Environment.NewLine}INTO {tmpOutputTableName}([{identityColumn.ColumnName}])");
            command.Append(BuildSelect(tmpTableName, columns, tableAlias: "Source"));

            return command.ToString();
        }

        internal static string BuildMergeIntoUpdate<TEntity>(
            string sourceTableName,
            string targetTableName,
            IEnumerable<IPropertyMap> keyColumns,
            IEnumerable<IPropertyMap> columns)
            where TEntity : class
        {
            var command = new StringBuilder();

            command.Append($"{Environment.NewLine}MERGE INTO {targetTableName} WITH (HOLDLOCK) AS {Target}");
            command.Append($"{Environment.NewLine}USING {sourceTableName} AS {Source}");
            command.Append($"{Environment.NewLine}{BuildMergeOnPart(keyColumns)}");
            command.Append($"{Environment.NewLine}WHEN MATCHED");
            command.Append($"{BuildMergeUpdatePart<TEntity>(columns)};");

            return command.ToString();
        }

        internal static string BuildMergeUpdatePart<TEntity>(IEnumerable<IPropertyMap> columns)
        {
            var command = new StringBuilder();
            var parameters = columns
                .Select(c => $"{Environment.NewLine}[{Target}].[{c.ColumnName}] = [{Source}].[{c.ColumnName}]");

            command.Append($"{Environment.NewLine}THEN UPDATE SET");
            command.Append(string.Join(", ", parameters));

            return command.ToString();
        }

        internal static string BuildMergeOnPart(IEnumerable<IPropertyMap> columns)
        {
            var command = new StringBuilder();
            var parameters = columns.Select(c => $"[{Target}].[{c.ColumnName}] = [{Source}].[{c.ColumnName}]");

            command.Append($"ON ");
            command.Append(string.Join(" AND ", parameters));

            return command.ToString();
        }

        internal static string BuildMergeIntoDelete(
            string sourceTableName,
            string targetTableName,
            IEnumerable<IPropertyMap> keyColumns
            )
        {
            var command = new StringBuilder();

            command.Append($"{Environment.NewLine}MERGE INTO {targetTableName} WITH (HOLDLOCK) AS {Target}");
            command.Append($"{Environment.NewLine}USING {sourceTableName} AS {Source}");
            command.Append($"{Environment.NewLine}{BuildMergeOnPart(keyColumns)}");
            command.Append($"{Environment.NewLine}WHEN MATCHED THEN DELETE;");

            return command.ToString();
        }

        internal static string BuildDropTable(string tableName)
        {
            return $"{Environment.NewLine}DROP TABLE {tableName};";
        }

        internal static string BuildInsertInto(string tableName, IEnumerable<IPropertyMap> columns)
        {
            var command = new StringBuilder();

            command.Append($"{Environment.NewLine}INSERT INTO ");
            command.Append(tableName);
            command.Append(" (");
            command.Append(string.Join(", ", columns.Select(c => $"[{c.ColumnName}]")));
            command.Append($")");

            return command.ToString();
        }

        internal static string BuildSelect(
            string tableName,
            IEnumerable<IPropertyMap> columns,
            string tableAlias = null,
            IEnumerable<IPropertyMap> OrderByColumns = null)
        {
            var command = new StringBuilder();

            if (!string.IsNullOrEmpty(tableAlias))
            {
                command.Append($"{Environment.NewLine}SELECT {string.Join(", ", columns.Select(c => $"[{tableAlias}].[{c.ColumnName}]"))}");
            }
            else
            {
                command.Append($"{Environment.NewLine}SELECT {string.Join(", ", columns.Select(c => $"[{c.ColumnName}]"))}");
            }

            command.Append($"{Environment.NewLine}FROM {tableName}");

            if (OrderByColumns != null && OrderByColumns.Any())
            {
                command.Append($"{Environment.NewLine}ORDER BY {string.Join(", ", OrderByColumns.Select(c => c.ColumnName))}");
            }

            if (!string.IsNullOrEmpty(tableAlias))
            {
                command.Append($" AS {tableAlias};");
            }

            return command.ToString();
        }

        private static string GetSchemaType(IPropertyMap column, string columnType)
        {
            switch (columnType)
            {
                case "varchar":
                case "nvarchar":
                case "char":
                case "binary":
                case "varbinary":
                case "nchar":
                    if (column.MaxLength != 0)
                        columnType = columnType + $"({column.MaxLength})";
                    break;
                case "decimal":
                case "numeric":
                    columnType = columnType + $"({column.Precision}, {column.Scale})";
                    break;
                case "datetime2":
                case "time":
                    break;
            }

            return columnType;
        }
    }
}
