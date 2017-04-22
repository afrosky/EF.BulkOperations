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

        private const string OutputInserted = "INSERTED";

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

        internal static StringBuilder BuildMergeIntoInsert(
            string sourceTableName,
            string targetTableName,
            IEnumerable<IPropertyMap> keyColumns,
            IEnumerable<IPropertyMap> insertColumns)
        {
            var command = BuildBaseMergeInto(sourceTableName, targetTableName, keyColumns);
            command.Append($"{Environment.NewLine}WHEN NOT MATCHED");
            command.Append($"{BuildMergeInsertPart(insertColumns)}");

            return command;
        }

        internal static StringBuilder BuildMergeIntoInsertOutput(
            string sourceTableName,
            string targetTableName,
            string targetOutputTableName,
            IEnumerable<IPropertyMap> keyColumns,
            IEnumerable<IPropertyMap> insertColumns,
            IEnumerable<IPropertyMap> outputColumns)
        {
            var command = BuildMergeIntoInsert(sourceTableName, targetTableName, keyColumns, insertColumns);
            command.Append(BuildOutputPart(OutputInserted, outputColumns, targetOutputTableName));

            return command;
        }


        internal static StringBuilder BuildMergeIntoUpdate(
            string sourceTableName,
            string targetTableName,
            IEnumerable<IPropertyMap> keyColumns,
            IEnumerable<IPropertyMap> updateColumns)
        {
            var command = BuildBaseMergeInto(sourceTableName, targetTableName, keyColumns);
            command.Append($"{Environment.NewLine}WHEN MATCHED");
            command.Append($"{BuildMergeUpdatePart(updateColumns)}");

            return command;
        }

        internal static StringBuilder BuildMergeIntoDelete(
            string sourceTableName,
            string targetTableName,
            IEnumerable<IPropertyMap> keyColumns)
        {
            var command = BuildBaseMergeInto(sourceTableName, targetTableName, keyColumns);
            command.Append($"{Environment.NewLine}WHEN MATCHED THEN DELETE");

            return command;
        }

        internal static string BuildMergeInsertPart(IEnumerable<IPropertyMap> insertColumns)
        {
            var command = new StringBuilder();
            var columnInsertNames = insertColumns.Select(c => $"[{c.ColumnName}]");
            var columnValueNames = insertColumns.Select(c => $"[{Source}].[{c.ColumnName}]");

            command.Append($"{Environment.NewLine}THEN INSERT ({string.Join(", ", columnInsertNames)})");
            command.Append($"{Environment.NewLine}VALUES ({string.Join(", ", columnValueNames)})");

            return command.ToString();
        }

        internal static string BuildMergeUpdatePart(IEnumerable<IPropertyMap> updateColumns)
        {
            var command = new StringBuilder();
            var parameters = updateColumns
                .Select(c => $"{Environment.NewLine}[{Target}].[{c.ColumnName}] = [{Source}].[{c.ColumnName}]");

            command.Append($"{Environment.NewLine}THEN UPDATE SET");
            command.Append(string.Join(", ", parameters));

            return command.ToString();
        }

        internal static string BuildOutputPart(string outputType, IEnumerable<IPropertyMap> outputColumns, string targetTableName)
        {
            var command = new StringBuilder();
            var columnOutputNames = outputColumns.Select(c => $"{outputType}.[{c.ColumnName}]");
            var columnOutputIntoNames = outputColumns.Select(c => $"[{c.ColumnName}]");

            command.Append($"{Environment.NewLine}OUTPUT {string.Join(", ", columnOutputNames)}");
            command.Append($"{Environment.NewLine}INTO {targetTableName}({string.Join(", ", columnOutputIntoNames)})");

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

        internal static StringBuilder BuildSelect(
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
                command.Append($" AS {tableAlias}");
            }

            return command;
        }

        internal static string EndCommand(this StringBuilder command)
        {
            command.Append(";");
            return command.ToString();
        }

        private static StringBuilder BuildBaseMergeInto(
            string sourceTableName,
            string targetTableName,
            IEnumerable<IPropertyMap> keyColumns)
        {
            var command = new StringBuilder();

            command.Append($"{Environment.NewLine}MERGE INTO {targetTableName} WITH (HOLDLOCK) AS {Target}");
            command.Append($"{Environment.NewLine}USING {sourceTableName} AS {Source}");
            command.Append($"{Environment.NewLine}{BuildMergeOnPart(keyColumns)}");

            return command;
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
