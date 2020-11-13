namespace EFBulkExtensions.BulkOperations
{
    using System;
    using System.Collections.Generic;
    using System.Data.Entity;
    using System.Linq;
    using Helpers;
    using EntityFramework.Metadata;
    using EntityFramework.Metadata.Extensions;
    using System.Data.SqlClient;

    public class BulkTableInfo<TEntity> where TEntity : class
    {
        public BulkConfig<TEntity> Config { get; private set; }

        public IEntityMap<TEntity> EntityMap { get; private set; }

        public BulkMergeOperationType OperationType { get; private set; }

        public int EntityCount { get; private set; }

        public IEnumerable<IPropertyMap> IdentifierColumns { get; private set; }

        public IEnumerable<IPropertyMap> OperationIncludedColumns { get; private set; }

        public IEnumerable<IPropertyMap> TempTableIncludedColumns { get; private set; }

        public IEnumerable<IPropertyMap> TempOutputTableIncludedColumns { get; private set; }

        public IPropertyMap IdentityColumn { get; private set; }

        public string TempTableName { get; private set; }

        public string Schema { get; private set; }

        public string TableName { get; private set; }

        public string FullTableName => $"{SchemaFormated}[{TableName}]";

        public string FullTempTableName => $"{SchemaFormated}[{TempTableName}]";

        private string SchemaFormated => !string.IsNullOrEmpty(Schema) ? $"[{Schema}]." : string.Empty;

        private string TempDBPrefix => Config.UseTempDb ? "#" : string.Empty;

        public string FullTempOutputTableName => $"{SchemaFormated}[{TempTableName}Output]";

        public bool InsertToTempTable { get; set; }

        public bool HasIdentity => IdentityColumn != null;

        public bool KeepIdentity => Config.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);

        public BulkTableInfo(
            DbContext context,
            IEnumerable<TEntity> entities,
            BulkConfig<TEntity> config,
            BulkMergeOperationType operationType)
        {
            EntityMap = context.Db<TEntity>();
            Config = config;
            OperationType = operationType;

            EntityCount = entities.Count();
            TempTableName = string.Format("{0}{1}{2}", TempDBPrefix, EntityMap.TableName, Guid.NewGuid().ToString().Substring(0, 8));
            Schema = EntityMap.Schema;
            TableName = EntityMap.TableName;

            InitIdentifierColumns();
            InitOperationIncludedColumns();
            InitIdentityColumn();
            InitTempTableIncludedColumns();
            InitTempOutputTableIncludedColumns();
        }

        private void InitIdentifierColumns()
        {
            var identifierProperties = default(IEnumerable<string>);

            if (Config.IdentifierColumns != null)
            {
                identifierProperties = ExpressionHelper.GetPropertyNames(Config.IdentifierColumns.Body);
            }

            // Retrieve identifier columns definition
            IdentifierColumns = identifierProperties != null && identifierProperties.Any()
                ? EntityMap.Properties.Where(p => identifierProperties.Any(pn => p.PropertyName == pn))
                : EntityMap.Pks;
        }

        private void InitOperationIncludedColumns()
        {
            var properties = default(IEnumerable<string>);
            var includedColumns = default(IEnumerable<IPropertyMap>);

            if (Config.IncludedColumns != null)
            {
                properties = ExpressionHelper.GetPropertyNames(Config.IncludedColumns.Body);
            }

            // Retrieve included columns definition
            includedColumns = properties != null && properties.Any()
                ? EntityMap.Properties.Where(p => properties.Any(pn => p.PropertyName == pn && !p.IsNavigationProperty))
                : EntityMap.Properties.Where(p => !p.IsNavigationProperty);

            // Exclude identity column if not needed
            if (!(OperationType.HasFlag(BulkMergeOperationType.Insert) && KeepIdentity))
            {
                includedColumns = includedColumns.Where(p => !p.IsIdentity);
            }

            OperationIncludedColumns = includedColumns;
        }

        private void InitIdentityColumn()
        {
            if (Config.SetOutputIdentity)
            {
                IdentityColumn = EntityMap.Properties.Single(p => p.IsIdentity);
            }
        }

        private void InitTempTableIncludedColumns()
        {
            IEnumerable<IPropertyMap> includedColumns = new List<IPropertyMap>();

            if (IdentityColumn != null)
            {
                includedColumns = includedColumns.Union(new List<IPropertyMap> { IdentityColumn });
            }

            if (OperationType.HasFlag(BulkMergeOperationType.Insert))
            {
                includedColumns = includedColumns.Union(OperationIncludedColumns);
            }

            if (OperationType.HasFlag(BulkMergeOperationType.Update))
            {
                includedColumns = includedColumns.Union(OperationIncludedColumns);
                includedColumns = includedColumns.Union(IdentifierColumns);
            }

            if (OperationType.HasFlag(BulkMergeOperationType.Delete))
            {
                includedColumns = includedColumns.Union(OperationIncludedColumns);
                includedColumns = includedColumns.Union(IdentifierColumns);
            }

            TempTableIncludedColumns = includedColumns;
        }

        private void InitTempOutputTableIncludedColumns()
        {
            IEnumerable<IPropertyMap> includedColumns = new List<IPropertyMap>();

            if (IdentityColumn != null)
            {
                includedColumns = includedColumns.Union(new List<IPropertyMap> { IdentityColumn });
            }

            TempOutputTableIncludedColumns = includedColumns;
        }
    }
}
