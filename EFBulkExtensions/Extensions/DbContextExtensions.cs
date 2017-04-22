namespace EFBulkExtensions.Extensions
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Entity;
    using System.Data.Entity.Core.Metadata.Edm;
    using System.Data.Entity.Infrastructure;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Reflection;
    using BulkOperations;
    using EntityFramework.MappingAPI;
    using EntityFramework.MappingAPI.Extensions;

    /// <summary>
    /// This class contains extension methods on <see cref="System.Data.Entity.DbContext"/>.
    /// </summary>
    public static class DbContextExtensions
    {
        /// <summary>
        /// Inserts a list of <typeparamref name="TEntity"/>.
        /// </summary>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="context">The database context.</param>
        /// <param name="entities">The list of entities to insert.</param>
        /// <param name="settings">The bulk operation settings.</param>
        /// <returns>The number of affected entities.</returns>
        public static int BulkInsert<TEntity>(
            this DbContext context,
            IEnumerable<TEntity> entities,
            Action<BulkOperationSettings<TEntity>> settings = null)
            where TEntity : class
        {
            return BulkInsertOperation.New.Execute(context, entities, settings);
        }

        /// <summary>
        /// Updates a list of <typeparamref name="TEntity"/>.
        /// </summary>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="context">The database context.</param>
        /// <param name="entities">The list of entities to update.</param>
        /// <param name="settings">The bulk operation settings.</param>
        /// <returns>The number of affected entities.</returns>
        public static int BulkUpdate<TEntity>(
            this DbContext context,
            IEnumerable<TEntity> entities,
            Action<BulkOperationSettings<TEntity>> settings = null)
            where TEntity : class
        {
            return BulkUpdateOperation.New.Execute(context, entities, settings);
        }

        /// <summary>
        /// Deletes a list of <typeparamref name="TEntity"/>.
        /// </summary>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="context">The database context.</param>
        /// <param name="entities">The list of entities to delete.</param>
        /// <param name="settings">The bulk operation settings.</param>
        /// <returns>The number of affected entities.</returns>
        public static int BulkDelete<TEntity>(
            this DbContext context,
            IEnumerable<TEntity> entities,
            Action<BulkOperationSettings<TEntity>> settings = null)
            where TEntity : class
        {
            return BulkDeleteOperation.New.Execute(context, entities, settings);
        }

        internal static bool ContainsTable<TEntity>(
            this IObjectContextAdapter context)
            where TEntity : class
        {
            var entityName = typeof(TEntity).Name;
            var workspace = context.ObjectContext.MetadataWorkspace;

            return workspace.GetItems<EntityType>(DataSpace.CSpace).Any(e => e.Name == entityName);
        }

        internal static string RandomTableName<TEntity>(
            this DbContext context)
        {
            var entityMap = context.Db<TEntity>();
            return $"[{entityMap.Schema}].[_tmp{Guid.NewGuid().ToString()}]";
        }

        internal static string GetTableName<TEntity>(
            this DbContext context)
            where TEntity : class
        {
            var entityMap = context.Db<TEntity>();
            return $"[{entityMap.Schema}].[{entityMap.TableName}]";
        }

        internal static DbContextTransaction InternalTransaction(
            this DbContext context)
        {
            DbContextTransaction transaction = null;

            if (context.Database.CurrentTransaction == null)
            {
                transaction = context.Database.BeginTransaction();
            }

            return transaction;
        }

        internal static IEnumerable<IPropertyMap> GetTablePrimaryKeys<TEntity>(
            this DbContext context)
            where TEntity : class
        {
            var entityMap = context.Db<TEntity>();
            return entityMap.Pks;
        }

        internal static IEnumerable<IPropertyMap> GetTableColumns<TEntity>(
            this DbContext context)
            where TEntity : class
        {
            var entityMap = context.Db<TEntity>();
            return entityMap.Properties.Where(map => !map.IsNavigationProperty);
        }

        internal static IEnumerable<IPropertyMap> GetTableColumns<TEntity>(
            this DbContext context,
            IEnumerable<string> propertyNames)
            where TEntity : class
        {
            return context.GetTableColumns<TEntity>().Where(p => propertyNames.Any(pn => p.PropertyName == pn));
        }

        internal static DataTable ToDataTable<TEntity>(
            this DbContext context,
            IEnumerable<TEntity> entities,
            IEnumerable<IPropertyMap> columns)
            where TEntity : class
        {
            var dt = new DataTable(typeof(TEntity).Name);

            // Create columns
            var entityProperties = typeof(TEntity).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            var dataColumns = columns
                .Select(c => new DataColumn(c.ColumnName, Nullable.GetUnderlyingType(c.Type) ?? c.Type));

            dt.Columns.AddRange(dataColumns.ToArray());

            // Create rows
            foreach (var item in entities)
            {
                var values = new List<object>();

                foreach (var c in columns)
                {
                    var prop = entityProperties.SingleOrDefault(pi => pi.Name == c.PropertyName);
                    if (prop != null)
                        values.Add(prop.GetValue(item, null));
                }

                dt.Rows.Add(values.ToArray());
            }

            return dt;
        }

        internal static void UpdateEntitiesIdentity<TEntity>(
            this DbContext context,
            IEnumerable<long> identities,
            IPropertyMap identityColumn,
            IList<TEntity> entities)
        {
            if (identityColumn != null)
            {
                var counter = 0;

                foreach (var result in identities)
                {
                    var property = entities[counter].GetType().GetProperty(identityColumn.PropertyName);

                    if (!property.CanWrite)
                    {
                        throw new Exception();
                    }

                    property.SetValue(entities[counter], result, null);
                    counter++;
                }
            }
        }

        internal static IDictionary<string, string> GetPrimitiveType<TEntity>(
            this DbContext context)
            where TEntity : class
        {
            var map = new Dictionary<string, string>();
            var members = context.EntitySchema<TEntity>().Members;

            return members.ToDictionary(m => m.Name, m => m.TypeUsage.EdmType.Name);
        }

        private static EntityType EntitySchema<TEntity>(
            this IObjectContextAdapter context) where TEntity : class
        {
            var items = context.ObjectContext.MetadataWorkspace.GetItems<EntityType>(DataSpace.SSpace);
            var name = typeof(TEntity).Name;

            return items.SingleOrDefault(type => type.Name == name);
        }

        internal static void BulkCopy(
            this DbContext context,
            DataTable dataTable,
            string tableName,
            SqlBulkCopyOptions sqlBulkCopyOptions)
        {
            using (
                var bulkcopy = new SqlBulkCopy(
                    (SqlConnection)context.Database.Connection,
                    sqlBulkCopyOptions,
                    (SqlTransaction)context.Database.CurrentTransaction.UnderlyingTransaction))
            {
                foreach (var dataTableColumn in dataTable.Columns)
                {
                    bulkcopy.ColumnMappings.Add(dataTableColumn.ToString(), dataTableColumn.ToString());
                }

                bulkcopy.DestinationTableName = tableName;
                bulkcopy.BulkCopyTimeout = context.Database.Connection.ConnectionTimeout;
                bulkcopy.WriteToServer(dataTable);
            }
        }

        internal static IPropertyMap GetTableIdentityColumn<TEntity>(
            this DbContext context)
            where TEntity : class
        {
            var entityMap = context.Db<TEntity>();
            return entityMap.Properties.FirstOrDefault(map => map.IsIdentity);
        }
    }
}
