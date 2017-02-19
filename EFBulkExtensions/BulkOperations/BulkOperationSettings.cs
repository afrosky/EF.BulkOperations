namespace EFBulkExtensions.BulkOperations
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// This class regroups bulk operation available settings.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    public class BulkOperationSettings<TEntity>
        where TEntity : class
    {
        /// <summary>
        /// The included columns. By default, all columns are included.
        /// </summary>
        public Expression<Func<TEntity, object>> IncludedColumns { get; set; }

        /// <summary>
        /// True if identity should be returned, otherwise false.
        /// </summary>
        public bool IsIdentityOutputEnabled { get; set; }
    }
}
