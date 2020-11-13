namespace EFBulkExtensions.BulkOperations
{
    using System.Collections.Generic;
    using System.Data.Entity;

    public class BulkDelete : BulkOperationBase
    {
        public static BulkDelete New
        {
            get
            {
                return new BulkDelete();
            }
        }

        private BulkDelete()
        {
        }

        protected override void ExecuteCommand<TEntity>(
            DbContext context, IEnumerable<TEntity> entities, BulkTableInfo<TEntity> tableInfo)
        {
            SqlBulkOperation.Merge(context, entities, tableInfo);
        }
    }
}
