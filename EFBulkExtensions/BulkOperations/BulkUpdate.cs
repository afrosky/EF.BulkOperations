namespace EFBulkExtensions.BulkOperations
{
    using System.Collections.Generic;
    using System.Data.Entity;

    public class BulkUpdate : BulkOperationBase
    {
        public static BulkUpdate New
        {
            get
            {
                return new BulkUpdate();
            }
        }

        private BulkUpdate()
        {
        }

        protected override void ExecuteCommand<TEntity>(
            DbContext context, IEnumerable<TEntity> entities, BulkTableInfo<TEntity> tableInfo)
        {
            SqlBulkOperation.Merge(context, entities, tableInfo);
        }
    }
}
