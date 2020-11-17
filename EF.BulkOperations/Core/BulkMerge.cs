namespace EF.BulkOperations.Core
{
    using System.Collections.Generic;
    using System.Data.Entity;

    public class BulkMerge : BulkOperationBase
    {
        public static BulkMerge New
        {
            get
            {
                return new BulkMerge();
            }
        }

        private BulkMerge()
        {
        }

        protected override void ExecuteCommand<TEntity>(
            DbContext context, IEnumerable<TEntity> entities, BulkTableInfo<TEntity> tableInfo)
        {
            SqlBulkOperation.Merge(context, entities, tableInfo);
        }
    }
}
