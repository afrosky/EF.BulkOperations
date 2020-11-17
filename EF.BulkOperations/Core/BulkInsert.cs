namespace EF.BulkOperations.Core
{
    using System.Collections.Generic;
    using System.Data.Entity;

    public class BulkInsert : BulkOperationBase
    {
        public static BulkInsert New
        {
            get
            {
                return new BulkInsert();
            }
        }

        private BulkInsert()
        {
        }

        protected override void ExecuteCommand<TEntity>(
            DbContext context, IEnumerable<TEntity> entities, BulkTableInfo<TEntity> tableInfo)
        {
            if (tableInfo.Config.SetOutputIdentity || tableInfo.Config.IsBulkResultEnabled)
            {
                SqlBulkOperation.Merge(context, entities, tableInfo);
            }
            else
            {
                SqlBulkOperation.Insert(context, entities, tableInfo);
            }
        }
    }
}
