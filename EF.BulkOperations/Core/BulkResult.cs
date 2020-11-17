namespace EF.BulkOperations.Core
{
    public class BulkResult
    {
        /// <summary>
        /// The number of inserted rows.
        /// </summary>
        public int Inserted { get; set; }

        /// <summary>
        /// The number of updated rows.
        /// </summary>
        public int Updated { get; set; }

        /// <summary>
        /// The number of deleted rows.
        /// </summary>
        public int Deleted { get; set; }
    }
}
