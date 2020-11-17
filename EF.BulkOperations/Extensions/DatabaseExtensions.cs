namespace EF.BulkOperations.Extensions
{
    using System.Collections.Generic;
    using System.Data.Entity;
    using System.Data.SqlClient;
    using System.Linq;

    /// <summary>
    /// This class contains extension methods on <see cref="System.Data.Entity.Database"/>.
    /// </summary>
    public static class DatabaseExtensions
    {
        /// <summary>
        /// Creates a raw SQL query that will return elements as a list of dictionaries representing result rows.
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns>A list of dictionaries representing result rows.</returns>
        public static IEnumerable<IDictionary<string, object>> SqlQuery(
            this Database db, string sql, params SqlParameter[] parameters)
        {
            var result = new List<IDictionary<string, object>>();

            using (var cmd = db.Connection.CreateCommand())
            {
                cmd.CommandText = sql;

                if (db.CurrentTransaction != null)
                {
                    cmd.Transaction = db.CurrentTransaction.UnderlyingTransaction;
                }

                if (parameters.Any())
                {
                    cmd.Parameters.Add(parameters);
                }

                using (var dataReader = cmd.ExecuteReader())
                {
                    while (dataReader.Read())
                    {
                        var dataRow = new Dictionary<string, object>();

                        for (var fieldCount = 0; fieldCount < dataReader.FieldCount; fieldCount++)
                        {
                            dataRow.Add(dataReader.GetName(fieldCount), dataReader[fieldCount]);
                        }

                        result.Add(dataRow);
                    }
                }
            }

            return result;
        }
    }
}
