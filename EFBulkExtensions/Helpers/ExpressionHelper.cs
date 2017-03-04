namespace EFBulkExtensions.Helpers
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;

    internal class ExpressionHelper
    {
        public static IEnumerable<string> GetPropertyNames(Expression expression)
        {
            var properties = default(IEnumerable<string>);

            if (expression is NewExpression)
            {
                properties = ((NewExpression)expression).Members.Select(m => m.Name);
            }

            return properties;
        }
    }
}
