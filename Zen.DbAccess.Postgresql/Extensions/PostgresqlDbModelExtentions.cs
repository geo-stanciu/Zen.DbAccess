using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.Models;

namespace Zen.DbAccess.Postgresql.Extensions;

public static class PostgresqlDbModelExtentions
{
    public static bool IsJsonDataType(this DbModel dbModel, PropertyInfo propertyInfo)
    {
        object[] attrs = propertyInfo.GetCustomAttributes(true);

        if (attrs == null || attrs.Length == 0)
            return false;

        return attrs.Any(x => x is JsonDbTypeAttribute);
    }
}
