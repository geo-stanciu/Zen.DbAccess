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
        return Attribute.IsDefined(propertyInfo, typeof(JsonDbTypeAttribute));
    }
}
