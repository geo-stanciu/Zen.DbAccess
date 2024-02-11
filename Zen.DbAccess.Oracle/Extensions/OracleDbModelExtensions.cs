using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Models;

namespace Zen.DbAccess.Oracle.Extensions;

public static class OracleDbModelExtensions
{
    public static bool IsClobDataType(this DbModel dbModel, PropertyInfo propertyInfo)
    {
        object[] attrs = propertyInfo.GetCustomAttributes(true);

        if (attrs == null || attrs.Length == 0)
            return false;

        return attrs.Any(x => x is ClobDbTypeAttribute || x is JsonDbTypeAttribute);
    }
}
