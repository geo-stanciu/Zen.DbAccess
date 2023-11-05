using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Zen.DbAccess.Utils;

internal static class ColumnNameMapUtils
{
    public static PropertyInfo? GetModelPropertyForDbColumn(Type classType, string dbCol)
    {
        var p = classType.GetProperty(dbCol);

        if (p != null)
            return p;

        var lCol = dbCol.ToLower();

        p = classType.GetProperty(lCol);

        if (p != null)
            return p;

        string[] parts = lCol.Split('_', StringSplitOptions.RemoveEmptyEntries);
        StringBuilder sbCol = new StringBuilder();

        foreach (string part in parts)
        {
            sbCol
                .Append(part.Substring(0, 1).ToUpper())
                .Append(part.Substring(1).ToLower());
        }

        var camelCaseDbCol = sbCol.ToString();

        p = classType.GetProperty(camelCaseDbCol);

        if (p != null)
            return p;

        return null;
    }
}
