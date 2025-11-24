using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Utils;

namespace Zen.DbAccess.Extensions;

public static class DataRowExtensions
{
    private static Type tint = typeof(int);
    private static Type tlong = typeof(long);
    private static Type tbool = typeof(bool);
    private static Type tdecimal = typeof(decimal);
    private static Type tdatetime = typeof(DateTime);
    private static Type tEnum = typeof(Enum);

    public static T ToModel<T>(this DataRow row, ref Dictionary<string, PropertyInfo>? properties, ref bool propertiesAlreadyDetermined)
    {
        Type classType = typeof(T);
        T? rez = (T?)Activator.CreateInstance(classType);

        if (row == null)
            throw new NullReferenceException(nameof(rez));

        if (properties == null)
            properties = new Dictionary<string, PropertyInfo>();

        for (int i = 0; i < row.Table.Columns.Count; i++)
        {
            string colName = row.Table.Columns[i].ColumnName;
            PropertyInfo? p = null;

            if (propertiesAlreadyDetermined)
            {
                if (!properties.TryGetValue(colName, out p))
                    continue;
            }
            else
            {
                p = ColumnNameMapUtils.GetModelPropertyForDbColumn(classType, colName);

                if (p == null)
                    continue;

                properties[colName] = p;
            }

            object val = row[i];

            if (val == null || val == DBNull.Value)
                continue;

            Type t = p.PropertyType;
            Type u = Nullable.GetUnderlyingType(t);

            if (t == tint || (u != null && u == tint))
                p.SetValue(rez, Convert.ToInt32(val), null);
            else if (t == tlong || (u != null && u == tlong))
                p.SetValue(rez, Convert.ToInt64(val), null);
            else if (t == tbool || (u != null && u == tbool))
                p.SetValue(rez, Convert.ToInt32(val) == 1, null);
            else if (t == tdecimal || (u != null && u == tdecimal))
                p.SetValue(rez, Convert.ToDecimal(val), null);
            else if (t == tdatetime || (u != null && u == tdatetime))
                p.SetValue(rez, Convert.ToDateTime(val), null);
            else if (t.IsEnum || t.IsSubclassOf(tEnum))
                p.SetValue(rez, Enum.ToObject(t, Convert.ToInt32(val)), null);
            else if (u != null && (u.IsEnum || u.IsSubclassOf(tEnum)))
                p.SetValue(rez, Enum.ToObject(u, Convert.ToInt32(val)), null);
            else
                p.SetValue(rez, val, null);
        }

        return rez!;
    }
}
