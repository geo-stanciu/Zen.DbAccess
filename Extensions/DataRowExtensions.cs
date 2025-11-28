using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Models;
using Zen.DbAccess.Utils;

namespace Zen.DbAccess.Extensions;

public static class DataRowExtensions
{
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

            Type t = Nullable.GetUnderlyingType(p.PropertyType) ?? p.PropertyType;

            if (t == typeof(int))
                p.SetValue(rez, Convert.ToInt32(val), null);
            else if (t == typeof(long))
                p.SetValue(rez, Convert.ToInt64(val), null);
            else if (t == typeof(bool))
                p.SetValue(rez, Convert.ToInt32(val) == 1, null);
            else if (t == typeof(decimal))
                p.SetValue(rez, Convert.ToDecimal(val), null);
            else if (t == typeof(TimeOnly))
            {
                if (val.GetType() == typeof(DateTime))
                    p.SetValue(rez, TimeOnly.FromDateTime(Convert.ToDateTime(val)), null);
                else
                    p.SetValue(rez, (TimeOnly)val, null);
            }
            else if (t == typeof(DateOnly))
            {
                if (val.GetType() == typeof(DateTime))
                    p.SetValue(rez, DateOnly.FromDateTime(Convert.ToDateTime(val)), null);
                else
                    p.SetValue(rez, (DateOnly)val, null);
            }
            else if (t == typeof(DateTime))
            {
                if (val.GetType() == typeof(DateOnly))
                    p.SetValue(rez, ((DateOnly)val).ToDateTime(TimeOnly.MinValue), null);
                if (val.GetType() == typeof(TimeOnly))
                    p.SetValue(rez, DateTime.MinValue.Date.Add(((TimeOnly)val).ToTimeSpan()), null);
                else
                    p.SetValue(rez, Convert.ToDateTime(val), null);
            }
            else if (t.IsEnum || t.IsSubclassOf(typeof(Enum)))
                p.SetValue(rez, Enum.ToObject(t, Convert.ToInt32(val)), null);
            else
                p.SetValue(rez, val, null);
        }

        return rez!;
    }
}
