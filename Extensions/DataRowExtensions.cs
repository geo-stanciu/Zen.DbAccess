﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Zen.DbAccess.Extensions;

public static class DataRowExtensions
{
    private static Type tint = typeof(int);
    private static Type tintNull = typeof(int?);
    private static Type tlong = typeof(long);
    private static Type tlongNull = typeof(long?);
    private static Type tbool = typeof(bool);
    private static Type tboolNull = typeof(bool?);
    private static Type tdecimal = typeof(decimal);
    private static Type tdecimalNull = typeof(decimal?);
    private static Type tdatetime = typeof(DateTime);
    private static Type tdatetimeNull = typeof(DateTime?);
    private static Type tEnum = typeof(Enum);

    public static T ToModel<T>(this DataRow row)
    {
        Type classType = typeof(T);
        T? rez = (T?)Activator.CreateInstance(classType);

        if (row == null)
            throw new NullReferenceException(nameof(rez));

        for (int i = 0; i < row.Table.Columns.Count; i++)
        {
            PropertyInfo? p = classType.GetProperty(row.Table.Columns[i].ColumnName.ToLower());

            if (p == null)
                continue;

            object val = row[i];

            if (val == null || val == DBNull.Value)
                continue;

            var t = p.PropertyType;

            if (t == tint || t == tintNull)
                p.SetValue(rez, Convert.ToInt32(val), null);
            else if (t == tlong || t == tlongNull)
                p.SetValue(rez, Convert.ToInt64(val), null);
            else if (t == tbool || t == tboolNull)
                p.SetValue(rez, Convert.ToInt32(val) == 1, null);
            else if (t == tdecimal || t == tdecimalNull)
                p.SetValue(rez, Convert.ToDecimal(val), null);
            else if (t == tdatetime || t == tdatetimeNull)
                p.SetValue(rez, Convert.ToDateTime(val), null);
            else if (t.IsSubclassOf(tEnum))
                p.SetValue(rez, Enum.ToObject(t, Convert.ToInt32(val)), null);
            else
                p.SetValue(rez, val, null);
        }

        return rez!;
    }
}
