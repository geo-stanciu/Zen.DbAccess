using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Zen.DbAccess.Helpers;

internal static class PropertyMapHelper
{
    internal static void SetPropertyValue<T>(T? rez, PropertyInfo? p, object? val)
    {
        if (rez == null || p == null || val == null || val == DBNull.Value)
            return;

        Type t = Nullable.GetUnderlyingType(p.PropertyType) ?? p.PropertyType;

        if (t == typeof(int))
        {
            p.SetValue(rez, Convert.ToInt32(val), null);
        }
        else if (t == typeof(long))
        {
            p.SetValue(rez, Convert.ToInt64(val), null);
        }
        else if (t == typeof(bool))
        {
            Type tVal = val.GetType();
            Type realValType = Nullable.GetUnderlyingType(tVal) ?? tVal;

            if (realValType == typeof(bool))
                p.SetValue(rez, val, null);
            else
                p.SetValue(rez, Convert.ToInt32(val) == 1, null);
        }
        else if (t == typeof(decimal))
        {
            p.SetValue(rez, Convert.ToDecimal(val), null);
        }
        else if (t == typeof(TimeOnly))
        {
            Type tVal = val.GetType();
            Type realValType = Nullable.GetUnderlyingType(tVal) ?? tVal;

            if (realValType == typeof(DateTime))
                p.SetValue(rez, TimeOnly.FromDateTime(Convert.ToDateTime(val)), null);
            else if (realValType == typeof(string))
                p.SetValue(rez, TimeOnly.FromDateTime(DateTime.Parse((val as string)!)), null);
            else
                p.SetValue(rez, (TimeOnly)val, null);
        }
        else if (t == typeof(DateOnly))
        {
            Type tVal = val.GetType();
            Type realValType = Nullable.GetUnderlyingType(tVal) ?? tVal;

            if (realValType == typeof(DateTime))
                p.SetValue(rez, DateOnly.FromDateTime(Convert.ToDateTime(val)), null);
            else if (realValType == typeof(string))
                p.SetValue(rez, DateOnly.FromDateTime(DateTime.Parse((val as string)!)), null);
            else
                p.SetValue(rez, (DateOnly)val, null);
        }
        else if (t == typeof(DateTime))
        {
            Type tVal = val.GetType();
            Type realValType = Nullable.GetUnderlyingType(tVal) ?? tVal;

            if (realValType == typeof(DateOnly))
                p.SetValue(rez, ((DateOnly)val).ToDateTime(TimeOnly.MinValue), null);
            else if (realValType == typeof(TimeOnly))
                p.SetValue(rez, DateTime.MinValue.Date.Add(((TimeOnly)val).ToTimeSpan()), null);
            else if (realValType == typeof(string))
                p.SetValue(rez, DateTime.Parse((val as string)!), null);
            else
                p.SetValue(rez, Convert.ToDateTime(val), null);
        }
        else if (t.IsEnum || t.IsSubclassOf(typeof(Enum)))
        {
            p.SetValue(rez, Enum.ToObject(t, Convert.ToInt32(val)), null);
        }
        else
        {
            p.SetValue(rez, val, null);
        }
    }
}
