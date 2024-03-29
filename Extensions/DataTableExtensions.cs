﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Zen.DbAccess.Extensions;

public static class DataTableExtensions
{
    public static T FirstRowToModel<T>(this DataTable dt)
    {
        if (dt.Rows.Count == 0)
            throw new ArgumentException("DataTable contains 0 rows");

        Dictionary<string, PropertyInfo>? properties = null;
        bool propertiesAlreadyDetermined = false;

        return dt.Rows[0].ToModel<T>(ref properties, ref propertiesAlreadyDetermined);
    }

    public static List<T> ToList<T>(this DataTable dt)
    {
        List<T> data = new List<T>();
        Dictionary<string, PropertyInfo>? properties = null;
        bool propertiesAlreadyDetermined = false;

        foreach (DataRow row in dt.Rows)
        {
            T item = row.ToModel<T>(ref properties, ref propertiesAlreadyDetermined);
            data.Add(item);
        }
        return data;
    }

    public static void CreateRowFromModel<T>(this DataTable dt, T model)
    {
        DataRow dr = dt.NewRow();
        Type classType = typeof(T);

        foreach (PropertyInfo propertyInfo in classType.GetProperties())
        {
            if (!dt.Columns.Contains(propertyInfo.Name))
                continue;

            dr[propertyInfo.Name] = propertyInfo.GetValue(model) ?? DBNull.Value;
        }

        dt.Rows.Add(dr);
    }
}
