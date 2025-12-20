using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Helpers;

namespace Zen.DbAccess.Extensions;

public static class DataTableExtensions
{
    public static T FirstRowToModel<T>(this DataTable dt)
    {
        if (dt.Rows.Count == 0)
            throw new ArgumentException("DataTable contains 0 rows");

        string cachekey = $"{typeof(T).FullName}_{dt.Columns.Count}_{(dt.Columns.Count > 0 ? dt.Columns[0].ColumnName : "")}";

        Dictionary<string, PropertyInfo>? properties = CacheHelper.TryGetValue<Dictionary<string, PropertyInfo>>(cachekey, out var cachedProperties) ? cachedProperties : null;
        bool propertiesAlreadyDetermined = properties != null;
        bool shoudCacheProperties = !propertiesAlreadyDetermined;

        var result = dt.Rows[0].ToModel<T>(ref properties, ref propertiesAlreadyDetermined);

        if (shoudCacheProperties)
            CacheHelper.TryAdd(cachekey, properties);

        return result;
    }

    public static List<T> ToList<T>(this DataTable dt)
    {
        List<T> data = new List<T>();

        string cachekey = $"{typeof(T).FullName}_{dt.Columns.Count}_{(dt.Columns.Count > 0 ? dt.Columns[0].ColumnName : "")}";

        Dictionary<string, PropertyInfo>? properties = CacheHelper.TryGetValue<Dictionary<string, PropertyInfo>>(cachekey, out var cachedProperties) ? cachedProperties : null;
        bool propertiesAlreadyDetermined = properties != null;
        bool shoudCacheProperties = !propertiesAlreadyDetermined;

        foreach (DataRow row in dt.Rows)
        {
            T item = row.ToModel<T>(ref properties, ref propertiesAlreadyDetermined);
            data.Add(item);
        }

        if (shoudCacheProperties)
            CacheHelper.TryAdd(cachekey, properties);

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
