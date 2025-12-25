using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Helpers;
using Zen.DbAccess.Interfaces;
using Zen.DbAccess.Models;
using Zen.DbAccess.Utils;

namespace Zen.DbAccess.Extensions;

public static class DbClientExtensions
{
    public static List<SqlParam> ExecuteProcedure(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteProcedure(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedureAsync(dbConnectionFactory, query, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedureAsync(dbConnectionFactory, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedureAsync(conn, query, parameters);
    }

    public static DataTable? ExecuteProcedure2DataTable(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure2DataTable(conn, query, parameters);
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataTableAsync(conn, query, parameters);
    }

    public static DataSet? ExecuteProcedure2DataSet(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure2DataSet(conn, query, parameters);
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataSetAsync(dbConnectionFactory, query, parameters);
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataSetAsync(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteFunction(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteFunction(conn, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteFunctionAsync(conn, query, parameters);
    }

    public static object? ExecuteScalar(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteScalar(dbConnectionFactory, query, parameters);
    }

    public static object? ExecuteScalar(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteScalar(conn, query, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteScalarAsync(dbConnectionFactory, query, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteScalarAsync(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteNonQuery(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteNonQuery(dbConnectionFactory, query, parameters);
    }

    public static List<SqlParam> ExecuteNonQuery(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteNonQuery(conn, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteNonQueryAsync(dbConnectionFactory, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteNonQueryAsync(conn, query, parameters);
    }

    public static T? QueryRow<T>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.QueryRow<T>(dbConnectionFactory, query, parameters);
    }

    public static T? QueryRow<T>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.QueryRow<T>(conn, query, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryRowAsync<T>(dbConnectionFactory, query, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryRowAsync<T>(conn, query, parameters);
    }

    public static (List<T>?, List<T2>?, List<T3>?) QueryProcedure<T, T2, T3>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.QueryProcedure<T, T2, T3>(dbConnectionFactory, query, parameters);
    }

    public static (List<T>?, List<T2>?, List<T3>?) QueryProcedure<T, T2, T3>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.QueryProcedure<T, T2, T3>(conn, query, parameters);
    }

    public static (List<T>?, List<T2>?) QueryProcedure<T, T2>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.QueryProcedure<T,T2>(dbConnectionFactory, query, parameters);
    }

    public static (List<T>?, List<T2>?) QueryProcedure<T, T2>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.QueryProcedure<T, T2>(conn, query, parameters);
    }

    public static List<T>? QueryProcedure<T>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.QueryProcedure<T>(dbConnectionFactory, query, parameters);
    }

    public static List<T>? QueryProcedure<T>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.QueryProcedure<T>(conn, query, parameters);
    }

    public static List<T>? Query<T>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.Query<T>(dbConnectionFactory, query, parameters);
    }

    public static List<T>? Query<T>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.Query<T>(conn, query, parameters);
    }

    public static async Task<(List<T>, List<T2>, List<T3>, List<T4>, List<T5>)> QueryProcedureAsync<T, T2, T3, T4, T5>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T, T2, T3, T4, T5>(dbConnectionFactory, query, parameters);
    }

    public static async Task<(List<T>, List<T2>, List<T3>, List<T4>, List<T5>)> QueryProcedureAsync<T, T2, T3, T4, T5>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T, T2, T3, T4, T5>(conn, query, parameters);
    }

    public static async Task<(List<T>, List<T2>, List<T3>, List<T4>)> QueryProcedureAsync<T, T2, T3, T4>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T, T2, T3, T4>(dbConnectionFactory, query, parameters);
    }

    public static async Task<(List<T>, List<T2>, List<T3>, List<T4>)> QueryProcedureAsync<T, T2, T3, T4>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T, T2, T3, T4>(conn, query, parameters);
    }

    public static async Task<(List<T>, List<T2>, List<T3>)> QueryProcedureAsync<T, T2, T3>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T, T2, T3>(dbConnectionFactory, query, parameters);
    }

    public static async Task<(List<T>, List<T2>, List<T3>)> QueryProcedureAsync<T, T2, T3>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T, T2, T3>(conn, query, parameters);
    }

    public static async Task<(List<T>, List<T2>)> QueryProcedureAsync<T, T2>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T, T2>(dbConnectionFactory, query, parameters);
    }

    public static async Task<(List<T>, List<T2>)> QueryProcedureAsync<T, T2>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T, T2>(conn, query, parameters);
    }

    public static async Task<List<T>?> QueryProcedureAsync<T>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T>(dbConnectionFactory, query, parameters);
    }

    public static async Task<List<T>?> QueryProcedureAsync<T>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T>(conn, query, parameters);
    }

    public static async Task<List<T>?> QueryProcedureAsync<T>(this string query, IDbConnectionFactory dbConnectionFactory, bool isTableProcedure, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T>(dbConnectionFactory, query, isTableProcedure, parameters);
    }

    public static async Task<List<T>?> QueryProcedureAsync<T>(this string query, IZenDbConnection conn, bool isTableProcedure, params SqlParam[] parameters)
    {
        return await DBUtils.QueryProcedureAsync<T>(conn, query, isTableProcedure, parameters);
    }

    public static async Task<List<T>?> QueryAsync<T>(this string query, IDbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryAsync<T>(dbConnectionFactory, query, parameters);
    }

    public static async Task<List<T>?> QueryAsync<T>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryAsync<T>(conn, query, queryCacheName: null, parameters);
    }

    public static async Task<List<T>?> QueryAsync<T>(this string query, IZenDbConnection conn, string queryCacheName, params SqlParam[] parameters)
    {
        return await DBUtils.QueryAsync<T>(conn, query, queryCacheName, parameters);
    }

    public static async Task<List<T>?> FetchCursorAsync<T>(this string query, IZenDbConnection conn, string procedureName, params SqlParam[] parameters)
    {
        return await DBUtils.FetchCursorAsync<T>(conn, query, procedureName, parameters);
    }

    public static DataTable? QueryDataTable(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.QueryDataTable(conn, query, parameters);
    }

    public static async Task<DataTable?> QueryDataTableAsync(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryDataTableAsync(conn, query, parameters);
    }

    public static DataSet? QueryDataSet(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.QueryDataSet(conn, query, parameters);
    }

    public static async Task<DataSet?> QueryDataSetAsync(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryDataSetAsync(conn, query, parameters);
    }

    public static void UpdateTable<T>(this string tableName, IDbConnectionFactory dbConnectionFactory, T model)
    {
        DBUtils.UpdateTableAsync(dbConnectionFactory, tableName, model).Wait();
    }

    public static void UpdateTable<T>(this string tableName, IZenDbConnection conn, T model)
    {
        DBUtils.UpdateTable(conn, tableName, model);
    }

    public static void UpdateTable<T>(this string tableName, IDbConnectionFactory dbConnectionFactory, List<T> models)
    {
        DBUtils.UpdateTableAsync(dbConnectionFactory, tableName, models).Wait();
    }

    public static void UpdateTable<T>(this string tableName, IZenDbConnection conn, List<T> models)
    {
        DBUtils.UpdateTable(conn, tableName, models);
    }

    public static T? QueryRow<T>(this DbCommand cmd)
    {
        return QueryRow<T>(cmd, tx: null);
    }

    public static T? QueryRow<T>(this DbCommand cmd, DbTransaction? tx)
    {
        bool found = false;

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        using (var dRead = cmd.ExecuteReader())
        {
            string cachekey = $"{typeof(T).FullName}_{cmd.Connection?.GetType()}_{Sha256Helper.Sha256(cmd.CommandText)}";

            Dictionary<string, PropertyInfo>? properties = CacheHelper.TryGetValue<Dictionary<string, PropertyInfo>>(cachekey, out var cachedProperties) ? cachedProperties : null;
            bool propertiesAlreadyDetermined = properties != null;
            bool shoudCacheProperties = !propertiesAlreadyDetermined;

            while (dRead.Read())
            {
                found = true;
                var rez = dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined);

                if (shoudCacheProperties)
                    CacheHelper.TryAdd(cachekey, properties);

                return rez;
            }
        }

        if (!found)
            throw new Exception("No data found");

        return default;
    }

    public static List<T> Query<T>(this DbCommand cmd)
    {
        return Query<T>(cmd, tx: null);
    }

    public static List<T> Query<T>(this DbCommand cmd, DbTransaction? tx)
    {
        List<T> rez = new List<T>();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        using (var dRead = cmd.ExecuteReader())
        {
            string cachekey = $"{typeof(T).FullName}_{cmd.Connection?.GetType()}_{Sha256Helper.Sha256(cmd.CommandText)}";

            Dictionary<string, PropertyInfo>? properties = CacheHelper.TryGetValue<Dictionary<string, PropertyInfo>>(cachekey, out var cachedProperties) ? cachedProperties : null;
            bool propertiesAlreadyDetermined = properties != null;
            bool shoudCacheProperties = !propertiesAlreadyDetermined;

            while (dRead.Read())
            {
                rez.Add(dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined));
            }

            if (shoudCacheProperties)
                CacheHelper.TryAdd(cachekey, properties);
        }

        return rez;
    }

    public static Task<T?> QueryRowAsync<T>(this DbCommand cmd)
    {
        return QueryRowAsync<T>(cmd, tx: null);
    }

    public static async Task<T?> QueryRowAsync<T>(this DbCommand cmd, DbTransaction? tx)
    {
        bool found = false;

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        using (var dRead = await cmd.ExecuteReaderAsync())
        {
            string cachekey = $"{typeof(T).FullName}_{cmd.Connection?.GetType()}_{Sha256Helper.Sha256(cmd.CommandText)}";

            Dictionary<string, PropertyInfo>? properties = CacheHelper.TryGetValue<Dictionary<string, PropertyInfo>>(cachekey, out var cachedProperties) ? cachedProperties : null;
            bool propertiesAlreadyDetermined = properties != null;
            bool shoudCacheProperties = !propertiesAlreadyDetermined;

            while (await dRead.ReadAsync())
            {
                found = true;
                var rez = dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined);

                if (shoudCacheProperties)
                    CacheHelper.TryAdd(cachekey, properties);

                return rez;
            }
        }

        if (!found)
            throw new Exception("No data found");

        return default;
    }

    public static Task<(List<T>, List<T2>, List<T3>, List<T4>, List<T5>)> QueryAsync<T, T2, T3, T4, T5>(this DbCommand cmd, IZenDbConnection conn, string? queryCacheName)
    {
        return QueryAsync<T, T2, T3, T4, T5>(cmd, conn, tx: null, queryCacheName: queryCacheName);
    }

    public static async Task<(List<T>, List<T2>, List<T3>, List<T4>, List<T5>)> QueryAsync<T, T2, T3, T4, T5>(this DbCommand cmd, IZenDbConnection conn, DbTransaction? tx, string? queryCacheName)
    {
        List<T> rez = new List<T>();
        List<T2> rez2 = new List<T2>();
        List<T3> rez3 = new List<T3>();
        List<T4> rez4 = new List<T4>();
        List<T5> rez5 = new List<T5>();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        using (var dRead = await cmd.ExecuteReaderAsync())
        {
            int k = 0;

            do
            {
                string cachekey = $"{typeof(T).FullName}_{conn.DbType}_{k}_{Sha256Helper.Sha256(queryCacheName ?? cmd.CommandText)}";

                Dictionary<string, PropertyInfo>? properties = CacheHelper.TryGetValue<Dictionary<string, PropertyInfo>>(cachekey, out var cachedProperties) ? cachedProperties : null;
                bool propertiesAlreadyDetermined = properties != null;
                bool shoudCacheProperties = !propertiesAlreadyDetermined;

                while (await dRead.ReadAsync())
                {
                    if (k == 0)
                        rez.Add(dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined));
                    else if (k == 1)
                        rez2.Add(dRead.Row2Model<T2>(ref properties, ref propertiesAlreadyDetermined));
                    else if (k == 2)
                        rez3.Add(dRead.Row2Model<T3>(ref properties, ref propertiesAlreadyDetermined));
                    else if (k == 3)
                        rez4.Add(dRead.Row2Model<T4>(ref properties, ref propertiesAlreadyDetermined));
                    else
                        rez5.Add(dRead.Row2Model<T5>(ref properties, ref propertiesAlreadyDetermined));
                }

                k++;

                if (shoudCacheProperties)
                    CacheHelper.TryAdd(cachekey, properties);
            }
            while (await dRead.NextResultAsync());
        }

        return (rez, rez2, rez3, rez4, rez5);
    }

    public static Task<(List<T>, List<T2>, List<T3>, List<T4>)> QueryAsync<T, T2, T3, T4>(this DbCommand cmd, IZenDbConnection conn, string? queryCacheName)
    {
        return QueryAsync<T, T2, T3, T4>(cmd, conn, tx: null, queryCacheName: queryCacheName);
    }

    public static async Task<(List<T>, List<T2>, List<T3>, List<T4>)> QueryAsync<T, T2, T3, T4>(this DbCommand cmd, IZenDbConnection conn, DbTransaction? tx, string? queryCacheName)
    {
        List<T> rez = new List<T>();
        List<T2> rez2 = new List<T2>();
        List<T3> rez3 = new List<T3>();
        List<T4> rez4 = new List<T4>();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        using (var dRead = await cmd.ExecuteReaderAsync())
        {
            int k = 0;

            do
            {
                string cachekey = $"{typeof(T).FullName}_{conn.DbType}_{k}_{Sha256Helper.Sha256(queryCacheName ?? cmd.CommandText)}";

                Dictionary<string, PropertyInfo>? properties = CacheHelper.TryGetValue<Dictionary<string, PropertyInfo>>(cachekey, out var cachedProperties) ? cachedProperties : null;
                bool propertiesAlreadyDetermined = properties != null;
                bool shoudCacheProperties = !propertiesAlreadyDetermined;

                while (await dRead.ReadAsync())
                {
                    if (k == 0)
                        rez.Add(dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined));
                    else if (k == 1)
                        rez2.Add(dRead.Row2Model<T2>(ref properties, ref propertiesAlreadyDetermined));
                    else if (k == 2)
                        rez3.Add(dRead.Row2Model<T3>(ref properties, ref propertiesAlreadyDetermined));
                    else
                        rez4.Add(dRead.Row2Model<T4>(ref properties, ref propertiesAlreadyDetermined));
                }

                k++;

                if (shoudCacheProperties)
                    CacheHelper.TryAdd(cachekey, properties);
            }
            while (await dRead.NextResultAsync());
        }

        return (rez, rez2, rez3, rez4);
    }

    public static Task<(List<T>, List<T2>, List<T3>)> QueryAsync<T, T2, T3>(this DbCommand cmd, IZenDbConnection conn, string? queryCacheName)
    {
        return QueryAsync<T, T2, T3>(cmd, conn, tx: null, queryCacheName: queryCacheName);
    }

    public static async Task<(List<T>, List<T2>, List<T3>)> QueryAsync<T, T2, T3>(this DbCommand cmd, IZenDbConnection conn, DbTransaction? tx, string? queryCacheName)
    {
        List<T> rez = new List<T>();
        List<T2> rez2 = new List<T2>();
        List<T3> rez3 = new List<T3>();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        using (var dRead = await cmd.ExecuteReaderAsync())
        {
            int k = 0;

            do
            {
                string cachekey = $"{typeof(T).FullName}_{conn.DbType}_{k}_{Sha256Helper.Sha256(queryCacheName ?? cmd.CommandText)}";

                Dictionary<string, PropertyInfo>? properties = CacheHelper.TryGetValue<Dictionary<string, PropertyInfo>>(cachekey, out var cachedProperties) ? cachedProperties : null;
                bool propertiesAlreadyDetermined = properties != null;
                bool shoudCacheProperties = !propertiesAlreadyDetermined;

                while (await dRead.ReadAsync())
                {
                    if (k == 0)
                        rez.Add(dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined));
                    else if (k == 1)
                        rez2.Add(dRead.Row2Model<T2>(ref properties, ref propertiesAlreadyDetermined));
                    else
                        rez3.Add(dRead.Row2Model<T3>(ref properties, ref propertiesAlreadyDetermined));
                }

                k++;

                if (shoudCacheProperties)
                    CacheHelper.TryAdd(cachekey, properties);
            }
            while (await dRead.NextResultAsync());
        }

        return (rez, rez2, rez3);
    }

    public static Task<(List<T>, List<T2>)> QueryAsync<T, T2>(this DbCommand cmd, IZenDbConnection conn, string? queryCacheName)
    {
        return QueryAsync<T, T2>(cmd, conn, tx: null, queryCacheName: queryCacheName);
    }

    public static async Task<(List<T>, List<T2>)> QueryAsync<T, T2>(this DbCommand cmd, IZenDbConnection conn, DbTransaction? tx, string? queryCacheName)
    {
        List<T> rez = new List<T>();
        List<T2> rez2 = new List<T2>();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        using (var dRead = await cmd.ExecuteReaderAsync())
        {
            int k = 0;

            do
            {
                string cachekey = $"{typeof(T).FullName}_{conn.DbType}_{k}_{Sha256Helper.Sha256(queryCacheName ?? cmd.CommandText)}";

                Dictionary<string, PropertyInfo>? properties = CacheHelper.TryGetValue<Dictionary<string, PropertyInfo>>(cachekey, out var cachedProperties) ? cachedProperties : null;
                bool propertiesAlreadyDetermined = properties != null;
                bool shoudCacheProperties = !propertiesAlreadyDetermined;

                while (await dRead.ReadAsync())
                {
                    if (k == 0)
                        rez.Add(dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined));
                    else
                        rez2.Add(dRead.Row2Model<T2>(ref properties, ref propertiesAlreadyDetermined));
                }

                k++;

                if (shoudCacheProperties)
                    CacheHelper.TryAdd(cachekey, properties);
            }
            while (await dRead.NextResultAsync());
        }

        return (rez, rez2);
    }

    public static Task<List<T>> QueryAsync<T>(this DbCommand cmd, IZenDbConnection conn, string? queryCacheName)
    {
        return QueryAsync<T>(cmd, conn, tx: null, queryCacheName: queryCacheName);
    }

    public static async Task<List<T>> QueryAsync<T>(this DbCommand cmd, IZenDbConnection conn, DbTransaction? tx, string? queryCacheName)
    {
        List<T> rez = new List<T>();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        using (var dRead = await cmd.ExecuteReaderAsync())
        {
            string cachekey = $"{typeof(T).FullName}_{conn.DbType}_{Sha256Helper.Sha256(queryCacheName ?? cmd.CommandText)}";

            Dictionary<string, PropertyInfo>? properties = CacheHelper.TryGetValue<Dictionary<string, PropertyInfo>>(cachekey, out var cachedProperties) ? cachedProperties : null;
            bool propertiesAlreadyDetermined = properties != null;
            bool shoudCacheProperties = !propertiesAlreadyDetermined;

            while (await dRead.ReadAsync())
            {
                rez.Add(dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined));
            }

            if (shoudCacheProperties)
                CacheHelper.TryAdd(cachekey, properties);
        }

        return rez;
    }

    private static T Row2Model<T>(this DbDataReader dRead, ref Dictionary<string, PropertyInfo>? properties, ref bool propertiesAlreadyDetermined)
    {
        var classType = typeof(T);
        T? rez = (T?)Activator.CreateInstance(classType);

        if (rez == null)
            throw new NullReferenceException(nameof(rez));

        if (properties == null)
            properties = new Dictionary<string, PropertyInfo>();

        var customColumnNames = classType
            .GetProperties()
            .Select(x => new { x.Name, Attributes = x.GetCustomAttributes<DbNameAttribute>()?.ToList() })
            .Where(x => x.Attributes != null && x.Attributes.Count > 0)
            .ToDictionary(x => x.Name, y => y.Attributes!);

        for (int i = 0; i < dRead.FieldCount; i++)
        {
            string dbCol = dRead.GetName(i);
            PropertyInfo? p = null;

            if (propertiesAlreadyDetermined)
            {
                if (!properties.TryGetValue(dbCol, out p))
                    continue;
            }
            else
            {
                p = ColumnNameMapUtils.GetModelPropertyForDbColumn(classType, dbCol, customColumnNames);

                if (p == null)
                    continue;

                properties[dbCol] = p;
            }

            var val = dRead[i];

            PropertyMapHelper.SetPropertyValue<T>(rez, p, val);
        }

        if (!propertiesAlreadyDetermined)
            propertiesAlreadyDetermined = true;

        return rez;
    }
}
