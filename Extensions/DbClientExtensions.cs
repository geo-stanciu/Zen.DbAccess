using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Models;
using Zen.DbAccess.Utils;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Interfaces;

namespace Zen.DbAccess.Extensions;

public static class DbClientExtensions
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

    public static List<SqlParam> ExecuteProcedure(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteProcedure(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedureAsync(dbConnectionFactory, query, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
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

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
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

    public static object? ExecuteScalar(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteScalar(dbConnectionFactory, query, parameters);
    }

    public static object? ExecuteScalar(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteScalar(conn, query, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteScalarAsync(dbConnectionFactory, query, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteScalarAsync(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteNonQuery(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteNonQuery(dbConnectionFactory, query, parameters);
    }

    public static List<SqlParam> ExecuteNonQuery(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteNonQuery(conn, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteNonQueryAsync(dbConnectionFactory, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteNonQueryAsync(conn, query, parameters);
    }

    public static T? QueryRow<T>(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.QueryRow<T>(dbConnectionFactory, query, parameters);
    }

    public static T? QueryRow<T>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.QueryRow<T>(conn, query, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryRowAsync<T>(dbConnectionFactory, query, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryRowAsync<T>(conn, query, parameters);
    }

    public static List<T>? Query<T>(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.Query<T>(dbConnectionFactory, query, parameters);
    }

    public static List<T>? Query<T>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.Query<T>(conn, query, parameters);
    }

    public static async Task<List<T>?> QueryAsync<T>(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryAsync<T>(dbConnectionFactory, query, parameters);
    }

    public static async Task<List<T>?> QueryAsync<T>(this string query, IZenDbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryAsync<T>(conn, query, parameters);
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

    public static void UpdateTable<T>(this string tableName, DbConnectionFactory dbConnectionFactory, T model)
    {
        DBUtils.UpdateTableAsync(dbConnectionFactory, tableName, model).Wait();
    }

    public static void UpdateTable<T>(this string tableName, IZenDbConnection conn, T model)
    {
        DBUtils.UpdateTable(conn, tableName, model);
    }

    public static void UpdateTable<T>(this string tableName, DbConnectionFactory dbConnectionFactory, List<T> models)
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
            Dictionary<string, PropertyInfo>? properties = null;
            bool propertiesAlreadyDetermined = false;

            while (dRead.Read())
            {
                found = true;
                var rez = dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined);
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
            Dictionary<string, PropertyInfo>? properties = null;
            bool propertiesAlreadyDetermined = false;

            while (dRead.Read())
            {
                rez.Add(dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined));
            }
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
            Dictionary<string, PropertyInfo>? properties = null;
            bool propertiesAlreadyDetermined = false;

            while (await dRead.ReadAsync())
            {
                found = true;
                var rez = dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined);
                return rez;
            }
        }

        if (!found)
            throw new Exception("No data found");

        return default;
    }

    public static Task<List<T>> QueryAsync<T>(this DbCommand cmd)
    {
        return QueryAsync<T>(cmd, tx: null);
    }

    public static async Task<List<T>> QueryAsync<T>(this DbCommand cmd, DbTransaction? tx)
    {
        List<T> rez = new List<T>();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        using (var dRead = await cmd.ExecuteReaderAsync())
        {
            Dictionary<string, PropertyInfo>? properties = null;
            bool propertiesAlreadyDetermined = false;

            while (await dRead.ReadAsync())
            {
                rez.Add(dRead.Row2Model<T>(ref properties, ref propertiesAlreadyDetermined));
            }
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
                p = ColumnNameMapUtils.GetModelPropertyForDbColumn(classType, dbCol);

                if (p == null)
                    continue;

                properties[dbCol] = p;
            }

            var val = dRead[i];

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

        if (!propertiesAlreadyDetermined)
            propertiesAlreadyDetermined = true;

        return rez;
    }
}
