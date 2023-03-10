using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Shared.Models;
using Zen.DbAccess.Utils;
using Zen.DbAccess.Factories;

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

    public static List<SqlParam> ExecuteProcedure(this string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure(conn_str, query, parameters);
    }

    public static List<SqlParam> ExecuteProcedure(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure(dbtype, conn_str, query, parameters);
    }

    public static List<SqlParam> ExecuteProcedure(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteProcedure(this DbConnection conn, string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteProcedure(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedureAsync(dbConnectionFactory, query, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(this string query, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedureAsync(conn_str, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedureAsync(dbtype, conn_str, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedureAsync(dbConnectionFactory, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedureAsync(conn, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedureAsync(conn, query, parameters);
    }

    public static DataTable? ExecuteProcedure2DataTable(this string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure2DataTable(conn_str, query, parameters);
    }

    public static DataTable? ExecuteProcedure2DataTable(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure2DataTable(dbtype, conn_str, query, parameters);
    }

    public static DataTable? ExecuteProcedure2DataTable(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure2DataTable(conn, query, parameters);
    }

    public static DataTable? ExecuteProcedure2DataTable(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure2DataTable(conn, query, parameters);
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(this string query, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataTableAsync(conn_str, query, parameters);
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataTableAsync(dbtype, conn_str, query, parameters);
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataTableAsync(conn, query, parameters);
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataTableAsync(conn, query, parameters);
    }

    public static DataSet? ExecuteProcedure2DataSet(this string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure2DataSet(conn_str, query, parameters);
    }

    public static DataSet? ExecuteProcedure2DataSet(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure2DataSet(dbtype, conn_str, query, parameters);
    }

    public static DataSet? ExecuteProcedure2DataSet(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure2DataSet(conn, query, parameters);
    }

    public static DataSet? ExecuteProcedure2DataSet(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteProcedure2DataSet(conn, query, parameters);
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(this string query, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataSetAsync(conn_str, query, parameters);
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataSetAsync(dbtype, conn_str, query, parameters);
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataSetAsync(dbConnectionFactory, query, parameters);
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataSetAsync(conn, query, parameters);
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteProcedure2DataSetAsync(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteFunction(this string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteFunction(conn_str, query, parameters);
    }

    public static List<SqlParam> ExecuteFunction(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteFunction(dbtype, conn_str, query, parameters);
    }

    public static List<SqlParam> ExecuteFunction(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteFunction(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteFunction(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteFunction(conn, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(this string query, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteFunctionAsync(conn_str, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteFunctionAsync(dbtype, conn_str, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteFunctionAsync(conn, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteFunctionAsync(conn, query, parameters);
    }

    public static object? ExecuteScalar(this string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteScalar(conn_str, query, parameters);
    }

    public static object? ExecuteScalar(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteScalar(dbtype, conn_str, query, parameters);
    }

    public static object? ExecuteScalar(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteScalar(dbConnectionFactory, query, parameters);
    }

    public static object? ExecuteScalar(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteScalar(conn, query, parameters);
    }

    public static object? ExecuteScalar(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteScalar(conn, query, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(this string query, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteScalarAsync(conn_str, query, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteScalarAsync(dbtype, conn_str, query, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteScalarAsync(dbConnectionFactory, query, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteScalarAsync(conn, query, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteScalarAsync(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteNonQuery(this string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteNonQuery(conn_str, query, parameters);
    }

    public static List<SqlParam> ExecuteNonQuery(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteNonQuery(dbtype, conn_str, query, parameters);
    }

    public static List<SqlParam> ExecuteNonQuery(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteNonQuery(dbConnectionFactory, query, parameters);
    }

    public static List<SqlParam> ExecuteNonQuery(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteNonQuery(conn, query, parameters);
    }

    public static List<SqlParam> ExecuteNonQuery(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return DBUtils.ExecuteNonQuery(conn, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(this string query, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteNonQueryAsync(conn_str, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteNonQueryAsync(dbtype, conn_str, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteNonQueryAsync(dbConnectionFactory, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteNonQueryAsync(conn, query, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return await DBUtils.ExecuteNonQueryAsync(conn, query, parameters);
    }

    public static T? QueryRow<T>(this string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.QueryRow<T>(conn_str, query, parameters);
    }

    public static T? QueryRow<T>(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.QueryRow<T>(dbtype, conn_str, query, parameters);
    }

    public static T? QueryRow<T>(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.QueryRow<T>(dbConnectionFactory, query, parameters);
    }

    public static T? QueryRow<T>(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.QueryRow<T>(conn, query, parameters);
    }

    public static T? QueryRow<T>(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return DBUtils.QueryRow<T>(conn, query, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(this string query, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.QueryRowAsync<T>(conn_str, query, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.QueryRowAsync<T>(dbtype, conn_str, query, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryRowAsync<T>(dbConnectionFactory, query, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryRowAsync<T>(conn, query, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return await DBUtils.QueryRowAsync<T>(conn, query, parameters);
    }

    public static List<T>? Query<T>(this string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.Query<T>(conn_str, query, parameters);
    }

    public static List<T> Query<T>(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.Query<T>(dbtype, conn_str, query, parameters);
    }

    public static List<T>? Query<T>(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return DBUtils.Query<T>(dbConnectionFactory, query, parameters);
    }

    public static List<T>? Query<T>(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.Query<T>(conn, query, parameters);
    }

    public static List<T>? Query<T>(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return DBUtils.Query<T>(conn, query, parameters);
    }

    public static async Task<List<T>?> QueryAsync<T>(this string query, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.QueryAsync<T>(conn_str, query, parameters);
    }

    public static async Task<List<T>?> QueryAsync<T>(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.QueryAsync<T>(dbtype, conn_str, query, parameters);
    }

    public static async Task<List<T>?> QueryAsync<T>(this string query, DbConnectionFactory dbConnectionFactory, params SqlParam[] parameters)
    {
        return await DBUtils.QueryAsync<T>(dbConnectionFactory, query, parameters);
    }

    public static async Task<List<T>?> QueryAsync<T>(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryAsync<T>(conn, query, parameters);
    }

    public static async Task<List<T>?> QueryAsync<T>(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return await DBUtils.QueryAsync<T>(conn, query, parameters);
    }

    public static DataTable? QueryDataTable(this string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.QueryDataTable(conn_str, query, parameters);
    }

    public static DataTable? QueryDataTable(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.QueryDataTable(dbtype, conn_str, query, parameters);
    }

    public static DataTable? QueryDataTable(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.QueryDataTable(conn, query, parameters);
    }

    public static DataTable? QueryDataTable(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return DBUtils.QueryDataTable(conn, query, parameters);
    }

    public static async Task<DataTable?> QueryDataTableAsync(this string query, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.QueryDataTableAsync(conn_str, query, parameters);
    }

    public static async Task<DataTable?> QueryDataTableAsync(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.QueryDataTableAsync(dbtype, conn_str, query, parameters);
    }

    public static async Task<DataTable?> QueryDataTableAsync(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryDataTableAsync(conn, query, parameters);
    }

    public static async Task<DataTable?> QueryDataTableAsync(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return await DBUtils.QueryDataTableAsync(conn, query, parameters);
    }

    public static DataSet? QueryDataSet(this string query, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.QueryDataSet(conn_str, query, parameters);
    }

    public static DataSet? QueryDataSet(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return DBUtils.QueryDataSet(dbtype, conn_str, query, parameters);
    }

    public static DataSet? QueryDataSet(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return DBUtils.QueryDataSet(conn, query, parameters);
    }

    public static DataSet? QueryDataSet(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return DBUtils.QueryDataSet(conn, query, parameters);
    }

    public static async Task<DataSet?> QueryDataSetAsync(this string query, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.QueryDataSetAsync(conn_str, query, parameters);
    }

    public static async Task<DataSet?> QueryDataSetAsync(this string query, DbConnectionType dbtype, string conn_str, params SqlParam[] parameters)
    {
        return await DBUtils.QueryDataSetAsync(dbtype, conn_str, query, parameters);
    }

    public static async Task<DataSet?> QueryDataSetAsync(this string query, DbConnection conn, params SqlParam[] parameters)
    {
        return await DBUtils.QueryDataSetAsync(conn, query, parameters);
    }

    public static async Task<DataSet?> QueryDataSetAsync(this DbConnection conn, string query, params SqlParam[] parameters)
    {
        return await DBUtils.QueryDataSetAsync(conn, query, parameters);
    }

    public static void UpdateTable<T>(this string tableName, string conn_str, T model)
    {
        DBUtils.UpdateTable(conn_str, tableName, model);
    }

    public static void UpdateTable<T>(this string tableName, DbConnectionType dbtype, string conn_str, T model)
    {
        DBUtils.UpdateTable(dbtype, conn_str, tableName, model);
    }

    public static void UpdateTable<T>(this string tableName, DbConnectionFactory dbConnectionFactory, T model)
    {
        DBUtils.UpdateTable(dbConnectionFactory, tableName, model);
    }

    public static void UpdateTable<T>(this string tableName, DbConnection conn, T model)
    {
        DBUtils.UpdateTable(conn, tableName, model);
    }

    public static void UpdateTable<T>(this DbConnection conn, string tableName, T model)
    {
        DBUtils.UpdateTable(conn, tableName, model);
    }

    public static void UpdateTable<T>(this string tableName, string conn_str, List<T> models)
    {
        DBUtils.UpdateTable(conn_str, tableName, models);
    }

    public static void UpdateTable<T>(this string tableName, DbConnectionType dbtype, string conn_str, List<T> models)
    {
        DBUtils.UpdateTable(dbtype, conn_str, tableName, models);
    }

    public static void UpdateTable<T>(this string tableName, DbConnectionFactory dbConnectionFactory, List<T> models)
    {
        DBUtils.UpdateTable(dbConnectionFactory, tableName, models);
    }

    public static void UpdateTable<T>(this string tableName, DbConnection conn, List<T> models)
    {
        DBUtils.UpdateTable(conn, tableName, models);
    }

    public static void UpdateTable<T>(this DbConnection conn, string tableName, List<T> models)
    {
        DBUtils.UpdateTable(conn, tableName, models);
    }

    public static T? QueryRow<T>(this DbCommand cmd)
    {
        bool found = false;

        using (var dRead = cmd.ExecuteReader())
        {
            while (dRead.Read())
            {
                found = true;
                var rez = dRead.Row2Model<T>();
                return rez;
            }
        }

        if (!found)
            throw new Exception("No data found");

        return default;
    }

    public static List<T> Query<T>(this DbCommand cmd)
    {
        List<T> rez = new List<T>();

        using (var dRead = cmd.ExecuteReader())
        {
            while (dRead.Read())
            {
                rez.Add(dRead.Row2Model<T>());
            }
        }

        return rez;
    }

    public static async Task<T?> QueryRowAsync<T>(this DbCommand cmd)
    {
        bool found = false;

        using (var dRead = await cmd.ExecuteReaderAsync())
        {
            while (await dRead.ReadAsync())
            {
                found = true;
                var rez = dRead.Row2Model<T>();
                return rez;
            }
        }

        if (!found)
            throw new Exception("No data found");

        return default;
    }

    public static async Task<List<T>> QueryAsync<T>(this DbCommand cmd)
    {
        List<T> rez = new List<T>();

        using (var dRead = await cmd.ExecuteReaderAsync())
        {
            while (await dRead.ReadAsync())
            {
                rez.Add(dRead.Row2Model<T>());
            }
        }

        return rez;
    }

    private static T Row2Model<T>(this DbDataReader dRead)
    {
        var classType = typeof(T);
        T? rez = (T?)Activator.CreateInstance(classType);

        if (rez == null)
            throw new NullReferenceException(nameof(rez));

        for (int i = 0; i < dRead.FieldCount; i++)
        {
            var lcol = dRead.GetName(i).ToLower();

            var p = classType.GetProperty(lcol);

            if (p == null)
                continue;

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

        return rez;
    }
}
