using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Models;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Extensions;

namespace Zen.DbAccess.Utils;

public static class DBUtils
{
    public static DateTime GetServerDateTime(string conn_str)
    {
        return GetServerDateTime(DbConnectionFactory.DefaultDbType, conn_str).Result;
    }

    public static async Task<DateTime> GetServerDateTime(DbConnectionType dbtype, string conn_str)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync();
        return GetServerDateTime(dbtype, conn);
    }

    public static async Task<DateTime> GetServerDateTime(DbConnectionFactory dbConnectionFactory)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        return GetServerDateTime(dbConnectionFactory.DbType, conn);
    }

    public static DateTime GetServerDateTime(DbConnectionType dbtype, DbConnection conn)
    {
        return GetServerDateTime(dbtype, conn, tx: null);
    }

    public static DateTime GetServerDateTime(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx)
    {
        string sql;

        if (dbtype == DbConnectionType.SqlServer)
            sql = "SELECT GETDATE()";
        else if (dbtype == DbConnectionType.Oracle)
            sql = "SELECT sysdate from dual";
        else if (dbtype == DbConnectionType.Postgresql)
            sql = "SELECT now()";
        else if (dbtype == DbConnectionType.Sqlite)
            sql = "SELECT current_timestamp";
        else
            throw new NotImplementedException($"Unknown connection type");

        DateTime dt = (DateTime)ExecuteScalar(dbtype, conn, tx, sql)!;
        return dt;
    }

    public static List<SqlParam> ExecuteProcedure(string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedureAsync(conn_str, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteProcedure(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedureAsync(dbtype, conn_str, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteProcedure(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedureAsync(dbtype, conn, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteProcedure(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedureAsync(dbtype, conn, tx, sql, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteProcedureAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        return await ExecuteProcedureAsync(dbConnectionFactory.DbType, conn, sql, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync();
        return await ExecuteProcedureAsync(dbtype, conn, sql, parameters);
    }

    private static void SetupFunctionCall(DbConnectionType dbtype, DbCommand cmd, string sql, params SqlParam[] parameters)
    {
        if (dbtype == DbConnectionType.Postgresql)
            cmd.CommandType = CommandType.StoredProcedure;

        StringBuilder sbSql = new StringBuilder();
        sbSql.Append($"select {sql}(");

        bool firstParam = true;
        foreach (SqlParam prm in parameters.Where(x => x.paramDirection != ParameterDirection.ReturnValue).ToArray())
        {
            if (prm.paramDirection == ParameterDirection.ReturnValue)
                continue; // do not add in the call

            if (firstParam)
                firstParam = false;
            else
                sbSql.Append(", ");

            if (prm.name.StartsWith("@"))
                sbSql.Append($"{prm.name}");
            else
                sbSql.Append($"@{prm.name}");
        }

        sbSql.Append($") ");

        string? returnValueParameterName = parameters.FirstOrDefault(x => x.paramDirection == ParameterDirection.ReturnValue)?.name;

        if (!string.IsNullOrEmpty(returnValueParameterName))
            sbSql.Append($" AS {returnValueParameterName} ");

        if (dbtype == DbConnectionType.Oracle)
            sbSql.Append(" from dual");

        cmd.CommandText = sbSql.ToString();
    }

    private static void SetupProcedureCall(DbConnectionType dbtype, DbCommand cmd, string sql, bool isDataSetReturn, bool isTableReturn, params SqlParam[] parameters)
    {
        if (dbtype == DbConnectionType.Postgresql)
        {
            // commented on purpose.
            // Npgsql supports this mainly for portability,
            // but this style of calling has no advantage over the regular command shown above.
            // When CommandType.StoredProcedure is set,
            // Npgsql will simply generate the appropriate SELECT my_func() for you, nothing more.
            // Unless you have specific portability requirements,
            // it is recommended you simply avoid CommandType.StoredProcedure and construct the SQL yourself.
            //cmd.CommandType = CommandType.StoredProcedure;

            int countDots = sql.Split('.').Length - 1; // if countDots > 1 then we have also the schema in the name of the procedure being called

            StringBuilder sbSql = new StringBuilder();

            if (isDataSetReturn)
            {
                // we expect the p$ to be a function returning a refcursor
                sbSql.Append($"SELECT ");
            }
            else if (isTableReturn)
            {
                // we expect the p$ to be a function returning a table
                sbSql.Append($"SELECT * FROM ");
            }
            else
            {
                sbSql.Append($"CALL ");
            }

            sbSql.Append($"{(countDots > 1 ? sql.Substring(sql.IndexOf(".") + 1) : sql)}(");

            bool firstParam = true;
            foreach (SqlParam prm in parameters)
            {
                if (firstParam)
                    firstParam = false;
                else
                    sbSql.Append(", ");

                if (prm.name.StartsWith("@"))
                    sbSql.Append($"{prm.name}");
                else
                    sbSql.Append($"@{prm.name}");
            }

            sbSql.Append(")");

            cmd.CommandText = sbSql.ToString();
        }
        else
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = sql;
        }
    }

    public static Task<List<SqlParam>> ExecuteProcedureAsync(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedureAsync(dbtype, conn, tx: null, sql, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        List<SqlParam> outParameters = new List<SqlParam>();

        using (DbCommand cmd = conn.CreateCommand())
        {
            if (tx != null && cmd.Transaction == null)
                cmd.Transaction = tx;

            SetupProcedureCall(dbtype, cmd, sql, isDataSetReturn: false, isTableReturn: true, parameters);

            AddParameters(dbtype,cmd, parameters);

            await cmd.ExecuteNonQueryAsync();

            foreach (DbParameter param in cmd.Parameters)
            {
                if (param.Direction != ParameterDirection.InputOutput && param.Direction != ParameterDirection.Output)
                    continue;

                outParameters.Add(new SqlParam(param.ParameterName, param.Value) { paramDirection = param.Direction });
            }

            DisposeLobParameters(dbtype,cmd, parameters);
        }

        return outParameters;
    }

    public static DataTable? ExecuteProcedure2DataTable(string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataTableAsync(conn_str, sql, parameters).Result;
    }

    public static DataTable? ExecuteProcedure2DataTable(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataTableAsync(dbtype, conn_str, sql, parameters).Result;
    }

    public static DataTable? ExecuteProcedure2DataTable(DbConnectionType dbtype,  DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataTableAsync(dbtype,conn, sql, parameters).Result;
    }

    public static DataTable? ExecuteProcedure2DataTable(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataTableAsync(dbtype, conn, tx, sql, parameters).Result;
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteProcedure2DataTableAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters);
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync();
        using DbTransaction tx = await conn.BeginTransactionAsync();
        var result = await ExecuteProcedure2DataTableAsync(dbtype, conn, sql, parameters);
        await tx.CommitAsync();

        return result;
    }

    public static Task<DataTable?> ExecuteProcedure2DataTableAsync(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataTableAsync(dbtype, conn, tx: null, sql, parameters);
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        DataTable? dt = null;

        await Task.Run(() =>
        {
            using DbCommand cmd = conn.CreateCommand();

            if (tx != null && cmd.Transaction == null)
                cmd.Transaction = tx;

            SetupProcedureCall(dbtype,cmd, sql, isDataSetReturn: false, isTableReturn: true, parameters);

            AddParameters(dbtype,cmd, parameters);

            using DbDataAdapter da = CreateDataAdapter(dbtype,conn)!;
            da.SelectCommand = cmd;

            dt = new DataTable();
            da.Fill(dt);

            DisposeLobParameters(dbtype, da.SelectCommand, parameters);
        });

        return dt;
    }

    public static DataTable? ExecuteProcedure2DataTable(DbConnectionType dbtype, DbCommand cmd)
    {
        return ExecuteProcedure2DataTableAsync(dbtype, cmd).Result;
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(DbConnectionType dbtype, DbCommand cmd)
    {
        DataSet? ds = await ExecuteProcedure2DataSetAsync(dbtype, cmd);

        if (ds == null)
            return null;

        return ds.Tables[0];
    }

    public static DataSet? ExecuteProcedure2DataSet(string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataSetAsync(conn_str, sql, parameters).Result;
    }

    public static DataSet? ExecuteProcedure2DataSet(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataSetAsync(dbtype, conn_str, sql, parameters).Result;
    }

    public static DataSet? ExecuteProcedure2DataSet(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataSetAsync(dbtype, conn, sql, parameters).Result;
    }

    public static DataSet? ExecuteProcedure2DataSet(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataSetAsync(dbtype, conn, tx, sql, parameters).Result;
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteProcedure2DataSetAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters);
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync();
        using DbTransaction tx = await conn.BeginTransactionAsync();
        var result = await ExecuteProcedure2DataSetAsync(dbtype, conn, sql, parameters);
        await tx.CommitAsync();

        return result;
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        using DbTransaction tx = await conn.BeginTransactionAsync();
        var result = await ExecuteProcedure2DataSetAsync(dbConnectionFactory.DbType, conn, sql, parameters);
        await tx.CommitAsync();

        return result;
    }

    public static Task<DataSet?> ExecuteProcedure2DataSetAsync(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataSetAsync(dbtype, conn, tx: null, sql, parameters);
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        DataSet? ds = null;

        await Task.Run(async () =>
        {
            using DbCommand cmd = conn.CreateCommand();

            if (tx != null && cmd.Transaction == null)
                cmd.Transaction = tx;

            SetupProcedureCall(dbtype, cmd, sql, isDataSetReturn: true, isTableReturn: false, parameters);

            AddParameters(dbtype, cmd, parameters);

            using DbDataAdapter da = CreateDataAdapter(dbtype, conn)!;
            da.SelectCommand = cmd;

            ds = new DataSet();
            da.Fill(ds);

            if (dbtype == DbConnectionType.Postgresql && ds.Tables.Count == 1 && ds.Tables[0].Columns.Count == 1)
            {
                string procedureName = sql.IndexOf(".") > 0 ? sql.Substring(sql.IndexOf(".") + 1) : sql;
                
                if (ds.Tables[0].Columns[0].ToString().ToLower() == procedureName.ToLower())
                {
                    string[] openCursors = ds.Tables[0].AsEnumerable()
                        .Select(x => x[0]!.ToString()!)
                        .Where(x => x.StartsWith("<unnamed") && x.EndsWith(">") && !x.Contains(";"))
                        .ToArray();

                    if (openCursors.Any())
                    {
                        ds = new DataSet();
                        int k = 1;

                        foreach (string openCursor in openCursors)
                        {
                            DataTable dt = await ExecutePostgresCursorToTableAsync(dbtype, conn, openCursor);
                            dt.TableName = $"TABLE{k++}";
                            ds.Tables.Add(dt);
                        }
                    }
                }
            }

            DisposeLobParameters(dbtype, da.SelectCommand, parameters);
        });

        return ds;
    }

    private static Task<DataTable> ExecutePostgresCursorToTableAsync(DbConnectionType dbtype, DbConnection conn, string cursorName)
    {
        return ExecutePostgresCursorToTableAsync(dbtype, conn, tx: null, cursorName);
    }

    private static Task<DataTable> ExecutePostgresCursorToTableAsync(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string cursorName)
    {
        string sql = $"FETCH ALL IN \"{cursorName}\"";

        using DbCommand cmd = conn.CreateCommand();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        cmd.CommandText = sql;

        using DbDataAdapter da = CreateDataAdapter(dbtype, conn)!;
        da.SelectCommand = cmd;

        DataTable dt = new DataTable();
        da.Fill(dt);

        return Task.FromResult(dt);
    }

    public static DataSet? ExecuteProcedure2DataSet(DbConnectionType dbtype, DbCommand cmd)
    {
        return ExecuteProcedure2DataSetAsync(dbtype, cmd).Result;
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(DbConnectionType dbtype, DbCommand cmd)
    {
        DataSet? ds = null;

        await Task.Run(() =>
        {
            using DbDataAdapter da = CreateDataAdapter(dbtype, cmd.Connection)!;
            da.SelectCommand = cmd;

            ds = new DataSet();
            da.Fill(ds);
        });

        return ds;
    }

    public static List<SqlParam> ExecuteFunction(string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteFunctionAsync(conn_str, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteFunction(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteFunctionAsync(dbtype, conn_str, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteFunction(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteFunctionAsync(dbtype, conn, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteFunction(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        return ExecuteFunctionAsync(dbtype, conn, tx, sql, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteFunctionAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync();
        return await ExecuteFunctionAsync(dbtype, conn, sql, parameters);
    }

    public static Task<List<SqlParam>> ExecuteFunctionAsync(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteFunctionAsync(dbtype, conn, tx: null, sql, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        List<SqlParam> outParameters = new List<SqlParam>();

        using DbCommand cmd = conn.CreateCommand();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        SetupFunctionCall(dbtype, cmd, sql, parameters);

        AddParameters(dbtype, cmd, parameters.Where(x => x.paramDirection != ParameterDirection.ReturnValue).ToArray());

        using DataTable dt = await ExecuteProcedure2DataTableAsync(dbtype, cmd) ?? new DataTable();

        foreach (DbParameter param in cmd.Parameters)
        {
            if (param.Direction != ParameterDirection.InputOutput
                && param.Direction != ParameterDirection.Output
                && param.Direction != ParameterDirection.ReturnValue)
                continue;

            if (dt.Rows.Count > 0 && dt.Columns.Contains(param.ParameterName))
                outParameters.Add(new SqlParam(param.ParameterName, dt.Rows[0][param.ParameterName]) { paramDirection = param.Direction });
        }

        DisposeLobParameters(dbtype, cmd, parameters);

        return outParameters;
    }

    public static object? ExecuteScalar(string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteScalarAsync(conn_str, sql, parameters).Result;
    }

    public static object? ExecuteScalar(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteScalarAsync(dbtype, conn_str, sql, parameters).Result;
    }

    public static object? ExecuteScalar(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        return ExecuteScalarAsync(dbConnectionFactory, sql, parameters).Result;
    }

    public static object? ExecuteScalar(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteScalarAsync(dbtype, conn, sql, parameters).Result;
    }

    public static object? ExecuteScalar(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        return ExecuteScalarAsync(dbtype, conn, tx, sql, parameters).Result;
    }

    public static async Task<object?> ExecuteScalarAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteScalarAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteScalarAsync(new DbConnectionFactory(dbtype, conn_str), sql, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        return await ExecuteScalarAsync(dbConnectionFactory.DbType, conn, sql, parameters);
    }

    public static Task<object?> ExecuteScalarAsync(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteScalarAsync(dbtype,conn, tx: null, sql, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        using DbCommand cmd = conn.CreateCommand();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        cmd.CommandText = sql;

        AddParameters(dbtype, cmd, parameters);

        var result = await cmd.ExecuteScalarAsync();

        DisposeLobParameters(dbtype, cmd, parameters);

        return result;
    }

    public static List<SqlParam> ExecuteNonQuery(string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteNonQueryAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteNonQuery(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return ExecuteNonQueryAsync(dbtype, conn_str, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteNonQuery(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        return ExecuteNonQueryAsync(dbConnectionFactory, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteNonQuery(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteNonQueryAsync(dbtype, conn, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteNonQuery(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        return ExecuteNonQueryAsync(dbtype, conn, tx, sql, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteNonQueryAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteNonQueryAsync(new DbConnectionFactory(dbtype, conn_str), sql, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        return await ExecuteNonQueryAsync(dbConnectionFactory.DbType, conn, sql, parameters);
    }

    public static Task<List<SqlParam>> ExecuteNonQueryAsync(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteNonQueryAsync(dbtype, conn, tx: null, sql, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        List<SqlParam> outParameters = new List<SqlParam>();

        using DbCommand cmd = conn.CreateCommand();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        cmd.CommandText = sql;

        AddParameters(dbtype, cmd, parameters);

        await cmd.ExecuteNonQueryAsync();

        foreach (DbParameter param in cmd.Parameters)
        {
            if (param.Direction != ParameterDirection.InputOutput
                && param.Direction != ParameterDirection.Output
                && param.Direction != ParameterDirection.ReturnValue)
                continue;

            outParameters.Add(new SqlParam(param.ParameterName, param.Value) { paramDirection = param.Direction });
        }

        DisposeLobParameters(dbtype, cmd, parameters);

        return outParameters;
    }

    public static T? QueryRow<T>(string conn_str, string sql, params SqlParam[] parameters)
    {
        return QueryRowAsync<T>(conn_str, sql, parameters).Result;
    }

    public static T? QueryRow<T>(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return QueryRowAsync<T>(dbtype, conn_str, sql, parameters).Result;
    }

    public static T? QueryRow<T>(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        return QueryRowAsync<T>(dbConnectionFactory, sql, parameters).Result;
    }

    public static T? QueryRow<T>(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryRowAsync<T>(dbtype, conn, sql, parameters).Result;
    }

    public static T? QueryRow<T>(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        return QueryRowAsync<T>(dbtype, conn, tx, sql, parameters).Result;
    }

    public static async Task<T?> QueryRowAsync<T>(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryRowAsync<T>(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryRowAsync<T>(new DbConnectionFactory(dbtype, conn_str), sql, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        return await QueryRowAsync<T>(dbConnectionFactory.DbType, conn, sql, parameters);
    }

    public static Task<T?> QueryRowAsync<T>(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryRowAsync<T>(dbtype, conn, tx: null, sql, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        List<T> results = await QueryAsync<T>(dbtype, conn, tx, sql, parameters);

        if (results.Count == 0)
            throw new Exception("no data found");

        return results.FirstOrDefault();
    }

    public static List<T> Query<T>(string conn_str, string sql, params SqlParam[] parameters)
    {
        return QueryAsync<T>(conn_str, sql, parameters).Result;
    }

    public static List<T> Query<T>(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return QueryAsync<T>(dbtype, conn_str, sql, parameters).Result;
    }

    public static List<T> Query<T>(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        return QueryAsync<T>(dbConnectionFactory, sql, parameters).Result;
    }

    public static List<T> Query<T>(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryAsync<T>(dbtype, conn, sql, parameters).Result;
    }

    public static List<T> Query<T>(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        return QueryAsync<T>(dbtype, conn, tx, sql, parameters).Result;
    }

    public static async Task<List<T>> QueryAsync<T>(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryAsync<T>(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters);
    }

    public static async Task<List<T>> QueryAsync<T>(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryAsync<T>(new DbConnectionFactory(dbtype, conn_str), sql, parameters);
    }

    public static async Task<List<T>> QueryAsync<T>(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        return await QueryAsync<T>(dbConnectionFactory.DbType, conn, sql, parameters);
    }

    public static Task<List<T>> QueryAsync<T>(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryAsync<T>(dbtype, conn, tx: null, sql, parameters);
    }

    public static async Task<List<T>> QueryAsync<T>(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        using DbCommand cmd = conn.CreateCommand();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        cmd.CommandText = sql;

        AddParameters(dbtype, cmd, parameters);

        var result = await cmd.QueryAsync<T>();

        DisposeLobParameters(dbtype, cmd, parameters);
        return result;
    }

    public static DataTable? QueryDataTable(string conn_str, string sql, params SqlParam[] parameters)
    {
        return QueryDataTableAsync(conn_str, sql, parameters).Result;
    }

    public static DataTable? QueryDataTable(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return QueryDataTableAsync(dbtype, conn_str, sql, parameters).Result;
    }

    public static DataTable? QueryDataTable(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryDataTableAsync(dbtype, conn, sql, parameters).Result;
    }

    public static DataTable? QueryDataTable(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        return QueryDataTableAsync(dbtype, conn, tx, sql, parameters).Result;
    }

    public static async Task<DataTable?> QueryDataTableAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryDataTableAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters);
    }

    public static async Task<DataTable?> QueryDataTableAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync();
        return await QueryDataTableAsync(dbtype, conn, sql, parameters);
    }

    public static Task<DataTable?> QueryDataTableAsync(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryDataTableAsync(dbtype, conn, tx: null, sql, parameters);
    }

    public static async Task<DataTable?> QueryDataTableAsync(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        DataTable? dt = null;

        await Task.Run(() =>
        {
            using DbCommand cmd = conn.CreateCommand();

            if (tx != null && cmd.Transaction == null)
                cmd.Transaction = tx;

            cmd.CommandText = sql;

            AddParameters(dbtype, cmd, parameters);

            using DbDataAdapter da = CreateDataAdapter(dbtype, conn)!;
            da.SelectCommand = cmd;

            dt = new DataTable();
            da.Fill(dt);
            DisposeLobParameters(dbtype, da.SelectCommand, parameters);
        });

        return dt;
    }

    public static DataSet? QueryDataSet(string conn_str, string sql, params SqlParam[] parameters)
    {
        return QueryDataSetAsync(conn_str, sql, parameters).Result;
    }

    public static DataSet? QueryDataSet(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return QueryDataSetAsync(dbtype, conn_str, sql, parameters).Result;
    }

    public static DataSet? QueryDataSet(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryDataSetAsync(dbtype, conn, sql, parameters).Result;
    }

    public static DataSet? QueryDataSet(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        return QueryDataSetAsync(dbtype, conn, tx, sql, parameters).Result;
    }

    public static async Task<DataSet?> QueryDataSetAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryDataSetAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters);
    }

    public static async Task<DataSet?> QueryDataSetAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync();
        return await QueryDataSetAsync(dbtype, conn, sql, parameters);
    }

    public static Task<DataSet?> QueryDataSetAsync(DbConnectionType dbtype, DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryDataSetAsync(dbtype, conn, tx: null, sql, parameters);
    }

    public static async Task<DataSet?> QueryDataSetAsync(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string sql, params SqlParam[] parameters)
    {
        DataSet? ds = null;

        await Task.Run(() =>
        {
            using DbCommand cmd = conn.CreateCommand();

            if (tx != null && cmd.Transaction == null)
                cmd.Transaction = tx;

            cmd.CommandText = sql;

            AddParameters(dbtype, cmd, parameters);

            using DbDataAdapter da = CreateDataAdapter(dbtype, conn)!;
            da.SelectCommand = cmd;

            ds = new DataSet();
            da.Fill(ds);
            DisposeLobParameters(dbtype, da.SelectCommand, parameters);
        });

        return ds;
    }

    public static void UpdateTable<T>(string conn_str, string table, T model)
    {
        UpdateTable(DbConnectionFactory.DefaultDbType, conn_str, table, model);
    }

    public static void UpdateTable<T>(DbConnectionType dbtype, string conn_str, string table, T model)
    {
        DbConnectionFactory dbConnectionFactory = new DbConnectionFactory(dbtype, conn_str);
        UpdateTable(dbConnectionFactory, table, model);
    }

    public static void UpdateTable<T>(DbConnectionFactory dbConnectionFactory, string table, T model)
    {
        using DbConnection conn = dbConnectionFactory.BuildAndOpenAsync().Result;
        UpdateTable(dbConnectionFactory.DbType, conn, table, model);
        conn.Close();
    }

    private static void UpdateTableFromDataTable(DbConnectionType dbtype, DbConnection conn, string table, DataTable modelTable)
    {
        UpdateTableFromDataTable(dbtype, conn, tx: null, table, modelTable);
    }

    private static void UpdateTableFromDataTable(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string table, DataTable modelTable)
    {
        string sql = $"select * from {table} where 1 = 2";

        using DataTable dt = new DataTable();
        using DbCommand cmd = conn.CreateCommand();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        cmd.CommandText = sql;

        using DbDataAdapter dataAdapter = CreateDataAdapter(dbtype, conn)!;
        dataAdapter.SelectCommand = cmd;
        dataAdapter.UpdateBatchSize = 128;

        using DbCommandBuilder commandBuilder = CreateCommandBuilder(conn, dataAdapter);
        dataAdapter.InsertCommand = commandBuilder.GetInsertCommand();
        dataAdapter.UpdateCommand = commandBuilder.GetUpdateCommand();

        if (tx != null && dataAdapter.InsertCommand.Transaction == null)
            dataAdapter.InsertCommand.Transaction = tx;

        if (tx != null && dataAdapter.UpdateCommand.Transaction == null)
            dataAdapter.UpdateCommand.Transaction = tx;

        dataAdapter.Fill(dt);

        foreach (DataRow row in modelTable.Rows)
        {
            DataRow newRow = dt.NewRow();

            foreach (DataColumn col in modelTable.Columns)
            {
                if (!dt.Columns.Contains(col.ColumnName))
                    continue;

                if (row[col.ColumnName] == DBNull.Value)
                {
                    newRow[col.ColumnName] = row[col.ColumnName];
                }
                else if (dt.Columns[col.ColumnName]?.DataType == typeof(decimal)
                    || dt.Columns[col.ColumnName]?.DataType == typeof(decimal?))
                {
                    string? val = row[col.ColumnName]?.ToString();

                    if (val != null && (val.Contains("E") || val.Contains("e")))
                        newRow[col.ColumnName] = decimal.Parse(val, NumberStyles.Float);
                    else
                        newRow[col.ColumnName] = Convert.ToDecimal(row[col.ColumnName]);
                }
                else
                {
                    newRow[col.ColumnName] = row[col.ColumnName];
                }
            }

            dt.Rows.Add(newRow);
        }

        dataAdapter.Update(dt);
    }

    private static void UpdateTableFromGenericModel<T>(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string table, T model)
    {
        string sql = $"select * from {table} where 1 = 2";

        using DataTable dt = new DataTable();
        using DbCommand cmd = conn.CreateCommand();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        cmd.CommandText = sql;

        using DbDataAdapter dataAdapter = CreateDataAdapter(dbtype, conn)!;
        dataAdapter.SelectCommand = cmd;
        dataAdapter.UpdateBatchSize = 128;

        using DbCommandBuilder commandBuilder = CreateCommandBuilder(conn, dataAdapter);
        dataAdapter.InsertCommand = commandBuilder.GetInsertCommand();
        dataAdapter.UpdateCommand = commandBuilder.GetUpdateCommand();

        if (tx != null && dataAdapter.InsertCommand.Transaction == null)
            dataAdapter.InsertCommand.Transaction = tx;

        if (tx != null && dataAdapter.UpdateCommand.Transaction == null)
            dataAdapter.UpdateCommand.Transaction = tx;

        dataAdapter.Fill(dt);

        dt.CreateRowFromModel(model);

        dataAdapter.Update(dt);
    }

    public static void UpdateTable<T>(DbConnectionType dbtype, DbConnection conn, string table, T model)
    {
        UpdateTable<T>(dbtype, conn, tx: null, table, model);
    }

    public static void UpdateTable<T>(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string table, T model)
    {
        if (model == null)
            throw new ArgumentNullException(nameof(model));

        string sql = $"select * from {table} where 1 = 2";

        if (typeof(T) == typeof(DataTable))
            UpdateTableFromDataTable(dbtype, conn, tx, table, (model as DataTable)!);
        else
            UpdateTableFromGenericModel(dbtype, conn, tx, table, model);
    }

    public static void UpdateTable<T>(string conn_str, string table, List<T> models)
    {
        UpdateTable(DbConnectionFactory.DefaultDbType, conn_str, table, models);
    }

    public static void UpdateTable<T>(DbConnectionType dbtype, string conn_str, string table, List<T> models)
    {
        DbConnectionFactory dbConnectionFactory = new DbConnectionFactory(dbtype, conn_str);
        UpdateTable(dbConnectionFactory, table, models);
    }

    public static void UpdateTable<T>(DbConnectionFactory dbConnectionFactory, string table, List<T> models)
    {
        using DbConnection conn = dbConnectionFactory.BuildAndOpenAsync().Result;
        UpdateTable(dbConnectionFactory.DbType, conn, table, models);
        conn.Close();
    }

    public static void UpdateTable<T>(DbConnectionType dbtype, DbConnection conn, string table, List<T> models)
    {
        UpdateTable<T>(dbtype, conn, tx: null, table, models);
    }

    public static void UpdateTable<T>(DbConnectionType dbtype, DbConnection conn, DbTransaction? tx, string table, List<T> models)
    {
        string sql = $"select * from {table} where 1 = 2";

        using DataTable dt = new DataTable();
        using DbCommand cmd = conn.CreateCommand();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        cmd.CommandText = sql;

        using DbDataAdapter dataAdapter = CreateDataAdapter(dbtype, conn)!;
        dataAdapter.SelectCommand = cmd;
        dataAdapter.UpdateBatchSize = 128;

        using DbCommandBuilder commandBuilder = CreateCommandBuilder(conn, dataAdapter);
        dataAdapter.InsertCommand = commandBuilder.GetInsertCommand();
        dataAdapter.UpdateCommand = commandBuilder.GetUpdateCommand();

        if (tx != null && dataAdapter.InsertCommand.Transaction == null)
            dataAdapter.InsertCommand.Transaction = tx;

        if (tx != null && dataAdapter.UpdateCommand.Transaction == null)
            dataAdapter.UpdateCommand.Transaction = tx;

        dataAdapter.Fill(dt);

        foreach (T model in models)
            dt.CreateRowFromModel(model);

        dataAdapter.Update(dt);
    }

    private static DbDataAdapter? CreateDataAdapter(DbConnectionType dbtype, DbConnection? conn)
    {
        if (conn == null)
            throw new ArgumentNullException(nameof(conn));

        DbDataAdapter? da = DbProviderFactories.GetFactory(conn)?.CreateDataAdapter();

        if (dbtype == DbConnectionType.Oracle)
            DbConnectionFactory.DatabaseSpeciffic[dbtype].PrepareDataAdapter(da);

        return da;
    }

    private static DbCommandBuilder CreateCommandBuilder(DbConnection conn, DbDataAdapter dataAdapter)
    {
        DbCommandBuilder? builder = DbProviderFactories.GetFactory(conn)?.CreateCommandBuilder();

        if (builder == null)
            throw new NullReferenceException(nameof(builder));

        builder.DataAdapter = dataAdapter;

        return builder;
    }

    private static void DisposeLobParameters(DbConnectionType dbtype, DbCommand cmd, params SqlParam[] parameters)
    {
        if (parameters == null)
            return;

        foreach (SqlParam prm in parameters)
        {
            if (prm.isClob && dbtype == DbConnectionType.Oracle)
            {
                DbConnectionFactory.DatabaseSpeciffic[dbtype].DisposeClob(cmd, prm);
            }
            else if (prm.isBlob && dbtype == DbConnectionType.Oracle && prm.value != null && prm.value != DBNull.Value)
            {
                DbConnectionFactory.DatabaseSpeciffic[dbtype].DisposeBlob(cmd, prm);
            }
        }
    }

    public static void AddParameters(DbConnectionType dbtype, DbCommand cmd, List<SqlParam> parameters)
    {
        AddParameters(dbtype, cmd, parameters.ToArray());
    }

    public static void AddParameters(DbConnectionType dbtype, DbCommand cmd, params SqlParam[] parameters)
    {
        if (parameters == null)
            return;

        foreach (SqlParam prm in parameters)
        {
            DbParameter param = cmd.CreateParameter();

            if (dbtype == DbConnectionType.Oracle)
            {
                string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;
                param.ParameterName = baseParameterName;
                cmd.CommandText = cmd.CommandText.Replace($"@{baseParameterName}", $":{baseParameterName}");
            }
            else if (dbtype == DbConnectionType.Sqlite)
            {
                string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;
                param.ParameterName = baseParameterName;
                cmd.CommandText = cmd.CommandText.Replace($"@{baseParameterName}", $"${baseParameterName}");
            }
            else
            {
                param.ParameterName = prm.name;
            }

            if (prm.value != null && prm.value != DBNull.Value && prm.value is bool)
            {
                param.Value = Convert.ToBoolean(prm.value) ? 1 : 0;
            }
            else if (prm.isClob && dbtype == DbConnectionType.Oracle && prm.value != null && prm.value != DBNull.Value)
            {
                if (prm.value == null)
                {
                    param.Value = DBNull.Value;
                }
                else
                {
                    param.Value = DbConnectionFactory.DatabaseSpeciffic[dbtype].GetValueAsClob(cmd.Connection, prm.value);
                }
            }
            else if (prm.isBlob && dbtype == DbConnectionType.Oracle && prm.value != null && prm.value != DBNull.Value)
            {
                if (prm.value == null)
                {
                    param.Value = DBNull.Value;
                }
                else
                {
                    param.Value = DbConnectionFactory.DatabaseSpeciffic[dbtype].GetValueAsBlob(cmd.Connection, prm.value);
                }
            }
            else if (prm.value is Enum)
            {
                param.Value = Convert.ToInt32(prm.value);
            }
            else
            {
                param.Value = prm.value ?? DBNull.Value;
            }

            param.Direction = prm.paramDirection;

            if (prm.size > 0)
            {
                param.Size = prm.size;
            }
            else if (prm.paramDirection == ParameterDirection.ReturnValue
                || prm.paramDirection == ParameterDirection.InputOutput
                || prm.paramDirection == ParameterDirection.Output)
            {
                param.Size = 1024;
            }

            cmd.Parameters.Add(param);
        }
    }

    public static (string filterString, SqlParam[] sqlParams) ConstructSqlParamsFromValueList<T>(List<T> valueList)
    {
        StringBuilder sbParams = new StringBuilder();
        SqlParam[] sqlParams = new SqlParam[valueList.Count];

        for (int i = 0; i < valueList.Count; i++)
        {
            if (sbParams.Length > 0)
                sbParams.Append(", ");

            string paramName = $"@p_{i}";

            sbParams.Append(paramName);
            sqlParams[i] = new SqlParam(paramName, valueList[i]);
        }

        return (sbParams.ToString(), sqlParams);
    }
}
