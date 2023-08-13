using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Shared.Models;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Extensions;
using System.Reflection.Metadata;

namespace Zen.DbAccess.Utils;

public static class DBUtils
{
    public static DateTime GetServerDateTime(string conn_str)
    {
        return GetServerDateTime(DbConnectionFactory.DefaultDbType, conn_str).Result;
    }

    public static async Task<DateTime> GetServerDateTime(DbConnectionType dbtype, string conn_str)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync().ConfigureAwait(false);
        return GetServerDateTime(conn);
    }

    public static DateTime GetServerDateTime(DbConnection conn)
    {
        string sql;

        if (conn is SqlConnection)
            sql = "SELECT GETDATE()";
        else if (conn is OracleConnection)
            sql = "SELECT sysdate from dual";
        else if (conn is NpgsqlConnection)
            sql = "SELECT now()";
        else if (conn is SQLiteConnection)
            sql = "SELECT current_timestamp";
        else
            throw new NotImplementedException($"Unknown connection type");

        DateTime dt = (DateTime)ExecuteScalar(conn, sql)!;
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

    public static List<SqlParam> ExecuteProcedure(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedureAsync(conn, sql, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteProcedureAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync().ConfigureAwait(false);
        return await ExecuteProcedureAsync(conn, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync().ConfigureAwait(false);
        return await ExecuteProcedureAsync(conn, sql, parameters).ConfigureAwait(false);
    }

    private static void SetupFunctionCall(DbCommand cmd, string sql, params SqlParam[] parameters)
    {
        if (cmd.Connection is not NpgsqlConnection)
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

        if (cmd.Connection is OracleConnection)
            sbSql.Append(" from dual");

        cmd.CommandText = sbSql.ToString();
    }

    private static void SetupProcedureCall(DbCommand cmd, string sql, bool isDataSetReturn, params SqlParam[] parameters)
    {
        if (cmd.Connection is NpgsqlConnection)
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

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        List<SqlParam> outParameters = new List<SqlParam>();

        using (DbCommand cmd = conn.CreateCommand())
        {
            SetupProcedureCall(cmd, sql, false, parameters);

            AddParameters(cmd, parameters);

            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

            foreach (DbParameter param in cmd.Parameters)
            {
                if (param.Direction != ParameterDirection.InputOutput && param.Direction != ParameterDirection.Output)
                    continue;

                outParameters.Add(new SqlParam(param.ParameterName, param.Value) { paramDirection = param.Direction });
            }

            DisposeLobParameters(cmd, parameters);
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

    public static DataTable? ExecuteProcedure2DataTable(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataTableAsync(conn, sql, parameters).Result;
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteProcedure2DataTableAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync().ConfigureAwait(false);
        using DbTransaction tx = await conn.BeginTransactionAsync().ConfigureAwait(false);
        var result = await ExecuteProcedure2DataTableAsync(conn, sql, parameters).ConfigureAwait(false);
        await tx.CommitAsync().ConfigureAwait(false);

        return result;
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        DataSet? ds = await ExecuteProcedure2DataSetAsync(conn, sql, parameters).ConfigureAwait(false);

        if (ds == null)
            return null;

        return ds.Tables[0];
    }

    public static DataTable? ExecuteProcedure2DataTable(DbCommand cmd)
    {
        return ExecuteProcedure2DataTableAsync(cmd).Result;
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(DbCommand cmd)
    {
        DataSet? ds = await ExecuteProcedure2DataSetAsync(cmd).ConfigureAwait(false);

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

    public static DataSet? ExecuteProcedure2DataSet(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataSetAsync(conn, sql, parameters).Result;
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteProcedure2DataSetAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync().ConfigureAwait(false);
        using DbTransaction tx = await conn.BeginTransactionAsync().ConfigureAwait(false);
        var result = await ExecuteProcedure2DataSetAsync(conn, sql, parameters).ConfigureAwait(false);
        await tx.CommitAsync().ConfigureAwait(false);

        return result;
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync().ConfigureAwait(false);
        using DbTransaction tx = await conn.BeginTransactionAsync().ConfigureAwait(false);
        var result = await ExecuteProcedure2DataSetAsync(conn, sql, parameters).ConfigureAwait(false);
        await tx.CommitAsync().ConfigureAwait(false);

        return result;
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        DataSet? ds = null;

        await Task.Run(async () =>
        {
            using DbCommand cmd = conn.CreateCommand();
            SetupProcedureCall(cmd, sql, isDataSetReturn: true, parameters);

            AddParameters(cmd, parameters);

            using DbDataAdapter da = CreateDataAdapter(conn)!;
            da.SelectCommand = cmd;

            ds = new DataSet();
            da.Fill(ds);

            if (conn is NpgsqlConnection && ds.Tables.Count == 1 && ds.Tables[0].Columns.Count == 1)
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
                            DataTable dt = await ExecutePostgresCursorToTableAsync(conn, openCursor).ConfigureAwait(false);
                            dt.TableName = $"TABLE{k++}";
                            ds.Tables.Add(dt);
                        }
                    }
                }
            }

            DisposeLobParameters(da.SelectCommand, parameters);
        });

        return ds;
    }

    private static Task<DataTable> ExecutePostgresCursorToTableAsync(DbConnection conn, string cursorName)
    {
        string sql = $"FETCH ALL IN \"{cursorName}\"";

        using DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using DbDataAdapter da = CreateDataAdapter(conn)!;
        da.SelectCommand = cmd;

        DataTable dt = new DataTable();
        da.Fill(dt);

        return Task.FromResult(dt);
    }

    public static DataSet? ExecuteProcedure2DataSet(DbCommand cmd)
    {
        return ExecuteProcedure2DataSetAsync(cmd).Result;
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(DbCommand cmd)
    {
        DataSet? ds = null;

        await Task.Run(() =>
        {
            using DbDataAdapter da = CreateDataAdapter(cmd.Connection)!;
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

    public static List<SqlParam> ExecuteFunction(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteFunctionAsync(conn, sql, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteFunctionAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync().ConfigureAwait(false);
        return await ExecuteFunctionAsync(conn, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        List<SqlParam> outParameters = new List<SqlParam>();

        using DbCommand cmd = conn.CreateCommand();
        SetupFunctionCall(cmd, sql, parameters);

        AddParameters(cmd, parameters.Where(x => x.paramDirection != ParameterDirection.ReturnValue).ToArray());

        using DataTable dt = await ExecuteProcedure2DataTableAsync(cmd).ConfigureAwait(false) ?? new DataTable();

        foreach (DbParameter param in cmd.Parameters)
        {
            if (param.Direction != ParameterDirection.InputOutput
                && param.Direction != ParameterDirection.Output
                && param.Direction != ParameterDirection.ReturnValue)
                continue;

            if (dt.Rows.Count > 0 && dt.Columns.Contains(param.ParameterName))
                outParameters.Add(new SqlParam(param.ParameterName, dt.Rows[0][param.ParameterName]) { paramDirection = param.Direction });
        }

        DisposeLobParameters(cmd, parameters);

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

    public static object? ExecuteScalar(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteScalarAsync(conn, sql, parameters).Result;
    }

    public static async Task<object?> ExecuteScalarAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteScalarAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<object?> ExecuteScalarAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteScalarAsync(new DbConnectionFactory(dbtype, conn_str), sql, parameters).ConfigureAwait(false);
    }

    public static async Task<object?> ExecuteScalarAsync(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync().ConfigureAwait(false);
        return await ExecuteScalarAsync(conn, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<object?> ExecuteScalarAsync(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        using DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        AddParameters(cmd, parameters);

        var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);

        DisposeLobParameters(cmd, parameters);

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

    public static List<SqlParam> ExecuteNonQuery(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteNonQueryAsync(conn, sql, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteNonQueryAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return await ExecuteNonQueryAsync(new DbConnectionFactory(dbtype, conn_str), sql, parameters).ConfigureAwait(false);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync().ConfigureAwait(false);
        return await ExecuteNonQueryAsync(conn, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        List<SqlParam> outParameters = new List<SqlParam>();

        using DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        AddParameters(cmd, parameters);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        foreach (DbParameter param in cmd.Parameters)
        {
            if (param.Direction != ParameterDirection.InputOutput
                && param.Direction != ParameterDirection.Output
                && param.Direction != ParameterDirection.ReturnValue)
                continue;

            outParameters.Add(new SqlParam(param.ParameterName, param.Value) { paramDirection = param.Direction });
        }

        DisposeLobParameters(cmd, parameters);

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

    public static T? QueryRow<T>(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryRowAsync<T>(conn, sql, parameters).Result;
    }

    public static async Task<T?> QueryRowAsync<T>(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryRowAsync<T>(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<T?> QueryRowAsync<T>(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryRowAsync<T>(new DbConnectionFactory(dbtype, conn_str), sql, parameters).ConfigureAwait(false);
    }

    public static async Task<T?> QueryRowAsync<T>(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync().ConfigureAwait(false);
        return await QueryRowAsync<T>(conn, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<T?> QueryRowAsync<T>(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        List<T> results = await QueryAsync<T>(conn, sql, parameters).ConfigureAwait(false);

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

    public static List<T> Query<T>(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryAsync<T>(conn, sql, parameters).Result;
    }

    public static async Task<List<T>> QueryAsync<T>(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryAsync<T>(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<List<T>> QueryAsync<T>(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryAsync<T>(new DbConnectionFactory(dbtype, conn_str), sql, parameters).ConfigureAwait(false);
    }

    public static async Task<List<T>> QueryAsync<T>(DbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync().ConfigureAwait(false);
        return await QueryAsync<T>(conn, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<List<T>> QueryAsync<T>(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        using DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        AddParameters(cmd, parameters);

        var result = await cmd.QueryAsync<T>().ConfigureAwait(false);

        DisposeLobParameters(cmd, parameters);
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

    public static DataTable? QueryDataTable(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryDataTableAsync(conn, sql, parameters).Result;
    }

    public static async Task<DataTable?> QueryDataTableAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryDataTableAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<DataTable?> QueryDataTableAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync().ConfigureAwait(false);
        return await QueryDataTableAsync(conn, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<DataTable?> QueryDataTableAsync(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        DataTable? dt = null;

        await Task.Run(() =>
        {
            using DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;

            AddParameters(cmd, parameters);

            using DbDataAdapter da = CreateDataAdapter(conn)!;
            da.SelectCommand = cmd;

            dt = new DataTable();
            da.Fill(dt);
            DisposeLobParameters(da.SelectCommand, parameters);
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

    public static DataSet? QueryDataSet(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryDataSetAsync(conn, sql, parameters).Result;
    }

    public static async Task<DataSet?> QueryDataSetAsync(string conn_str, string sql, params SqlParam[] parameters)
    {
        return await QueryDataSetAsync(DbConnectionFactory.DefaultDbType, conn_str, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<DataSet?> QueryDataSetAsync(DbConnectionType dbtype, string conn_str, string sql, params SqlParam[] parameters)
    {
        using DbConnection conn = await new DbConnectionFactory(dbtype, conn_str).BuildAndOpenAsync().ConfigureAwait(false);
        return await QueryDataSetAsync(conn, sql, parameters).ConfigureAwait(false);
    }

    public static async Task<DataSet?> QueryDataSetAsync(DbConnection conn, string sql, params SqlParam[] parameters)
    {
        DataSet? ds = null;

        await Task.Run(() =>
        {
            using DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;

            AddParameters(cmd, parameters);

            using DbDataAdapter da = CreateDataAdapter(conn)!;
            da.SelectCommand = cmd;

            ds = new DataSet();
            da.Fill(ds);
            DisposeLobParameters(da.SelectCommand, parameters);
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
        UpdateTable(conn, table, model);
        conn.Close();
    }

    private static void UpdateTableFromDataTable(DbConnection conn, string table, DataTable modelTable)
    {
        string sql = $"select * from {table} where 1 = 2";

        using DataTable dt = new DataTable();
        using DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using DbDataAdapter dataAdapter = CreateDataAdapter(conn)!;
        dataAdapter.SelectCommand = cmd;
        dataAdapter.UpdateBatchSize = 128;

        using DbCommandBuilder commandBuilder = CreateCommandBuilder(conn, dataAdapter);
        dataAdapter.InsertCommand = commandBuilder.GetInsertCommand();
        dataAdapter.UpdateCommand = commandBuilder.GetUpdateCommand();

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

    private static void UpdateTableFromGenericModel<T>(DbConnection conn, string table, T model)
    {
        string sql = $"select * from {table} where 1 = 2";

        using DataTable dt = new DataTable();
        using DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using DbDataAdapter dataAdapter = CreateDataAdapter(conn)!;
        dataAdapter.SelectCommand = cmd;
        dataAdapter.UpdateBatchSize = 128;

        using DbCommandBuilder commandBuilder = CreateCommandBuilder(conn, dataAdapter);
        dataAdapter.InsertCommand = commandBuilder.GetInsertCommand();
        dataAdapter.UpdateCommand = commandBuilder.GetUpdateCommand();

        dataAdapter.Fill(dt);

        dt.CreateRowFromModel(model);

        dataAdapter.Update(dt);
    }

    public static void UpdateTable<T>(DbConnection conn, string table, T model)
    {
        if (model == null)
            throw new ArgumentNullException(nameof(model));

        string sql = $"select * from {table} where 1 = 2";

        if (typeof(T) == typeof(DataTable))
            UpdateTableFromDataTable(conn, table, (model as DataTable)!);
        else
            UpdateTableFromGenericModel(conn, table, model);
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
        UpdateTable(conn, table, models);
        conn.Close();
    }

    public static void UpdateTable<T>(DbConnection conn, string table, List<T> models)
    {
        string sql = $"select * from {table} where 1 = 2";

        using DataTable dt = new DataTable();
        using DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using DbDataAdapter dataAdapter = CreateDataAdapter(conn)!;
        dataAdapter.SelectCommand = cmd;
        dataAdapter.UpdateBatchSize = 128;

        using DbCommandBuilder commandBuilder = CreateCommandBuilder(conn, dataAdapter);
        dataAdapter.InsertCommand = commandBuilder.GetInsertCommand();
        dataAdapter.UpdateCommand = commandBuilder.GetUpdateCommand();

        dataAdapter.Fill(dt);

        foreach (T model in models)
            dt.CreateRowFromModel(model);

        dataAdapter.Update(dt);
    }

    private static DbDataAdapter? CreateDataAdapter(DbConnection? conn)
    {
        if (conn == null)
            throw new ArgumentNullException(nameof(conn));
        
        DbDataAdapter? da = DbProviderFactories.GetFactory(conn)?.CreateDataAdapter();

        if (conn is OracleConnection && da != null && da is OracleDataAdapter)
            (da as OracleDataAdapter)!.SuppressGetDecimalInvalidCastException = true;

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

    private static void AddReturnValueParameter(DbCommand cmd)
    {
        DbParameter param = cmd.CreateParameter();
        param.Direction = ParameterDirection.ReturnValue;
        param.ParameterName = "returnValue";
        cmd.Parameters.Add(param);
    }

    private static void DisposeLobParameters(DbCommand cmd, params SqlParam[] parameters)
    {
        if (parameters == null)
            return;

        foreach (SqlParam prm in parameters)
        {
            if (prm.isClob && cmd.Connection is OracleConnection && prm.value != null && prm.value != DBNull.Value)
            {
                string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;

                if (cmd.Parameters[baseParameterName].Value as OracleClob != null)
                    (cmd.Parameters[baseParameterName].Value as OracleClob)!.Dispose();
            }
            else if (prm.isBlob && cmd.Connection is OracleConnection && prm.value != null && prm.value != DBNull.Value)
            {
                string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;

                if (cmd.Parameters[baseParameterName].Value as OracleBlob != null)
                    (cmd.Parameters[baseParameterName].Value as OracleBlob)!.Dispose();
            }
        }
    }

    public static void AddParameters(DbCommand cmd, List<SqlParam> parameters)
    {
        AddParameters(cmd, parameters.ToArray());
    }

    public static void AddParameters(DbCommand cmd, params SqlParam[] parameters)
    {
        if (parameters == null)
            return;

        foreach (SqlParam prm in parameters)
        {
            DbParameter param = cmd.CreateParameter();

            if (cmd.Connection is OracleConnection)
            {
                string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;
                param.ParameterName = baseParameterName;
                cmd.CommandText = cmd.CommandText.Replace($"@{baseParameterName}", $":{baseParameterName}");
            }
            else if (cmd.Connection is SQLiteConnection)
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
            else if (prm.isClob && cmd.Connection is OracleConnection && prm.value != null && prm.value != DBNull.Value)
            {
                if (prm.value == null)
                {
                    param.Value = DBNull.Value;
                }
                else
                {
                    OracleClob clob = new OracleClob(cmd.Connection as OracleConnection);
                    byte[] byteContent = Encoding.Unicode.GetBytes((prm.value as string)!);
                    clob.Write(byteContent, 0, byteContent.Length);

                    param.Value = clob;
                }
            }
            else if (prm.isBlob && cmd.Connection is OracleConnection && prm.value != null && prm.value != DBNull.Value)
            {
                if (prm.value == null)
                {
                    param.Value = DBNull.Value;
                }
                else
                {
                    OracleBlob blob = new OracleBlob(cmd.Connection as OracleConnection);
                    byte[] byteContent = (prm.value as byte[])!;
                    blob.Write(byteContent, 0, byteContent.Length);

                    param.Value = blob;
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
