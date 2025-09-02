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
using Zen.DbAccess.Interfaces;

namespace Zen.DbAccess.Utils;

public static class DBUtils
{
    public static async Task<DateTime> GetServerDateTime(IDbConnectionFactory dbConnectionFactory)
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        return GetServerDateTime(conn);
    }

    public static DateTime GetServerDateTime(IZenDbConnection conn)
    {
        string sql = conn.DatabaseSpeciffic.GetGetServerDateTimeQuery();

        DateTime dt = (DateTime)ExecuteScalar(conn, sql)!;
        return dt;
    }

    public static List<SqlParam> ExecuteProcedure(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedureAsync( conn, sql, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(IDbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        return await ExecuteProcedureAsync(conn, sql, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteProcedureAsync(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        List<SqlParam> outParameters = new List<SqlParam>();

        using (DbCommand cmd = conn.Connection.CreateCommand())
        {
            if (conn.Transaction != null && cmd.Transaction == null)
                cmd.Transaction = conn.Transaction;

            conn.DatabaseSpeciffic.SetupProcedureCall(conn, cmd, sql, isDataSetReturn: false, isTableReturn: false, parameters);

            AddParameters(conn, cmd, parameters);

            await cmd.ExecuteNonQueryAsync();

            foreach (DbParameter param in cmd.Parameters)
            {
                if (param.Direction != ParameterDirection.InputOutput && param.Direction != ParameterDirection.Output)
                    continue;

                outParameters.Add(new SqlParam(param.ParameterName, param.Value) { paramDirection = param.Direction });
            }

            DisposeLobParameters(conn, cmd, parameters);
        }

        return outParameters;
    }

    public static DataTable? ExecuteProcedure2DataTable(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataTableAsync(conn, sql, parameters).Result;
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        conn.DatabaseSpeciffic.SetupProcedureCall(conn, cmd, sql, isDataSetReturn: false, isTableReturn: true, parameters);

        AddParameters(conn, cmd, parameters);

        using DbDataAdapter da = conn.DatabaseSpeciffic.CreateDataAdapter(conn);
        da.SelectCommand = cmd;

        DataSet ds = await conn.DatabaseSpeciffic.ExecuteProcedure2DataSetAsync(conn, da);

        DisposeLobParameters(conn, da.SelectCommand, parameters);

        return ds.Tables[0];
    }

    public static async Task<DataTable?> ExecuteProcedure2DataTableAsync(IZenDbConnection conn, DbCommand cmd)
    {
        DataSet? ds = await ExecuteProcedure2DataSetAsync(conn, cmd);

        if (ds == null)
            return null;

        return ds.Tables[0];
    }

    public static DataSet? ExecuteProcedure2DataSet(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteProcedure2DataSetAsync(conn, sql, parameters).Result;
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(IDbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        var result = await ExecuteProcedure2DataSetAsync(conn, sql, parameters);

        return result;
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        DataSet? ds = null;

        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        conn.DatabaseSpeciffic.SetupProcedureCall(conn, cmd, sql, isDataSetReturn: true, isTableReturn: false, parameters);

        AddParameters(conn, cmd, parameters);

        using DbDataAdapter da = conn.DatabaseSpeciffic.CreateDataAdapter(conn);
        da.SelectCommand = cmd;

        ds = await conn.DatabaseSpeciffic.ExecuteProcedure2DataSetAsync(conn, da);

        DisposeLobParameters(conn, da.SelectCommand, parameters);

        return ds;
    }

    public static DataSet? ExecuteProcedure2DataSet(IZenDbConnection conn, DbCommand cmd)
    {
        return ExecuteProcedure2DataSetAsync(conn, cmd).Result;
    }

    public static async Task<DataSet?> ExecuteProcedure2DataSetAsync(IZenDbConnection conn, DbCommand cmd)
    {
        DataSet? ds = null;

        using DbDataAdapter da = conn.DatabaseSpeciffic.CreateDataAdapter(conn);
        da.SelectCommand = cmd;

        ds = await conn.DatabaseSpeciffic.ExecuteProcedure2DataSetAsync(conn, da);

        return ds;
    }

    public static List<SqlParam> ExecuteFunction(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteFunctionAsync(conn, sql, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteFunctionAsync(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        List<SqlParam> outParameters = new List<SqlParam>();

        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        conn.DatabaseSpeciffic.SetupFunctionCall(cmd, sql, parameters);

        AddParameters(conn, cmd, parameters.Where(x => x.paramDirection != ParameterDirection.ReturnValue).ToArray());

        using DataTable dt = await ExecuteProcedure2DataTableAsync(conn, cmd) ?? new DataTable();

        foreach (DbParameter param in cmd.Parameters)
        {
            if (param.Direction != ParameterDirection.InputOutput
                && param.Direction != ParameterDirection.Output
                && param.Direction != ParameterDirection.ReturnValue)
                continue;

            if (dt.Rows.Count > 0 && dt.Columns.Contains(param.ParameterName))
                outParameters.Add(new SqlParam(param.ParameterName, dt.Rows[0][param.ParameterName]) { paramDirection = param.Direction });
        }

        DisposeLobParameters(conn, cmd, parameters);

        return outParameters;
    }

    public static object? ExecuteScalar(IDbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        return ExecuteScalarAsync(dbConnectionFactory, sql, parameters).Result;
    }

    public static object? ExecuteScalar(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteScalarAsync(conn, sql, parameters).Result;
    }

    public static async Task<object?> ExecuteScalarAsync(IDbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        return await ExecuteScalarAsync(conn, sql, parameters);
    }

    public static async Task<object?> ExecuteScalarAsync(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        cmd.CommandText = sql;

        AddParameters(conn, cmd, parameters);

        var result = await cmd.ExecuteScalarAsync();

        DisposeLobParameters(conn, cmd, parameters);

        return result;
    }

    public static List<SqlParam> ExecuteNonQuery(IDbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        return ExecuteNonQueryAsync(dbConnectionFactory, sql, parameters).Result;
    }

    public static List<SqlParam> ExecuteNonQuery(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        return ExecuteNonQueryAsync(conn, sql, parameters).Result;
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(IDbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        return await ExecuteNonQueryAsync(conn, sql, parameters);
    }

    public static async Task<List<SqlParam>> ExecuteNonQueryAsync(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        List<SqlParam> outParameters = new List<SqlParam>();

        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        cmd.CommandText = sql;

        AddParameters(conn, cmd, parameters);

        await cmd.ExecuteNonQueryAsync();

        foreach (DbParameter param in cmd.Parameters)
        {
            if (param.Direction != ParameterDirection.InputOutput
                && param.Direction != ParameterDirection.Output
                && param.Direction != ParameterDirection.ReturnValue)
                continue;

            outParameters.Add(new SqlParam(param.ParameterName, param.Value) { paramDirection = param.Direction });
        }

        DisposeLobParameters(conn, cmd, parameters);

        return outParameters;
    }

    public static T? QueryRow<T>(IDbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        return QueryRowAsync<T>(dbConnectionFactory, sql, parameters).Result;
    }

    public static T? QueryRow<T>(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryRowAsync<T>(conn, sql, parameters).Result;
    }

    public static async Task<T?> QueryRowAsync<T>(IDbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        return await QueryRowAsync<T>(conn, sql, parameters);
    }

    public static async Task<T?> QueryRowAsync<T>(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        List<T> results = await QueryAsync<T>(conn, sql, parameters);

        if (results.Count == 0)
            throw new Exception("no data found");

        return results.FirstOrDefault();
    }

    public static List<T> Query<T>(IDbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        return QueryAsync<T>(dbConnectionFactory, sql, parameters).Result;
    }

    public static List<T> Query<T>(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryAsync<T>(conn, sql, parameters).Result;
    }

    public static async Task<List<T>> QueryAsync<T>(IDbConnectionFactory dbConnectionFactory, string sql, params SqlParam[] parameters)
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        return await QueryAsync<T>(conn, sql, parameters);
    }

    public static async Task<List<T>> QueryAsync<T>(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        cmd.CommandText = sql;

        AddParameters(conn, cmd, parameters);

        var result = await cmd.QueryAsync<T>();

        DisposeLobParameters(conn, cmd, parameters);
        return result;
    }

    public static DataTable? QueryDataTable(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryDataTableAsync(conn, sql, parameters).Result;
    }

    public static async Task<DataTable?> QueryDataTableAsync(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        DataTable? dt = null;

        await Task.Run(() =>
        {
            using DbCommand cmd = conn.Connection.CreateCommand();

            if (conn.Transaction != null && cmd.Transaction == null)
                cmd.Transaction = conn.Transaction;

            cmd.CommandText = sql;

            AddParameters(conn, cmd, parameters);

            using DbDataAdapter da = conn.DatabaseSpeciffic.CreateDataAdapter(conn);
            da.SelectCommand = cmd;

            dt = new DataTable();
            da.Fill(dt);
            DisposeLobParameters(conn, da.SelectCommand, parameters);
        });

        return dt;
    }

    public static DataSet? QueryDataSet(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        return QueryDataSetAsync(conn, sql, parameters).Result;
    }

    public static async Task<DataSet?> QueryDataSetAsync(IZenDbConnection conn, string sql, params SqlParam[] parameters)
    {
        DataSet? ds = null;

        await Task.Run(() =>
        {
            using DbCommand cmd = conn.Connection.CreateCommand();

            if (conn.Transaction != null && cmd.Transaction == null)
                cmd.Transaction = conn.Transaction;

            cmd.CommandText = sql;

            AddParameters(conn, cmd, parameters);

            using DbDataAdapter da = conn.DatabaseSpeciffic.CreateDataAdapter(conn);
            da.SelectCommand = cmd;

            ds = new DataSet();
            da.Fill(ds);
            DisposeLobParameters(conn, da.SelectCommand, parameters);
        });

        return ds;
    }

    public static async Task UpdateTableAsync<T>(IDbConnectionFactory dbConnectionFactory, string table, T model)
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        UpdateTable(conn, table, model);
    }

    private static void UpdateTableFromDataTable(IZenDbConnection conn, string table, DataTable modelTable)
    {
        string sql = $"select * from {table} where 1 = 2";

        using DataTable dt = new DataTable();
        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        cmd.CommandText = sql;

        using DbDataAdapter dataAdapter = conn.DatabaseSpeciffic.CreateDataAdapter(conn);
        dataAdapter.SelectCommand = cmd;
        dataAdapter.UpdateBatchSize = 128;

        using DbCommandBuilder commandBuilder = CreateCommandBuilder(conn, dataAdapter);
        dataAdapter.InsertCommand = commandBuilder.GetInsertCommand();
        dataAdapter.UpdateCommand = commandBuilder.GetUpdateCommand();

        if (conn.Transaction != null && dataAdapter.InsertCommand.Transaction == null)
            dataAdapter.InsertCommand.Transaction = conn.Transaction;

        if (conn.Transaction != null && dataAdapter.UpdateCommand.Transaction == null)
            dataAdapter.UpdateCommand.Transaction = conn.Transaction;

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

    private static void UpdateTableFromGenericModel<T>(IZenDbConnection conn, string table, T model)
    {
        string sql = $"select * from {table} where 1 = 2";

        using DataTable dt = new DataTable();
        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        cmd.CommandText = sql;

        using DbDataAdapter dataAdapter = conn.DatabaseSpeciffic.CreateDataAdapter(conn);
        dataAdapter.SelectCommand = cmd;
        dataAdapter.UpdateBatchSize = 128;

        using DbCommandBuilder commandBuilder = CreateCommandBuilder(conn, dataAdapter);
        dataAdapter.InsertCommand = commandBuilder.GetInsertCommand();
        dataAdapter.UpdateCommand = commandBuilder.GetUpdateCommand();

        if (conn.Transaction != null && dataAdapter.InsertCommand.Transaction == null)
            dataAdapter.InsertCommand.Transaction = conn.Transaction;

        if (conn.Transaction != null && dataAdapter.UpdateCommand.Transaction == null)
            dataAdapter.UpdateCommand.Transaction = conn.Transaction;

        dataAdapter.Fill(dt);

        dt.CreateRowFromModel(model);

        dataAdapter.Update(dt);
    }

    public static void UpdateTable<T>(IZenDbConnection conn, string table, T model)
    {
        if (model == null)
            throw new ArgumentNullException(nameof(model));

        string sql = $"select * from {table} where 1 = 2";

        if (typeof(T) == typeof(DataTable))
            UpdateTableFromDataTable(conn, table, (model as DataTable)!);
        else
            UpdateTableFromGenericModel(conn, table, model);
    }

    public static async Task UpdateTableAsync<T>(IDbConnectionFactory dbConnectionFactory, string table, List<T> models)
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        UpdateTable(conn, table, models);
    }

    public static void UpdateTable<T>(IZenDbConnection conn, string table, List<T> models)
    {
        string sql = $"select * from {table} where 1 = 2";

        using DataTable dt = new DataTable();
        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = cmd.Transaction;

        cmd.CommandText = sql;

        using DbDataAdapter dataAdapter = conn.DatabaseSpeciffic.CreateDataAdapter(conn);
        dataAdapter.SelectCommand = cmd;
        dataAdapter.UpdateBatchSize = 128;

        using DbCommandBuilder commandBuilder = CreateCommandBuilder(conn, dataAdapter);
        dataAdapter.InsertCommand = commandBuilder.GetInsertCommand();
        dataAdapter.UpdateCommand = commandBuilder.GetUpdateCommand();

        if (conn.Transaction != null && dataAdapter.InsertCommand.Transaction == null)
            dataAdapter.InsertCommand.Transaction = conn.Transaction;

        if (conn.Transaction != null && dataAdapter.UpdateCommand.Transaction == null)
            dataAdapter.UpdateCommand.Transaction = conn.Transaction;

        dataAdapter.Fill(dt);

        foreach (T model in models)
            dt.CreateRowFromModel(model);

        dataAdapter.Update(dt);
    }

    private static DbCommandBuilder CreateCommandBuilder(IZenDbConnection conn, DbDataAdapter dataAdapter)
    {
        DbCommandBuilder? builder = DbProviderFactories.GetFactory(conn.Connection)?.CreateCommandBuilder();

        if (builder == null)
            throw new NullReferenceException(nameof(builder));

        builder.DataAdapter = dataAdapter;

        return builder;
    }

    private static void DisposeLobParameters(IZenDbConnection conn, DbCommand cmd, params SqlParam[] parameters)
    {
        if (parameters == null)
            return;

        foreach (SqlParam prm in parameters)
        {
            if (prm.isClob)
            {
                conn.DatabaseSpeciffic.DisposeClob(cmd, prm);
            }
            else if (prm.isBlob)
            {
                conn.DatabaseSpeciffic.DisposeBlob(cmd, prm);
            }
        }
    }

    public static void AddParameters(IZenDbConnection conn, DbCommand cmd, List<SqlParam> parameters)
    {
        AddParameters(conn, cmd, parameters.ToArray());
    }

    public static void AddParameters(IZenDbConnection conn, DbCommand cmd, params SqlParam[] parameters)
    {
        if (parameters == null)
            return;

        foreach (SqlParam prm in parameters)
        {
            DbParameter param = conn.DatabaseSpeciffic.CreateDbParameter(cmd, prm);
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

            if (prm.value == null || prm.value == DBNull.Value)
            {
                param.Value = prm.value ?? DBNull.Value;

                cmd.Parameters.Add(param);

                continue;
            }

            if (prm.value is bool)
            {
                param.Value = Convert.ToBoolean(prm.value) ? 1 : 0;
            }
            else if (prm.isClob)
            {
                param.Value = conn.DatabaseSpeciffic.GetValueAsClob(conn, prm.value);
            }
            else if (prm.isBlob || conn.DatabaseSpeciffic.IsBlob(prm.value))
            {
                prm.isBlob = true;
                param.Value = conn.DatabaseSpeciffic.GetValueAsBlob(conn, prm.value);
            }
            else if (prm.value is Enum)
            {
                param.Value = Convert.ToInt32(prm.value);
            }
            else
            {
                param.Value = prm.value ?? DBNull.Value;
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
