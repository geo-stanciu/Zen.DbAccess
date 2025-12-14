using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Interfaces;
using Zen.DbAccess.Models;

namespace Zen.DbAccess.DatabaseSpeciffic;

public interface IDbSpeciffic
{
    DbProviderFactory BuildDbProviderFactory(DbConnectionType dbType)
    {
        DbProviderFactory dbProviderFactory = dbType switch
        {
            DbConnectionType.SqlServer => DbProviderFactories.GetFactory(DbFactoryNames.SQL_SERVER),
            DbConnectionType.Oracle => DbProviderFactories.GetFactory(DbFactoryNames.ORACLE),
            DbConnectionType.Postgresql => DbProviderFactories.GetFactory(DbFactoryNames.POSTGRESQL),
            DbConnectionType.MariaDb => DbProviderFactories.GetFactory(DbFactoryNames.MARIADB),
            _ => throw new NotImplementedException($"Not implemented {dbType}")
        };

        return dbProviderFactory;
    }

    (string, SqlParam) CommonPrepareEmptyParameter(PropertyInfo propertyInfo)
    {
        SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}", DBNull.Value);

        Type t = Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType;

        if (t == typeof(byte[]))
            prm.isBlob = true;

        return ($"@p_{propertyInfo.Name}", prm);
    }

    (string, SqlParam) CommonPrepareParameter(DbModel model, PropertyInfo propertyInfo)
    {
        SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}", propertyInfo.GetValue(model));

        Type t = Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType;

        if (t == typeof(byte[]))
            prm.isBlob = true;

        return ($"@p_{propertyInfo.Name}", prm);
    }

    (string, SqlParam) PrepareEmptyParameter(DbModel model, PropertyInfo propertyInfo)
    {
        return CommonPrepareEmptyParameter(propertyInfo);
    }

    (string, SqlParam) PrepareParameter(DbModel model, PropertyInfo propertyInfo)
    {
        return CommonPrepareParameter(model, propertyInfo);
    }

    void DisposeBlob(DbCommand cmd, SqlParam prm)
    {
    }

    void DisposeClob(DbCommand cmd, SqlParam prm)
    {
    }

    bool ShouldSetDbTypeBinary()
    {
        return true;
    }

    object GetValueAsBlob(IZenDbConnection conn, object value)
    {
        return value ?? DBNull.Value;
    }

    bool IsBlob(object value)
    {
        Type valType = value.GetType();
        Type t = Nullable.GetUnderlyingType(valType) ?? valType;

        return t == typeof(byte[]);
    }

    object GetValueAsClob(IZenDbConnection conn, object value)
    {
        return value ?? DBNull.Value;
    }

    DbParameter CreateDbParameter(DbCommand cmd, SqlParam prm)
    {
        DbParameter param = cmd.CreateParameter();
        param.ParameterName = prm.name;
        return param;
    }

    DbDataAdapter CreateDataAdapter(IZenDbConnection conn)
    {
        DbDataAdapter? da = DbProviderFactories.GetFactory(conn.Connection)?.CreateDataAdapter();

        if (da == null)
            throw new NullReferenceException("DataAdapter");

        return da;
    }

    bool UsePrimaryKeyPropertyForInsert()
    {
        return false;
    }

    async Task InsertAsync(DbModel model, DbCommand cmd, bool insertPrimaryKeyColumn, DbModelSaveType saveType)
    {
        if (!insertPrimaryKeyColumn && saveType != DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn)
        {
            object? val = await cmd.ExecuteScalarAsync();

            if (val == null || val == DBNull.Value)
                return;

            if (model.dbModel_primaryKey_dbColumns != null && model.dbModel_primaryKey_dbColumns.Any())
            {
                var pkProp = model.dbModel_dbColumn_map![model.dbModel_primaryKey_dbColumns[0]];

                if (pkProp.PropertyType == typeof(int))
                {
                    pkProp.SetValue(model, Convert.ToInt32(val), null);
                }
                else if (pkProp.PropertyType == typeof(long))
                {
                    pkProp.SetValue(model, Convert.ToInt64(val), null);
                }
                else
                {
                    pkProp.SetValue(model, val, null);
                }
            }
        }
        else
        {
            await cmd.ExecuteNonQueryAsync();
        }
    }

    void EnsureTempTable(string table);

    void CommonSetupFunctionCall(DbCommand cmd, string sql, params SqlParam[] parameters)
    {
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

        cmd.CommandText = sbSql.ToString();
    }

    void SetupFunctionCall(DbCommand cmd, string sql, params SqlParam[] parameters)
    {
        CommonSetupFunctionCall(cmd, sql, parameters);
    }

    void SetupProcedureCall(IZenDbConnection conn, DbCommand cmd, string sql, bool isQueryReturn, params SqlParam[] parameters)
    {
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = sql;
    }

    bool ShouldFetchProcedureAsCursorsAsync()
    {
        return false;
    }

    Task<List<T>> QueryCursorAsync<T>(IZenDbConnection conn, string procedureName, string cursorName)
    {
        throw new NotImplementedException($"QueryCursorAsync not available for {conn.DbType}");
    }

    Task<List<string>> QueryCursorNamesAsync(IZenDbConnection conn, DbCommand cmd)
    {
        throw new NotImplementedException($"QueryCursorAsync not available for {conn.DbType}");
    }

    Task<DataSet> ExecuteProcedure2DataSetAsync(IZenDbConnection conn, DbDataAdapter da)
    {
        DataSet ds = new DataSet();
        da.Fill(ds);

        return Task.FromResult(ds);
    }


    string GetGetServerDateTimeQuery();

    (string, IEnumerable<SqlParam>) GetInsertedIdQuery(string table, DbModel model, string firstPropertyName);

    Tuple<string, SqlParam[]> PrepareBulkInsertBatchWithSequence<T>(
       List<T> list,
       IZenDbConnection conn,
       string table,
       bool insertPrimaryKeyColumn,
       string sequence2UseForPrimaryKey) where T : DbModel;

    Tuple<string, SqlParam[]> PrepareBulkInsertBatch<T>(
        List<T> list,
        IZenDbConnection conn,
        string table) where T : DbModel;

    object? GetValueAsDateOnly(IZenDbConnection conn, DateOnly dtoValue)
    {
        return dtoValue;
    }

    object? GetValueAsTimeOnly(IZenDbConnection conn, TimeOnly toValue)
    {
        return toValue;
    }

    object GetValueForPreparedParameter(DbModel dbModel, PropertyInfo propertyInfo)
    {
        return propertyInfo.GetValue(dbModel) ?? DBNull.Value;
    }
}
