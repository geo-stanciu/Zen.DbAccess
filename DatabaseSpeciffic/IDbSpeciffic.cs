using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Helpers;
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

    char EscapeCustomNameStartChar()
    {
        return '"';
    }

    char EscapeCustomNameEndChar()
    {
        return '"';
    }

    string GetDbColumnNameForProperty<T>(
        IZenDbConnection conn,
        T model,
        PropertyInfo property,
        Dictionary<string, List<DbNameAttribute>> customColumnNames,
        char? startQuoteMark = null,
        char? endQuoteMark = null
        ) where T: DbModel
    {
        return GetDbColumnNameForProperty<T>(conn.DbType, conn.NamingConvention, property, customColumnNames, startQuoteMark, endQuoteMark);
    }

    string GetDbColumnNameForProperty<T>(
        DbConnectionType dbType,
        DbNamingConvention namingConvention,
        PropertyInfo property,
        Dictionary<string, List<DbNameAttribute>> customColumnNames,
        char? startQuoteMark = null,
        char? endQuoteMark = null
        ) where T: DbModel
    {
        var dbColumnNameFromAttribute = customColumnNames.TryGetValue(property.Name, out var customNames)
            ? customNames
                .OrderBy(x => x.DbTypes != null && x.DbTypes.Contains(dbType) ? 0 : 1)?
                .FirstOrDefault()?
                .DbColumn
            : null;

        if (!string.IsNullOrWhiteSpace(dbColumnNameFromAttribute))
        {
            dbColumnNameFromAttribute = $"{(startQuoteMark != null ? startQuoteMark : EscapeCustomNameStartChar())}{dbColumnNameFromAttribute}{(endQuoteMark != null ? endQuoteMark : EscapeCustomNameEndChar())}";
        }

        string dbColumnName = dbColumnNameFromAttribute
            ?? namingConvention switch
            {
                DbNamingConvention.SnakeCase => GetSnakeCaseColumnName(property.Name),
                DbNamingConvention.CamelCase => GetCamelCaseColumnName(property.Name, startQuoteMark, endQuoteMark),
                DbNamingConvention.UseQuoteMarkes => GetUseQuoteMarkesColumnName(property.Name, startQuoteMark, endQuoteMark),
                _ => throw new NotImplementedException($"{namingConvention}")
            };

        return dbColumnName;
    }

    string GetSnakeCaseColumnName(string propName)
    {
        string uppercaseChars = string.Concat(propName.Where(c => c >= 'A' && c <= 'Z'));

        if (uppercaseChars.Length == 0)
        {
            return propName;
        }

        StringBuilder sbName = new ();

        int k = 0;
        int pos = 0;

        while (k < uppercaseChars.Length && pos < propName.Length)
        {
            char c = uppercaseChars[k++];
            int idx = propName.IndexOf(c, pos);
            
            sbName.Append(propName.Substring(pos, idx - pos).ToLower());

            if (idx > 0)
            {
                sbName.Append("_");
            }

            sbName.Append(propName.Substring(idx, 1).ToLower());

            pos = idx + 1;
        }

        if (pos < propName.Length)
        {
            sbName.Append(propName.Substring(pos).ToLower());
        }

        return sbName.ToString();
    }

    string GetCamelCaseColumnName(string propName, char? startQuoteMark = null, char? endQuoteMark = null)
    {
        string[] parts = propName.Split('_', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length == 1)
            return parts[0];

        StringBuilder sbName = new ();

        for (int i = 0; i < parts.Length; i++)
        {
            if (string.IsNullOrEmpty(parts[i]))
                continue;

            sbName
                .Append(startQuoteMark != null ? startQuoteMark : EscapeCustomNameStartChar())
                .Append(parts[i].Substring(0, 1).ToUpper())
                .Append(parts[i].Substring(1).ToLower())
                .Append(endQuoteMark != null ? endQuoteMark : EscapeCustomNameEndChar());
        }

        return sbName.ToString();
    }

    string GetUseQuoteMarkesColumnName(
        string propName,
        char? startQuoteMark,
        char? endQuoteMark)
    {
        return $"{startQuoteMark}{propName}{endQuoteMark}";
    }

    string GenerateQueryColumns<T>(IZenDbConnection conn, T model) where T: DbModel
    {
        return GenerateQueryColumns<T>(conn);
    }
    
    string GenerateQueryColumns<T>(IZenDbConnection conn) where T: DbModel
    {
        return GenerateQueryColumns<T>(conn.DbType, conn.NamingConvention);
    }

    string GenerateQueryColumns<T>(DbConnectionType dbType, DbNamingConvention namingConvention, T model) where T: DbModel
    {
        return GenerateQueryColumns<T>(dbType, namingConvention);
    }

    string GenerateQueryColumns<T>(DbConnectionType dbType, DbNamingConvention namingConvention) where T: DbModel
    {
        string cachekey = $"{typeof(T).FullName}_{dbType}";
        
        var columnsQuery = CacheHelper.GetOrAdd(cachekey, () =>
        {
            var customColumnNames = typeof(T)
                .GetProperties()
                .Select(x => new { x.Name, Attributes = x.GetCustomAttributes<DbNameAttribute>()?.ToList() })
                .Where(x => x.Attributes != null && x.Attributes.Count > 0)
                .ToDictionary(x => x.Name, y => y.Attributes!);

            List<string> columnNames = typeof(T)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(x => !Attribute.IsDefined(x, typeof(DbIgnoreAttribute)))
                .Select(x => GetDbColumnNameForProperty<T>(dbType, namingConvention, x, customColumnNames))
                .ToList();

            return string.Join(", ", columnNames);
        });

        return columnsQuery;
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
