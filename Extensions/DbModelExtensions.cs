using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Models;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Utils;
using System.Security.AccessControl;
using Zen.DbAccess.Interfaces;
using System.Globalization;

namespace Zen.DbAccess.Extensions;

public static class DbModelExtensions
{
    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tint = typeof(int);

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tintNull = typeof(int?);

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tlong = typeof(long);

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tlongNull = typeof(long?);

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tbool = typeof(bool);

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tboolNull = typeof(bool?);

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tdecimal = typeof(decimal);

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tdecimalNull = typeof(decimal?);

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tdatetime = typeof(DateTime);

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tdatetimeNull = typeof(DateTime?);

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    private static Type dbModel_tString = typeof(string);

    public static bool HasAuditIgnoreAttribute(this DbModel dbModel,  PropertyInfo propertyInfo)
    {
        return Attribute.IsDefined(propertyInfo, typeof(AuditIgnoreAttribute));
    }

    public static bool HasDbModelPropertyIgnoreAttribute(this DbModel dbModel, PropertyInfo propertyInfo)
    {
        return Attribute.IsDefined(propertyInfo, typeof(DbIgnoreAttribute));
    }

    private static void RefreshDbColumnsIfEmpty(
        this DbModel dbModel,
        string table,
        DbNamingConvention dbNamingConvention,
        char? startQuoteMark = null,
        char? endQuoteMark = null)
    {
        if (dbModel.dbModel_dbColumns != null && dbModel.dbModel_dbColumns.Count > 0)
            return;

        dbModel.dbModel_dbColumns = new HashSet<string>();
        dbModel.dbModel_dbColumn_map = new Dictionary<string, PropertyInfo>();
        dbModel.dbModel_prop_map = new Dictionary<string, string>();

        var properties = dbModel.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);

        foreach (var property in properties)
        {
            string dbColumnName = dbNamingConvention switch
            {
                DbNamingConvention.SnakeCase => GetSnakeCaseColumnName(property.Name),
                DbNamingConvention.CamelCase => GetCamelCaseColumnName(property.Name),
                DbNamingConvention.UseQuoteMarkes => GetUseQuoteMarkesColumnName(property.Name, startQuoteMark, endQuoteMark),
                _ => throw new NotImplementedException($"{dbNamingConvention}")
            };

            dbModel.dbModel_dbColumns.Add(dbColumnName);
            dbModel.dbModel_dbColumn_map[dbColumnName] = property;
            dbModel.dbModel_prop_map[property.Name] = dbColumnName;
        }
    }

    private static string GetSnakeCaseColumnName(string propName)
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

    private static string GetCamelCaseColumnName(string propName)
    {
        string[] parts = propName.Split('_', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length == 1)
            return parts[0];

        StringBuilder sbName = new ();

        for (int i = 0; i < parts.Length; i++)
        {
            if (i > 0)
                sbName.Append('_');

            sbName.Append(parts[i]);
        }

        return sbName.ToString();
    }

    private static string GetUseQuoteMarkesColumnName(
        string propName,
        char? startQuoteMark,
        char? endQuoteMark)
    {
        return $"{startQuoteMark}{propName}{endQuoteMark}";
    }

    public static bool HasPrimaryKey(this DbModel dbModel)
    {
        if (dbModel.dbModel_primaryKey_dbColumns == null)
        {
            return false;
        }

        return dbModel.dbModel_primaryKey_dbColumns.Any();
    }

    public static List<string>? GetPrimaryKeyColumns(this DbModel dbModel)
    {
        return dbModel.dbModel_primaryKey_dbColumns;
    }

    public static bool IsPartOfThePrimaryKey(this DbModel dbModel, string dbColumn)
    {
        if (dbModel.dbModel_primaryKey_dbColumns == null)
        {
            return false;
        }

        return dbModel.dbModel_primaryKey_dbColumns.Any(x => x == dbColumn);
    }

    public static string? GetMappedProperty(this DbModel dbModel, string name)
    {
        if (dbModel.dbModel_prop_map == null || !dbModel.dbModel_prop_map.TryGetValue(name, out var propName))
        {
            return null;
        }

        return propName;
    }

    public static List<PropertyInfo> GetPropertiesToUpdate(this DbModel dbModel)
    {
        if (dbModel.dbModel_dbColumns == null)
            throw new NullReferenceException("dbModel_dbColumns");

        if (dbModel.dbModel_dbColumn_map == null)
            throw new NullReferenceException("dbModel_dbColumn_map");

        if (dbModel.dbModel_primaryKey_dbColumns == null)
            throw new NullReferenceException("dbModel_primaryKey_dbColumns");

        return dbModel.dbModel_dbColumns
            .Where(x => dbModel.dbModel_dbColumn_map.ContainsKey(x) && !dbModel.dbModel_primaryKey_dbColumns.Contains(x))
            .Select(x => dbModel.dbModel_dbColumn_map[x])
            .ToList();
    }

    public static List<PropertyInfo> GetPropertiesToInsert(this DbModel dbModel, IZenDbConnection conn, bool insertPrimaryKeyColumn, string sequence2UseForPrimaryKey = "")
    {
        if (dbModel.dbModel_dbColumns == null)
            throw new NullReferenceException("dbModel_dbColumns");

        if (dbModel.dbModel_dbColumn_map == null)
            throw new NullReferenceException("dbModel_dbColumn_map");

        if (dbModel.dbModel_primaryKey_dbColumns == null)
            throw new NullReferenceException("dbModel_primaryKey_dbColumns");

        return dbModel.dbModel_dbColumns
            .Where(x => dbModel.dbModel_dbColumn_map.ContainsKey(x)
                        && (insertPrimaryKeyColumn
                            || (!insertPrimaryKeyColumn && conn.DatabaseSpeciffic.UsePrimaryKeyPropertyForInsert() && !string.IsNullOrEmpty(sequence2UseForPrimaryKey))
                            || (!insertPrimaryKeyColumn && !dbModel.dbModel_primaryKey_dbColumns.Contains(x))
                        )
                    )
            .Select(x => dbModel.dbModel_dbColumn_map[x])
            .ToList();
    }

    public static List<PropertyInfo> GetPrimaryKeyProperties(this DbModel dbModel)
    {
        if (dbModel.dbModel_dbColumns == null)
            throw new NullReferenceException("dbModel_dbColumns");

        if (dbModel.dbModel_dbColumn_map == null)
            throw new NullReferenceException("dbModel_dbColumn_map");

        if (dbModel.dbModel_primaryKey_dbColumns == null)
            throw new NullReferenceException("dbModel_primaryKey_dbColumns");

        return dbModel.dbModel_primaryKey_dbColumns
            .Select(x => dbModel.dbModel_dbColumn_map[x])
            .ToList();
    }

    private static async Task ConstructUpdateQueryAsync(this DbModel dbModel, IZenDbConnection conn, string table)
    {
        RefreshDbColumnsIfEmpty(dbModel, table, conn.NamingConvention);

        StringBuilder sbUpdate = new StringBuilder();
        sbUpdate.Append($"update {table} set ");

        bool firstParam = true;

        DeterminePrimaryKey(dbModel);

        if (dbModel.dbModel_prop_map == null)
            throw new NullReferenceException("dbModel_prop_map");

        List<PropertyInfo> propertiesToUpdate = GetPropertiesToUpdate(dbModel);

        for (int i = 0; i < propertiesToUpdate.Count; i++)
        {
            PropertyInfo propertyInfo = propertiesToUpdate[i];

            if (firstParam)
                firstParam = false;
            else
                sbUpdate.Append(", ");

            (string preparedParameterName, SqlParam prm) = conn.DatabaseSpeciffic.PrepareParameter(dbModel, propertyInfo);

            sbUpdate.Append($" {dbModel.dbModel_prop_map[propertyInfo.Name]} = {preparedParameterName} ");

            dbModel.dbModel_sql_update.sql_parameters.Add(prm);
        }

        bool isFirstPkCol = true;

        foreach (var pkDbCol in dbModel.dbModel_primaryKey_dbColumns!)
        {
            if (isFirstPkCol)
            {
                sbUpdate.Append(" where ");
                isFirstPkCol = false;
            }
            else
            {
                sbUpdate.Append(" and ");
            }

            var prop = dbModel.dbModel_dbColumn_map![pkDbCol];

            var pkPrm = new SqlParam($"@p_{prop.Name}", prop.GetValue(dbModel));
            dbModel.dbModel_sql_update.sql_parameters.Add(pkPrm);

            sbUpdate.Append($" {pkDbCol} = @p_{prop.Name}");
        }

        dbModel.dbModel_sql_update.sql_query = sbUpdate.ToString();
    }

    private static void RefreshParameterValuesForUpdate(this DbModel dbModel)
    {
        List<PropertyInfo> propertiesToUpdate = GetPropertiesToUpdate(dbModel);

        for (int i = 0; i < propertiesToUpdate.Count; i++)
        {
            PropertyInfo propertyInfo = propertiesToUpdate[i];

            var dbCol = dbModel.dbModel_prop_map![propertyInfo.Name];

            SqlParam? prm = dbModel.dbModel_sql_update.sql_parameters.FirstOrDefault(x => x.name == $"@p_{propertyInfo.Name}");

            if (prm != null)
                prm.value = propertyInfo.GetValue(dbModel) ?? DBNull.Value;
        }

        List<PropertyInfo> primaryKeys = GetPrimaryKeyProperties(dbModel);

        for (int i = 0; i < primaryKeys.Count; i++)
        {
            PropertyInfo propertyInfo = primaryKeys[i];
            var dbCol = dbModel.dbModel_prop_map![propertyInfo.Name];

            SqlParam? prm = dbModel.dbModel_sql_update.sql_parameters.FirstOrDefault(x => x.name == $"@p_{propertyInfo.Name}");

            if (prm != null)
                prm.value = propertyInfo.GetValue(dbModel) ?? DBNull.Value;
        }
    }

    private static void DeterminePrimaryKey(this DbModel dbModel)
    {
        if (dbModel.dbModel_primaryKey_dbColumns != null && dbModel.dbModel_primaryKey_dbColumns.Count > 0)
            return;

        if (dbModel.dbModel_dbColumns == null)
            throw new NullReferenceException("dbModel_dbColumns");

        if (dbModel.dbModel_dbColumn_map == null)
            throw new NullReferenceException("dbModel_dbColumn_map");

        dbModel.dbModel_primaryKey_dbColumns = new List<string>();

        foreach (var dbCol in dbModel.dbModel_dbColumns)
        {
            if (!dbModel.dbModel_dbColumn_map.TryGetValue(dbCol, out PropertyInfo? prop))
                continue;

            if (prop == null)
                continue;

            if (Attribute.IsDefined(prop, typeof(PrimaryKeyAttribute)))
                dbModel.dbModel_primaryKey_dbColumns.Add(dbCol);
        }

        if (dbModel.dbModel_primaryKey_dbColumns.Count == 0)
            throw new Exception("There must be a property with the [PrimaryKey] attribute.");
    }

    public static async Task RefreshDbColumnsAndModelPropertiesAsync(this DbModel dbModel, IZenDbConnection conn, string table)
    {
        RefreshDbColumnsIfEmpty(dbModel, table, conn.NamingConvention);
        DeterminePrimaryKey(dbModel);
    }

    private static async Task ConstructInsertQueryAsync(
        this DbModel dbModel,
        DbModelSaveType saveType,
        IZenDbConnection conn,
        string table, 
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey = "")
    {
        RefreshDbColumnsIfEmpty(dbModel, table, conn.NamingConvention);

        StringBuilder sbInsertValues = new StringBuilder();
        StringBuilder sbInsert = new StringBuilder();

        sbInsert.Append($"insert into {table} (");

        DeterminePrimaryKey(dbModel);

        if (dbModel.dbModel_dbColumns == null)
            throw new NullReferenceException("dbModel_dbColumns");

        if (dbModel.dbModel_dbColumn_map == null)
            throw new NullReferenceException("dbModel_dbColumn_map");

        if (dbModel.dbModel_primaryKey_dbColumns == null)
            throw new NullReferenceException("dbModel_primaryKey_dbColumns");

        if (dbModel.dbModel_prop_map == null)
            throw new NullReferenceException("dbModel_prop_map");

        bool firstParam = true;

        List<PropertyInfo> propertiesToInsert = GetPropertiesToInsert(dbModel, conn, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);

        for (int i = 0; i < propertiesToInsert.Count; i++)
        {
            PropertyInfo propertyInfo = propertiesToInsert[i];

            if (firstParam)
                firstParam = false;
            else
            {
                sbInsert.Append(", ");
                sbInsertValues.Append(", ");
            }

            var dbCol = dbModel.dbModel_prop_map[propertyInfo.Name];

            if (!insertPrimaryKeyColumn
                && conn.DatabaseSpeciffic.UsePrimaryKeyPropertyForInsert()
                && !string.IsNullOrEmpty(sequence2UseForPrimaryKey)
                && dbModel.dbModel_primaryKey_dbColumns.Any(x => x == dbCol))
            {
                sbInsert.Append($" {dbCol} ");
                sbInsertValues.Append($"{sequence2UseForPrimaryKey}.nextval");

                continue;
            }

            (string preparedParameterName, SqlParam prm) = conn.DatabaseSpeciffic.PrepareParameter(dbModel, propertyInfo);

            sbInsert.Append($" {dbCol} ");
            sbInsertValues.Append($" {preparedParameterName} ");

            dbModel.dbModel_sql_insert.sql_parameters.Add(prm);
        }

        sbInsert.Append(") values (").Append(sbInsertValues).Append(")");

        if (!insertPrimaryKeyColumn && saveType != DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn)
        {
            (string sql, IEnumerable<SqlParam> sqlParams) = conn.DatabaseSpeciffic.GetInsertedIdQuery(table, dbModel, propertiesToInsert.First().Name);

            sbInsert.Append(sql);
            dbModel.dbModel_sql_insert.sql_parameters.AddRange(sqlParams);
        }

        dbModel.dbModel_sql_insert.sql_query = sbInsert.ToString();
    }

    private static void RefreshParameterValuesForInsert(this DbModel dbModel, IZenDbConnection conn, bool insertPrimaryKeyColumn)
    {
        if (dbModel.dbModel_sql_insert == null)
            throw new NullReferenceException("dbModel_sql_insert");

        List<PropertyInfo> propertiesToInsert = GetPropertiesToInsert(dbModel, conn, insertPrimaryKeyColumn);

        for (int i = 0; i < propertiesToInsert.Count; i++)
        {
            PropertyInfo propertyInfo = propertiesToInsert[i];

            SqlParam? prm = dbModel.dbModel_sql_insert.sql_parameters[i];

            if (prm.name != $"@p_{propertyInfo.Name}")
                prm = dbModel.dbModel_sql_insert.sql_parameters.FirstOrDefault(x => x.name == $"@p_{propertyInfo.Name}");

            if (prm != null)
                prm.value = propertyInfo.GetValue(dbModel) ?? DBNull.Value;
        }
    }

    private static async Task<int> RunQueryAsync(
        this DbModel dbModel,
        DbModelSaveType saveType,
        IZenDbConnection conn,
        string sql, 
        List<SqlParam> parameters, 
        bool insertPrimaryKeyColumn, 
        bool isInsert = false)
    {
        int affected = 0;

        DeterminePrimaryKey(dbModel);

        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        cmd.CommandText = sql;

        DBUtils.AddParameters(conn, cmd, parameters);

        if (!isInsert)
        {
            affected = await cmd.ExecuteNonQueryAsync();
            return affected;
        }

        affected = 1;
        await conn.DatabaseSpeciffic.InsertAsync(dbModel, cmd, insertPrimaryKeyColumn, saveType);

        return affected;
    }

    public static void Save(this DbModel dbModel, IZenDbConnection conn, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, DbModelSaveType.InsertUpdate, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbModelSaveType saveType, IZenDbConnection conn, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, saveType, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, IDbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, DbModelSaveType.InsertUpdate, dbConnectionFactory, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbModelSaveType saveType, IDbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, saveType, dbConnectionFactory, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static async Task SaveAsync(this DbModel dbModel, IDbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        await SaveAsync(dbModel, DbModelSaveType.InsertUpdate, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
    }

    public static async Task SaveAsync(this DbModel dbModel, DbModelSaveType saveType, IDbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        await SaveAsync(dbModel, saveType, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
    }

    public static async Task SaveAsync(
        this DbModel dbModel,
        IZenDbConnection conn,
        string table, 
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "")
    {
        await SaveAsync(
            dbModel, 
            DbModelSaveType.InsertUpdate,
            conn,
            table, 
            insertPrimaryKeyColumn,
            sequence2UseForPrimaryKey
        );
    }

    public static async Task SaveAsync(
        this DbModel dbModel,
        DbModelSaveType saveType,
        IZenDbConnection conn,
        string table, 
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "")
    {
        if (string.IsNullOrEmpty(dbModel.dbModel_table) || table != dbModel.dbModel_table)
        {
            dbModel.ResetDbModel();
        }

        RefreshDbColumnsIfEmpty(dbModel, table, conn.NamingConvention);

        if (saveType == DbModelSaveType.InsertUpdate && PrimaryKeyFieldsHaveValues(dbModel))
        {
            // we need to try tp update first since we have a value for the primary key field
            if (string.IsNullOrEmpty(dbModel.dbModel_sql_update.sql_query))
                await ConstructUpdateQueryAsync(dbModel, conn, table);
            else
                RefreshParameterValuesForUpdate(dbModel);

            // try to update
            int affected = await RunQueryAsync(
                dbModel,
                saveType,
                conn,
                dbModel.dbModel_sql_update.sql_query, 
                dbModel.dbModel_sql_update.sql_parameters, 
                insertPrimaryKeyColumn,
                false
            );

            if (affected > 0)
                return;
        }

        if (string.IsNullOrEmpty(dbModel.dbModel_sql_insert.sql_query))
            await ConstructInsertQueryAsync(dbModel, saveType, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
        else
            RefreshParameterValuesForInsert(dbModel, conn, insertPrimaryKeyColumn);

        // try to insert
        _ = await RunQueryAsync(
            dbModel,
            saveType,
            conn,
            dbModel.dbModel_sql_insert.sql_query, 
            dbModel.dbModel_sql_insert.sql_parameters, 
            insertPrimaryKeyColumn,
            true
        );
    }

    public static async Task DeleteAsync(this DbModel dbModel, IDbConnectionFactory dbConnectionFactory, string table)
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        await DeleteAsync(dbModel, conn, table);
    }

    public static async Task DeleteAsync(this DbModel dbModel, IZenDbConnection conn, string table)
    {
        if (string.IsNullOrEmpty(dbModel.dbModel_sql_delete.sql_query))
            await ConstructDeleteQueryAsync(dbModel, conn, table);
        
        string sql = dbModel.dbModel_sql_delete.sql_query;
        
        _ = await sql.ExecuteNonQueryAsync(conn, dbModel.dbModel_sql_delete.sql_parameters.ToArray());
    }

    private static async Task ConstructDeleteQueryAsync(DbModel dbModel, IZenDbConnection conn, string table)
    {
        RefreshDbColumnsIfEmpty(dbModel, table, conn.NamingConvention);
        DeterminePrimaryKey(dbModel);

        StringBuilder sbSql = new StringBuilder();
        sbSql.Append($"delete from {table} where ");

        bool isFirst = true;

        foreach (string pkDbCol in dbModel.dbModel_primaryKey_dbColumns!)
        {
            if (isFirst)
                isFirst = false;
            else
                sbSql.Append(" and ");

            PropertyInfo primaryKeyProp = dbModel.dbModel_dbColumn_map![pkDbCol];
            string prmName = $"@p_{primaryKeyProp.Name}";

            sbSql.Append($" {pkDbCol} = {prmName} ");

            SqlParam prm = new SqlParam(prmName, primaryKeyProp.GetValue(dbModel) ?? DBNull.Value);
            dbModel.dbModel_sql_delete.sql_parameters.Add(prm);
        }

        dbModel.dbModel_sql_delete.sql_query = sbSql.ToString();
    }

    private static bool PrimaryKeyFieldsHaveValues(this DbModel dbModel)
    {
        DeterminePrimaryKey(dbModel);

        List<PropertyInfo> primaryKeyProps = dbModel.dbModel_primaryKey_dbColumns!
            .Select(x => dbModel.dbModel_dbColumn_map![x])
            .ToList();

        if (!primaryKeyProps.Any())
            return false;

        foreach (PropertyInfo primaryKeyProp in primaryKeyProps)
        {
            object primaryKeyVal = primaryKeyProp.GetValue(dbModel) ?? DBNull.Value;
            Type primaryKeyValType = primaryKeyProp.PropertyType;
            object? defaultValue = primaryKeyValType.IsValueType ? Activator.CreateInstance(primaryKeyValType) : null;

            if (primaryKeyVal == null)
                return false;

            if (defaultValue == null && primaryKeyVal != null)
                return true;

            if (primaryKeyValType.IsValueType)
            {
                if (primaryKeyValType == dbModel_tint || primaryKeyValType == dbModel_tintNull)
                {
                    int val = Convert.ToInt32(primaryKeyVal);

                    if (val == -1 || val == Convert.ToInt32(defaultValue))
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == dbModel_tlong || primaryKeyValType == dbModel_tlongNull)
                {
                    long val = Convert.ToInt64(primaryKeyVal);

                    if (val == -1L || val == Convert.ToInt64(defaultValue))
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == dbModel_tbool || primaryKeyValType == dbModel_tboolNull)
                {
                    if (Convert.ToBoolean(primaryKeyVal) == Convert.ToBoolean(defaultValue))
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == dbModel_tdecimal || primaryKeyValType == dbModel_tdecimalNull)
                {
                    decimal val = Convert.ToDecimal(primaryKeyVal);

                    if (val == -1M || val == Convert.ToDecimal(defaultValue))
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == dbModel_tdatetime || primaryKeyValType == dbModel_tdatetimeNull)
                {
                    if (Convert.ToDateTime(primaryKeyVal) == Convert.ToDateTime(defaultValue))
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == dbModel_tString)
                {
                    if (Convert.ToString(primaryKeyVal) == Convert.ToString(defaultValue))
                        return false;
                    else
                        return true;
                }
            }
        }

        return false;
    }
}
