using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Helpers;
using Zen.DbAccess.Interfaces;
using Zen.DbAccess.Models;
using Zen.DbAccess.Utils;

namespace Zen.DbAccess.Extensions;

public static class DbModelExtensions
{
    private static ConcurrentDictionary<string, DbPropertiesCacheModel> _propertiesCache = new();

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

        string cachekey = $"{dbModel.GetType().FullName}_{table}";
        
        var cachedProps = _propertiesCache.GetOrAdd(cachekey, key =>
        {
            var dbColumns = new HashSet<string>();
            var dbColumnMap = new Dictionary<string, PropertyInfo>();
            var propMap = new Dictionary<string, string>();

            var properties = dbModel
                .GetType()
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(x => !dbModel.HasDbModelPropertyIgnoreAttribute(x))
                .ToArray();

            foreach (var property in properties)
            {
                string dbColumnName = dbNamingConvention switch
                {
                    DbNamingConvention.SnakeCase => GetSnakeCaseColumnName(property.Name),
                    DbNamingConvention.CamelCase => GetCamelCaseColumnName(property.Name),
                    DbNamingConvention.UseQuoteMarkes => GetUseQuoteMarkesColumnName(property.Name, startQuoteMark, endQuoteMark),
                    _ => throw new NotImplementedException($"{dbNamingConvention}")
                };

                dbColumns.Add(dbColumnName);
                dbColumnMap[dbColumnName] = property;
                propMap[property.Name] = dbColumnName;
            }

            return new DbPropertiesCacheModel
            {
                Table = table,
                DbColumns = dbColumns,
                DbColumnMap = dbColumnMap,
                PropMap = propMap,
            };
        });

        dbModel.dbModel_table = cachedProps?.Table;
        dbModel.dbModel_dbColumns = cachedProps?.DbColumns;
        dbModel.dbModel_dbColumn_map = cachedProps?.DbColumnMap;
        dbModel.dbModel_prop_map = cachedProps?.PropMap;
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

    public static List<PropertyInfo> GetPropertiesToUpdate(this DbModel dbModel, string table)
    {
        if (dbModel.dbModel_dbColumns == null)
            throw new NullReferenceException("dbModel_dbColumns");

        if (dbModel.dbModel_dbColumn_map == null)
            throw new NullReferenceException("dbModel_dbColumn_map");

        if (dbModel.dbModel_primaryKey_dbColumns == null)
            throw new NullReferenceException("dbModel_primaryKey_dbColumns");

        string cachekey = $"{dbModel.GetType().FullName}_{table}_PropertiesToUpdate";

        var cachedProps = _propertiesCache.GetOrAdd(cachekey, key =>
        {
            List<PropertyInfo> props = dbModel.dbModel_dbColumns
                .Where(x => dbModel.dbModel_dbColumn_map.ContainsKey(x)
                            && !dbModel.dbModel_primaryKey_dbColumns.Contains(x)
                            && !x.Equals("is_error", StringComparison.OrdinalIgnoreCase)
                            && !x.Equals("error_message", StringComparison.OrdinalIgnoreCase))
                .Select(x => dbModel.dbModel_dbColumn_map[x])
                .ToList();

            return new DbPropertiesCacheModel
            {
                PropertiesToUpdate = props,
            };
        });

        return cachedProps.PropertiesToUpdate;
    }

    public static List<PropertyInfo> GetPropertiesToInsert(this DbModel dbModel, IZenDbConnection conn, bool insertPrimaryKeyColumn, string table, string sequence2UseForPrimaryKey = "")
    {
        if (dbModel.dbModel_dbColumns == null)
            throw new NullReferenceException("dbModel_dbColumns");

        if (dbModel.dbModel_dbColumn_map == null)
            throw new NullReferenceException("dbModel_dbColumn_map");

        if (dbModel.dbModel_primaryKey_dbColumns == null)
            throw new NullReferenceException("dbModel_primaryKey_dbColumns");

        string cachekey = $"{dbModel.GetType().FullName}_{table}_{insertPrimaryKeyColumn}_{sequence2UseForPrimaryKey}_PropertiesToInsert";

        var cachedProps = _propertiesCache.GetOrAdd(cachekey, key =>
        {
            List<PropertyInfo> props = dbModel.dbModel_dbColumns
                .Where(x => dbModel.dbModel_dbColumn_map.ContainsKey(x)
                            && !x.Equals("is_error", StringComparison.OrdinalIgnoreCase)
                            && !x.Equals("error_message", StringComparison.OrdinalIgnoreCase)
                            && (insertPrimaryKeyColumn
                                || (!insertPrimaryKeyColumn && conn.DatabaseSpeciffic.UsePrimaryKeyPropertyForInsert() && !string.IsNullOrEmpty(sequence2UseForPrimaryKey))
                                || (!insertPrimaryKeyColumn && !dbModel.dbModel_primaryKey_dbColumns.Contains(x))
                            )
                        )
                .Select(x => dbModel.dbModel_dbColumn_map[x])
                .ToList();

            return new DbPropertiesCacheModel
            {
                PropertiesToInsert = props,
            };
        });

        return cachedProps.PropertiesToInsert;
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

    private static void ConstructUpdateQuery(this DbModel dbModel, IZenDbConnection conn, string table)
    {
        RefreshDbColumnsIfEmpty(dbModel, table, conn.NamingConvention);

        DeterminePrimaryKey(dbModel);

        if (dbModel.dbModel_prop_map == null)
            throw new NullReferenceException("dbModel_prop_map");

        string cachekey = $"{dbModel.GetType().FullName}_{table}_UpdateQuery";

        var cachedProps = _propertiesCache.GetOrAdd(cachekey, key =>
        {
            StringBuilder sbUpdate = new StringBuilder();
            sbUpdate.Append($"update {table} set ");

            bool firstParam = true;

            List<PropertyInfo> propertiesToUpdate = GetPropertiesToUpdate(dbModel, table);
            var sqlUpdateParams = new List<SqlParam>();

            for (int i = 0; i < propertiesToUpdate.Count; i++)
            {
                PropertyInfo propertyInfo = propertiesToUpdate[i];

                if (firstParam)
                    firstParam = false;
                else
                    sbUpdate.Append(", ");

                (string preparedParameterName, SqlParam prm) = conn.DatabaseSpeciffic.PrepareEmptyParameter(dbModel, propertyInfo);

                sbUpdate.Append($" {dbModel.dbModel_prop_map[propertyInfo.Name]} = {preparedParameterName} ");

                sqlUpdateParams.Add(prm);
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

                var pkPrm = new SqlParam($"@p_{prop.Name}", DBNull.Value);
                sqlUpdateParams.Add(pkPrm);

                sbUpdate.Append($" {pkDbCol} = @p_{prop.Name}");
            }

            return new DbPropertiesCacheModel
            {
                SqlUpdate = sbUpdate.ToString(),
                SqlUpdateParams = sqlUpdateParams,
            };
        });

        dbModel.dbModel_sql_update.sql_query = cachedProps.SqlUpdate;
        dbModel.dbModel_sql_update.sql_parameters = cachedProps.SqlUpdateParams;

        RefreshParameterValuesForUpdate(dbModel, conn, table);
    }

    private static void RefreshParameterValuesForUpdate(this DbModel dbModel, IZenDbConnection conn, string table)
    {
        List<PropertyInfo> propertiesToUpdate = GetPropertiesToUpdate(dbModel, table);

        for (int i = 0; i < propertiesToUpdate.Count; i++)
        {
            PropertyInfo propertyInfo = propertiesToUpdate[i];

            var dbCol = dbModel.dbModel_prop_map![propertyInfo.Name];

            SqlParam? prm = dbModel.dbModel_sql_update.sql_parameters.FirstOrDefault(x => x.name == $"@p_{propertyInfo.Name}");

            if (prm != null)
                prm.value = conn.DatabaseSpeciffic.GetValueForPreparedParameter(dbModel, propertyInfo);
        }

        List<PropertyInfo> primaryKeys = GetPrimaryKeyProperties(dbModel);

        for (int i = 0; i < primaryKeys.Count; i++)
        {
            PropertyInfo propertyInfo = primaryKeys[i];
            var dbCol = dbModel.dbModel_prop_map![propertyInfo.Name];

            SqlParam? prm = dbModel.dbModel_sql_update.sql_parameters.FirstOrDefault(x => x.name == $"@p_{propertyInfo.Name}");

            if (prm != null)
                prm.value = conn.DatabaseSpeciffic.GetValueForPreparedParameter(dbModel, propertyInfo);
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

        string cachekey = $"{dbModel.GetType().FullName}_PrimaryKeyDbColumns";

        var cachedProps = _propertiesCache.GetOrAdd(cachekey, key =>
        {
            var primaryKeyDbColumns = new List<string>();

            foreach (var dbCol in dbModel.dbModel_dbColumns)
            {
                if (!dbModel.dbModel_dbColumn_map.TryGetValue(dbCol, out PropertyInfo? prop))
                    continue;

                if (prop == null)
                    continue;

                if (Attribute.IsDefined(prop, typeof(PrimaryKeyAttribute)))
                    primaryKeyDbColumns.Add(dbCol);
            }

            if (primaryKeyDbColumns.Count == 0)
                throw new Exception("There must be a property with the [PrimaryKey] attribute.");

            return new DbPropertiesCacheModel
            {
                PrimaryKeyDbColumns = primaryKeyDbColumns,
            };
        });

        dbModel.dbModel_primaryKey_dbColumns = cachedProps.PrimaryKeyDbColumns;
    }

    public static void RefreshDbColumnsAndModelProperties(this DbModel dbModel, IZenDbConnection conn, string table)
    {
        RefreshDbColumnsIfEmpty(dbModel, table, conn.NamingConvention);
        DeterminePrimaryKey(dbModel);
    }

    private static void ConstructInsertQuery(
        this DbModel dbModel,
        DbModelSaveType saveType,
        IZenDbConnection conn,
        string table, 
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey = "")
    {
        RefreshDbColumnsIfEmpty(dbModel, table, conn.NamingConvention);

        DeterminePrimaryKey(dbModel);

        if (dbModel.dbModel_dbColumns == null)
            throw new NullReferenceException("dbModel_dbColumns");

        if (dbModel.dbModel_dbColumn_map == null)
            throw new NullReferenceException("dbModel_dbColumn_map");

        if (dbModel.dbModel_primaryKey_dbColumns == null)
            throw new NullReferenceException("dbModel_primaryKey_dbColumns");

        if (dbModel.dbModel_prop_map == null)
            throw new NullReferenceException("dbModel_prop_map");

        string cachekey = $"{dbModel.GetType().FullName}_{table}_InsertQuery";

        var cachedProps = _propertiesCache.GetOrAdd(cachekey, key =>
        {
            StringBuilder sbInsertValues = new StringBuilder();
            StringBuilder sbInsert = new StringBuilder();
            var sqlInsertParams = new List<SqlParam>();

            sbInsert.Append($"insert into {table} (");

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

                (string preparedParameterName, SqlParam prm) = conn.DatabaseSpeciffic.PrepareEmptyParameter(dbModel, propertyInfo);

                sbInsert.Append($" {dbCol} ");
                sbInsertValues.Append($" {preparedParameterName} ");

                sqlInsertParams.Add(prm);
            }

            sbInsert.Append(") values (").Append(sbInsertValues).Append(")");

            if (!insertPrimaryKeyColumn && saveType != DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn)
            {
                (string sql, IEnumerable<SqlParam> sqlParams) = conn.DatabaseSpeciffic.GetInsertedIdQuery(table, dbModel, propertiesToInsert.First().Name);

                sbInsert.Append(sql);
                sqlInsertParams.AddRange(sqlParams);
            }

            return new DbPropertiesCacheModel
            {
                SqlInsert = sbInsert.ToString(),
                SqlInsertParams = sqlInsertParams,
            };
        });

        dbModel.dbModel_sql_insert.sql_query = cachedProps.SqlInsert;
        dbModel.dbModel_sql_insert.sql_parameters = cachedProps.SqlInsertParams;

        RefreshParameterValuesForInsert(dbModel, conn, insertPrimaryKeyColumn, table);
    }

    private static void RefreshParameterValuesForInsert(this DbModel dbModel, IZenDbConnection conn, bool insertPrimaryKeyColumn, string table)
    {
        if (dbModel.dbModel_sql_insert == null)
            throw new NullReferenceException("dbModel_sql_insert");

        List<PropertyInfo> propertiesToInsert = GetPropertiesToInsert(dbModel, conn, insertPrimaryKeyColumn, table);

        for (int i = 0; i < propertiesToInsert.Count; i++)
        {
            PropertyInfo propertyInfo = propertiesToInsert[i];

            SqlParam? prm = dbModel.dbModel_sql_insert.sql_parameters[i];

            if (prm.name != $"@p_{propertyInfo.Name}")
                prm = dbModel.dbModel_sql_insert.sql_parameters.FirstOrDefault(x => x.name == $"@p_{propertyInfo.Name}");

            if (prm != null)
                prm.value = conn.DatabaseSpeciffic.GetValueForPreparedParameter(dbModel, propertyInfo);
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
            dbModel.dbModel_table = table;
        }

        RefreshDbColumnsIfEmpty(dbModel, table, conn.NamingConvention);

        if (saveType == DbModelSaveType.InsertUpdate && PrimaryKeyFieldsHaveValues(dbModel))
        {
            // we need to try tp update first since we have a value for the primary key field
            if (string.IsNullOrEmpty(dbModel.dbModel_sql_update.sql_query))
                ConstructUpdateQuery(dbModel, conn, table);
            else
                RefreshParameterValuesForUpdate(dbModel, conn, table);

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
            ConstructInsertQuery(dbModel, saveType, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
        else
            RefreshParameterValuesForInsert(dbModel, conn, insertPrimaryKeyColumn, table);

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
            ConstructDeleteQuery(dbModel, conn, table);
        
        string sql = dbModel.dbModel_sql_delete.sql_query;
        
        _ = await sql.ExecuteNonQueryAsync(conn, dbModel.dbModel_sql_delete.sql_parameters.ToArray());
    }

    private static void ConstructDeleteQuery(DbModel dbModel, IZenDbConnection conn, string table)
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
            Type primaryKeyValType = Nullable.GetUnderlyingType(primaryKeyProp.PropertyType) ?? primaryKeyProp.PropertyType;
            object? defaultValue = primaryKeyValType.IsValueType ? Activator.CreateInstance(primaryKeyValType) : null;

            if (primaryKeyVal == null)
                return false;

            if (defaultValue == null && primaryKeyVal != null)
                return true;

            if (primaryKeyValType.IsValueType)
            {
                if (primaryKeyValType == typeof(int))
                {
                    int val = Convert.ToInt32(primaryKeyVal);

                    if (val == -1 || val == Convert.ToInt32(defaultValue))
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == typeof(long))
                {
                    long val = Convert.ToInt64(primaryKeyVal);

                    if (val == -1L || val == Convert.ToInt64(defaultValue))
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == typeof(bool))
                {
                    if (Convert.ToBoolean(primaryKeyVal) == Convert.ToBoolean(defaultValue))
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == typeof(decimal))
                {
                    decimal val = Convert.ToDecimal(primaryKeyVal);

                    if (val == -1M || val == Convert.ToDecimal(defaultValue))
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == typeof(TimeOnly))
                {
                    if ((TimeOnly)primaryKeyVal! == (TimeOnly)defaultValue!)
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == typeof(DateOnly))
                {
                    if ((DateOnly)primaryKeyVal! == (DateOnly)defaultValue!)
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == typeof(DateTime))
                {
                    if (Convert.ToDateTime(primaryKeyVal) == Convert.ToDateTime(defaultValue))
                        return false;
                    else
                        return true;
                }
                else if (primaryKeyValType == typeof(string))
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
