using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Shared.Models;
using Zen.DbAccess.Shared.Attributes;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Utils;

namespace Zen.DbAccess.Extensions;

public static class DbModelExtensions
{
    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tint = typeof(int);

    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tintNull = typeof(int?);

    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tlong = typeof(long);

    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tlongNull = typeof(long?);

    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tbool = typeof(bool);

    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tboolNull = typeof(bool?);

    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tdecimal = typeof(decimal);

    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tdecimalNull = typeof(decimal?);

    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tdatetime = typeof(DateTime);

    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tdatetimeNull = typeof(DateTime?);

    [DbModelPropertyIgnore]
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    private static Type dbModel_tString = typeof(string);

    public static bool HasAuditIgnoreAttribute(this DbModel dbModel,  PropertyInfo propertyInfo)
    {
        object[] attrs = propertyInfo.GetCustomAttributes(true);

        if (attrs == null || attrs.Length == 0)
            return false;

        return attrs.Any(x => x is AuditIgnoreAttribute);
    }

    public static bool HasDbModelPropertyIgnoreAttribute(this DbModel dbModel, PropertyInfo propertyInfo)
    {
        object[] attrs = propertyInfo.GetCustomAttributes(true);

        if (attrs == null || attrs.Length == 0)
            return false;

        return attrs.Any(x => x is DbModelPropertyIgnoreAttribute);
    }

    public static bool IsPostgreSQLJsonDataType(this DbModel dbModel, DbConnection conn, PropertyInfo propertyInfo)
    {
        if (conn is NpgsqlConnection)
        {
            object[] attrs = propertyInfo.GetCustomAttributes(true);

            if (attrs == null || attrs.Length == 0)
                return false;

            return attrs.Any(x => x is JsonDbTypeAttribute);
        }

        return false;
    }

    public static bool IsOracleClobDataType(this DbModel dbModel, DbConnection conn, PropertyInfo propertyInfo)
    {
        if (conn is OracleConnection)
        {
            object[] attrs = propertyInfo.GetCustomAttributes(true);

            if (attrs == null || attrs.Length == 0)
                return false;

            return attrs.Any(x => x is ClobDbTypeAttribute || x is JsonDbTypeAttribute);
        }

        return false;
    }

    private static async Task RefreshDbColumnsIfEmptyAsync(this DbModel dbModel, DbConnection conn, DbTransaction? tx, string table)
    {
        if (dbModel.dbModel_dbColumns != null && dbModel.dbModel_dbColumns.Count > 0)
            return;

        dbModel.dbModel_dbColumns = new HashSet<string>();
        dbModel.dbModel_dbColumn_map = new Dictionary<string, PropertyInfo>();
        dbModel.dbModel_prop_map = new Dictionary<string, string>();

        string sql = $"select * from {table} where 1 = -1";

        DataTable? dt = await sql.QueryDataTableAsync(conn, tx);

        if (dt == null)
            throw new NullReferenceException(nameof(dt));

        foreach (DataColumn col in dt.Columns)
        {
            var p = ColumnNameMapUtils.GetModelPropertyForDbColumn(dbModel.GetType(), col.ColumnName);

            if (p == null)
                continue;

            dbModel.dbModel_dbColumns.Add(col.ColumnName);
            dbModel.dbModel_dbColumn_map[col.ColumnName] = p;
            dbModel.dbModel_prop_map[p.Name] = col.ColumnName;
        }
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

    public static List<PropertyInfo> GetPropertiesToInsert(this DbModel dbModel, bool insertPrimaryKeyColumn)
    {
        if (dbModel.dbModel_dbColumns == null)
            throw new NullReferenceException("dbModel_dbColumns");

        if (dbModel.dbModel_dbColumn_map == null)
            throw new NullReferenceException("dbModel_dbColumn_map");

        if (dbModel.dbModel_primaryKey_dbColumns == null)
            throw new NullReferenceException("dbModel_primaryKey_dbColumns");

        return dbModel.dbModel_dbColumns
            .Where(x => dbModel.dbModel_dbColumn_map.ContainsKey(x) && (insertPrimaryKeyColumn || (!insertPrimaryKeyColumn && !dbModel.dbModel_primaryKey_dbColumns.Contains(x))))
            .Select(x => dbModel.dbModel_dbColumn_map[x])
            .ToList();
    }
    
    private static async Task ConstructUpdateQueryAsync(this DbModel dbModel, DbConnection conn, DbTransaction? tx, string table)
    {
        await RefreshDbColumnsIfEmptyAsync(dbModel, conn, tx, table);

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

            string appendToParam;
            if (IsPostgreSQLJsonDataType(dbModel, conn, propertyInfo))
                appendToParam = "::jsonb";
            else
                appendToParam = string.Empty;

            sbUpdate.Append($" {dbModel.dbModel_prop_map[propertyInfo.Name]} = @p_{propertyInfo.Name}{appendToParam} ");

            SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}", propertyInfo.GetValue(dbModel));

            if (IsOracleClobDataType(dbModel, conn, propertyInfo))
                prm.isClob = true;

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

            SqlParam? prm = null;
            if (dbModel.dbModel_primaryKey_dbColumns!.Any(x => x == dbCol))
            {
                for (int j = dbModel.dbModel_sql_update.sql_parameters.Count - 1; j >= 0; j--)
                {
                    var updateParameter = dbModel.dbModel_sql_update.sql_parameters[j];

                    if (updateParameter.name == $"@p_{propertyInfo.Name}")
                    {
                        prm = updateParameter;
                        break;
                    }
                }
            }
            else if (i > 0)
            {
                prm = dbModel.dbModel_sql_update.sql_parameters[i - 1];
            }

            if (prm == null || prm.name != $"@p_{propertyInfo.Name}")
                prm = dbModel.dbModel_sql_update.sql_parameters.FirstOrDefault(x => x.name == $"@p_{propertyInfo.Name}");

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

            object[] attrs = prop.GetCustomAttributes(true);

            if (attrs == null || attrs.Length == 0)
                continue;

            if (attrs.Any(x => x is PrimaryKeyAttribute))
                dbModel.dbModel_primaryKey_dbColumns.Add(dbCol);
        }

        if (dbModel.dbModel_primaryKey_dbColumns.Count == 0)
            throw new Exception("There must be a property with the [PrimaryKey] attribute.");
    }

    public static Task RefreshDbColumnsAndModelPropertiesAsync(this DbModel dbModel, DbConnection conn, string table)
    {
        return RefreshDbColumnsAndModelPropertiesAsync(dbModel, conn, tx: null, table);
    }

    public static async Task RefreshDbColumnsAndModelPropertiesAsync(this DbModel dbModel, DbConnection conn, DbTransaction? tx, string table)
    {
        await RefreshDbColumnsIfEmptyAsync(dbModel, conn, tx, table);
        DeterminePrimaryKey(dbModel);
    }

    private static async Task ConstructInsertQueryAsync(
        this DbModel dbModel,
        DbModelSaveType saveType,
        DbConnection conn,
        DbTransaction? tx, 
        string table, 
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey = "")
    {
        await RefreshDbColumnsIfEmptyAsync(dbModel, conn, tx, table);

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

        List<PropertyInfo> propertiesToInsert = GetPropertiesToInsert(dbModel, insertPrimaryKeyColumn);

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
                && !string.IsNullOrEmpty(sequence2UseForPrimaryKey)
                && dbModel.dbModel_primaryKey_dbColumns.Any(x => x == dbCol))
            {
                if (conn is OracleConnection)
                {
                    sbInsert.Append($" {dbCol} ");
                    sbInsertValues.Append($"{sequence2UseForPrimaryKey}.nextval");

                    continue;
                }
            }

            string appendToParam;
            if (IsPostgreSQLJsonDataType(dbModel, conn, propertyInfo))
                appendToParam = "::jsonb";
            else
                appendToParam = string.Empty;

            sbInsert.Append($" {dbCol} ");
            sbInsertValues.Append($" @p_{propertyInfo.Name}{appendToParam} ");

            SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}", propertyInfo.GetValue(dbModel));

            if (IsOracleClobDataType(dbModel, conn, propertyInfo))
                prm.isClob = true;

            dbModel.dbModel_sql_insert.sql_parameters.Add(prm);
        }

        sbInsert.Append(") values (").Append(sbInsertValues).Append(")");

        if (!insertPrimaryKeyColumn && saveType != DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn)
        {
            if (conn is OracleConnection)
            {
                if (dbModel.dbModel_primaryKey_dbColumns.Count == 1)
                    sbInsert.AppendLine($" returning {dbModel.dbModel_primaryKey_dbColumns[0]} into @p_out_id ");
                else
                    sbInsert.AppendLine($" returning {dbModel.dbModel_prop_map[propertiesToInsert.First().Name]} into @p_out_id ");

                SqlParam prm = new SqlParam($"@p_out_id") { paramDirection = ParameterDirection.Output };
                dbModel.dbModel_sql_insert.sql_parameters.Add(prm);
            }
            else if (conn is SqlConnection)
            {
                sbInsert.Append("; select SCOPE_IDENTITY() as ROW_ID;");
            }
            else if (conn is NpgsqlConnection)
            {
                sbInsert.Append("; select currval(pg_get_serial_sequence(@p_serial_table, @p_serial_id));");

                SqlParam p_serial_table = new SqlParam($"@p_serial_table", table);
                dbModel.dbModel_sql_insert.sql_parameters.Add(p_serial_table);

                SqlParam p_serial_id = new SqlParam($"@p_serial_id", dbModel.dbModel_primaryKey_dbColumns.Any() ? dbModel.dbModel_primaryKey_dbColumns[0] : dbModel.dbModel_prop_map[propertiesToInsert.First().Name]);
                dbModel.dbModel_sql_insert.sql_parameters.Add(p_serial_id);
            }
            else if (conn is SQLiteConnection)
            {
                sbInsert.Append("; select last_insert_rowid() as ROW_ID;");
            }
        }

        dbModel.dbModel_sql_insert.sql_query = sbInsert.ToString();
    }

    private static void RefreshParameterValuesForInsert(this DbModel dbModel, bool insertPrimaryKeyColumn)
    {
        if (dbModel.dbModel_sql_insert == null)
            throw new NullReferenceException("dbModel_sql_insert");

        List<PropertyInfo> propertiesToInsert = GetPropertiesToInsert(dbModel, insertPrimaryKeyColumn);

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
        DbConnection conn,
        DbTransaction? tx, 
        string sql, 
        List<SqlParam> parameters, 
        bool insertPrimaryKeyColumn, 
        bool isInsert = false)
    {
        int affected = 0;

        DeterminePrimaryKey(dbModel);

        using DbCommand cmd = conn.CreateCommand();

        if (tx != null && cmd.Transaction == null)
            cmd.Transaction = tx;

        cmd.CommandText = sql;

        DBUtils.AddParameters(cmd, parameters);

        if (!isInsert)
        {
            affected = await cmd.ExecuteNonQueryAsync();
            return affected;
        }

        if (conn is OracleConnection)
        {
            affected = await cmd.ExecuteNonQueryAsync();

            if (!insertPrimaryKeyColumn && saveType != DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn)
            {
                foreach (DbParameter prm in cmd.Parameters)
                {
                    if (prm.Direction != ParameterDirection.Output)
                        continue;

                    if (dbModel.dbModel_primaryKey_dbColumns != null && dbModel.dbModel_primaryKey_dbColumns.Any() && prm.Value != null && prm.Value != DBNull.Value)
                    {
                        var pkProp = dbModel.dbModel_dbColumn_map![dbModel.dbModel_primaryKey_dbColumns[0]];

                        try
                        {
                            pkProp.SetValue(dbModel, Convert.ToInt64(prm.Value), null);
                        }
                        catch
                        {
                            pkProp.SetValue(dbModel, prm.Value, null);
                        }
                    }

                    break;
                }
            }

            return affected;
        }

        affected = 1;

        if (!insertPrimaryKeyColumn && saveType != DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn)
        {
            long id = Convert.ToInt64((await cmd.ExecuteScalarAsync())!.ToString());

            if (dbModel.dbModel_primaryKey_dbColumns != null && dbModel.dbModel_primaryKey_dbColumns.Any())
            {
                var pkProp = dbModel.dbModel_dbColumn_map![dbModel.dbModel_primaryKey_dbColumns[0]];
                pkProp.SetValue(dbModel, id, null);
            }
        }
        else
        {
            affected = await cmd.ExecuteNonQueryAsync();
        }

        return affected;
    }

    public static void Save(this DbModel dbModel, string conn_str, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, DbModelSaveType.InsertUpdate, DbConnectionFactory.DefaultDbType, conn_str, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbModelSaveType saveType, string conn_str, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, saveType, DbConnectionFactory.DefaultDbType, conn_str, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbConnectionType dbtype, string conn_str, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, DbModelSaveType.InsertUpdate, dbtype, conn_str, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbModelSaveType saveType, DbConnectionType dbtype, string conn_str, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, saveType, dbtype, conn_str, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbConnection conn, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, DbModelSaveType.InsertUpdate, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbConnection conn, DbTransaction? tx, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, DbModelSaveType.InsertUpdate, conn, tx, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbModelSaveType saveType, DbConnection conn, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, saveType, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbModelSaveType saveType, DbConnection conn, DbTransaction? tx, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, saveType, conn, tx, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, DbModelSaveType.InsertUpdate, dbConnectionFactory, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static void Save(this DbModel dbModel, DbModelSaveType saveType, DbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        SaveAsync(dbModel, saveType, dbConnectionFactory, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey).Wait();
    }

    public static async Task SaveAsync(this DbModel dbModel, string conn_str, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        await SaveAsync(dbModel, DbModelSaveType.InsertUpdate, DbConnectionFactory.DefaultDbType, conn_str, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
    }

    public static async Task SaveAsync(this DbModel dbModel, DbModelSaveType saveType, string conn_str, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        await SaveAsync(dbModel, saveType, DbConnectionFactory.DefaultDbType, conn_str, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
    }

    public static async Task SaveAsync(this DbModel dbModel, DbConnectionType dbtype, string conn_str, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        await SaveAsync(dbModel, DbModelSaveType.InsertUpdate, new DbConnectionFactory(dbtype, conn_str), table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
    }

    public static async Task SaveAsync(this DbModel dbModel, DbModelSaveType saveType, DbConnectionType dbtype, string conn_str, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        await SaveAsync(dbModel, saveType, new DbConnectionFactory(dbtype, conn_str), table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
    }

    public static async Task SaveAsync(this DbModel dbModel, DbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        await SaveAsync(dbModel, DbModelSaveType.InsertUpdate, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
        await conn.CloseAsync();
    }

    public static async Task SaveAsync(this DbModel dbModel, DbModelSaveType saveType, DbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false, string sequence2UseForPrimaryKey = "")
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        await SaveAsync(dbModel, saveType, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
        await conn.CloseAsync();
    }

    public static Task SaveAsync(
        this DbModel dbModel,
        DbConnection conn,
        string table,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "")
    {
        return SaveAsync(
            dbModel,
            conn,
            tx: null,
            table,
            insertPrimaryKeyColumn,
            sequence2UseForPrimaryKey);
    }

    public static async Task SaveAsync(
        this DbModel dbModel, 
        DbConnection conn,
        DbTransaction? tx, 
        string table, 
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "")
    {
        await SaveAsync(
            dbModel, 
            DbModelSaveType.InsertUpdate, 
            conn,
            tx,
            table, 
            insertPrimaryKeyColumn,
            sequence2UseForPrimaryKey
        );
    }

    public static Task SaveAsync(
        this DbModel dbModel,
        DbModelSaveType saveType,
        DbConnection conn,
        string table,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "")
    {
        return SaveAsync(
            dbModel,
            saveType,
            conn,
            tx: null,
            table,
            insertPrimaryKeyColumn,
            sequence2UseForPrimaryKey);
    }

    public static async Task SaveAsync(
        this DbModel dbModel,
        DbModelSaveType saveType, 
        DbConnection conn,
        DbTransaction? tx, 
        string table, 
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "")
    {
        if (saveType == DbModelSaveType.InsertUpdate && PrimaryKeyFieldsHaveValues(dbModel))
        {
            // we need to try tp update first since we have a value for the primary key field
            if (string.IsNullOrEmpty(dbModel.dbModel_sql_update.sql_query))
                await ConstructUpdateQueryAsync(dbModel, conn, tx, table);
            else
                RefreshParameterValuesForUpdate(dbModel);

            // try to update
            int affected = await RunQueryAsync(
                dbModel,
                saveType,
                conn, 
                tx,
                dbModel.dbModel_sql_update.sql_query, 
                dbModel.dbModel_sql_update.sql_parameters, 
                insertPrimaryKeyColumn,
                false
            );

            if (affected > 0)
                return;
        }

        if (string.IsNullOrEmpty(dbModel.dbModel_sql_insert.sql_query))
            await ConstructInsertQueryAsync(dbModel, saveType, conn, tx, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
        else
            RefreshParameterValuesForInsert(dbModel, insertPrimaryKeyColumn);

        // try to insert
        _ = await RunQueryAsync(
            dbModel,
            saveType,
            conn, 
            tx,
            dbModel.dbModel_sql_insert.sql_query, 
            dbModel.dbModel_sql_insert.sql_parameters, 
            insertPrimaryKeyColumn,
            true
        );
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
