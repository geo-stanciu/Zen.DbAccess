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

namespace Zen.DbAccess.Extensions
{
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

        private static void RefreshPropertiesIfEmpty(this DbModel dbModel)
        {
            if (dbModel.dbModel_properties != null && dbModel.dbModel_properties.Length > 0)
                return;

            Type classType = dbModel.GetType();
            dbModel.dbModel_properties = classType.GetProperties()
                .Where(x => x.PropertyType != typeof(DbSqlUpdateModel)
                    && x.PropertyType != typeof(DbSqlInsertModel)
                    //&& x.PropertyType != typeof(Avro.Schema)
                    && !HasDbModelPropertyIgnoreAttribute(dbModel, x))
                .ToArray();
        }

        public static PropertyInfo[]? GetDbModelProperties(this DbModel dbModel)
        {
            RefreshPropertiesIfEmpty(dbModel);
            return dbModel.dbModel_properties;
        }

        public static bool HasAuditIgnoreAttribute(this DbModel dbModel,  PropertyInfo propertyInfo)
        {
            object[] attrs = propertyInfo.GetCustomAttributes(true);

            if (attrs == null || attrs.Length == 0)
                return false;

            return attrs.Any(x => x is AuditIgnoreAttribute);
        }

        private static bool HasDbModelPropertyIgnoreAttribute(this DbModel dbModel, PropertyInfo propertyInfo)
        {
            object[] attrs = propertyInfo.GetCustomAttributes(true);

            if (attrs == null || attrs.Length == 0)
                return false;

            return attrs.Any(x => x is DbModelPropertyIgnoreAttribute);
        }

        private static bool IsPostgreSQLJsonDataType(this DbModel dbModel, DbConnection conn, PropertyInfo propertyInfo)
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

        private static bool IsPostgreSQLDateTimeDataType(this DbModel dbModel, DbConnection conn, PropertyInfo propertyInfo)
        {
            if (conn is NpgsqlConnection
                && (propertyInfo.PropertyType == typeof(DateTime) || propertyInfo.PropertyType == typeof(DateTime?)))
            {
                return true;
            }

            return false;
        }

        private static bool IsOracleClobDataType(this DbModel dbModel, DbConnection conn, PropertyInfo propertyInfo)
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

        private static async Task RefreshDbColumnsIfEmptyAsync(this DbModel dbModel, DbConnection conn, string table)
        {
            if (dbModel.dbModel_dbColumns != null && dbModel.dbModel_dbColumns.Count > 0)
                return;

            dbModel.dbModel_dbColumns = new List<string>();

            string sql = $"select * from {table} where 1 = -1";

            DataTable? dt = await sql.QueryDataTableAsync(conn);

            if (dt == null)
                throw new NullReferenceException(nameof(dt));

            foreach (DataColumn col in dt.Columns)
                dbModel.dbModel_dbColumns.Add(col.ColumnName.ToLower());
        }

        private static async Task ConstructUpdateQueryAsync(this DbModel dbModel, DbConnection conn, string table)
        {
            await RefreshDbColumnsIfEmptyAsync(dbModel, conn, table);

            StringBuilder sbUpdate = new StringBuilder();
            sbUpdate.Append($"update {table} set ");

            bool firstParam = true;

            RefreshPropertiesIfEmpty(dbModel);

            DeterminePrimaryKey(dbModel);

            if (dbModel.dbModel_properties == null)
                throw new NullReferenceException("dbModel_properties");

            if (dbModel.dbModel_dbColumns == null)
                throw new NullReferenceException("dbModel_dbColumns");

            if (dbModel.dbModel_primaryKey == null)
                throw new NullReferenceException("dbModel_primaryKey");

            SqlParam pkPrm = new SqlParam($"@p_{dbModel.dbModel_pkName}", dbModel.dbModel_primaryKey.GetValue(dbModel));

            PropertyInfo[] propertiesToUpdate = dbModel.dbModel_properties.Where(x => x.Name != dbModel.dbModel_pkName && dbModel.dbModel_dbColumns.Contains(x.Name)).ToArray();

            for (int i = 0; i < propertiesToUpdate.Length; i++)
            {
                PropertyInfo propertyInfo = propertiesToUpdate[i];

                if (firstParam)
                    firstParam = false;
                else
                    sbUpdate.Append(", ");

                string appendToParam;
                if (IsPostgreSQLJsonDataType(dbModel, conn, propertyInfo))
                    appendToParam = "::jsonb";
                else if (IsPostgreSQLDateTimeDataType(dbModel, conn, propertyInfo))
                    appendToParam = "::timestamp";
                else
                    appendToParam = string.Empty;

                sbUpdate.Append($" {propertyInfo.Name} = @p_{propertyInfo.Name}{appendToParam} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}", propertyInfo.GetValue(dbModel));

                if (IsOracleClobDataType(dbModel, conn, propertyInfo))
                    prm.isClob = true;

                dbModel.dbModel_sql_update.sql_parameters.Add(prm);
            }

            dbModel.dbModel_sql_update.sql_parameters.Add(pkPrm);

            sbUpdate.Append($" where {dbModel.dbModel_pkName} = @p_{dbModel.dbModel_pkName}");

            dbModel.dbModel_sql_update.sql_query = sbUpdate.ToString();
        }

        private static void RefreshParameterValuesForUpdate(this DbModel dbModel)
        {
            RefreshPropertiesIfEmpty(dbModel);

            if (dbModel.dbModel_properties == null)
                throw new NullReferenceException("dbModel_properties");

            for (int i = 0; i < dbModel.dbModel_properties.Length; i++)
            {
                PropertyInfo propertyInfo = dbModel.dbModel_properties[i];

                SqlParam? prm = null;
                if (propertyInfo.Name == dbModel.dbModel_pkName)
                    prm = dbModel.dbModel_sql_update.sql_parameters.LastOrDefault();
                else if (i > 0)
                    prm = dbModel.dbModel_sql_update.sql_parameters[i - 1];

                if (prm == null || prm.name != $"@p_{propertyInfo.Name}")
                    prm = dbModel.dbModel_sql_update.sql_parameters.FirstOrDefault(x => x.name == $"@p_{propertyInfo.Name}");

                if (prm != null)
                    prm.value = propertyInfo.GetValue(dbModel) ?? DBNull.Value;
            }
        }

        private static void DeterminePrimaryKey(this DbModel dbModel)
        {
            if (!string.IsNullOrEmpty(dbModel.dbModel_pkName))
                return;

            if (dbModel.dbModel_properties == null)
                throw new NullReferenceException("dbModel_properties");

            foreach (PropertyInfo propertyInfo in dbModel.dbModel_properties)
            {
                bool primaryKeyFound = false;
                object[] attrs = propertyInfo.GetCustomAttributes(true);

                foreach (object attr in attrs)
                {
                    if (attr is PrimaryKeyAttribute)
                    {
                        dbModel.dbModel_pkName = propertyInfo.Name;
                        dbModel.dbModel_primaryKey = propertyInfo;
                        primaryKeyFound = true;
                        break;
                    }
                }

                if (primaryKeyFound)
                    return;
            }

            if (string.IsNullOrEmpty(dbModel.dbModel_pkName))
                throw new Exception("There must be a property with the [PrimaryKey] attribute.");
        }

        private static async Task ConstructInsertQueryAsync(this DbModel dbModel, DbConnection conn, string table, bool insertPrimaryKeyColumn)
        {
            await RefreshDbColumnsIfEmptyAsync(dbModel, conn, table);
            RefreshPropertiesIfEmpty(dbModel);

            StringBuilder sbInsertValues = new StringBuilder();
            StringBuilder sbInsert = new StringBuilder();

            sbInsert.Append($"insert into {table} (");

            DeterminePrimaryKey(dbModel);

            if (dbModel.dbModel_properties == null)
                throw new NullReferenceException("dbModel_properties");

            if (dbModel.dbModel_dbColumns == null)
                throw new NullReferenceException("dbModel_dbColumns");

            bool firstParam = true;
            PropertyInfo[] propertiesToInsert;

            if (!insertPrimaryKeyColumn)
                propertiesToInsert = dbModel.dbModel_properties.Where(x => x.Name != dbModel.dbModel_pkName && dbModel.dbModel_dbColumns.Contains(x.Name)).ToArray();
            else
                propertiesToInsert = dbModel.dbModel_properties.Where(x => dbModel.dbModel_dbColumns.Contains(x.Name)).ToArray();

            for (int i = 0; i < propertiesToInsert.Length; i++)
            {
                PropertyInfo propertyInfo = propertiesToInsert[i];

                if (firstParam)
                    firstParam = false;
                else
                {
                    sbInsert.Append(", ");
                    sbInsertValues.Append(", ");
                }

                string appendToParam;
                if (IsPostgreSQLJsonDataType(dbModel, conn, propertyInfo))
                    appendToParam = "::jsonb";
                else if (IsPostgreSQLDateTimeDataType(dbModel, conn, propertyInfo))
                    appendToParam = "::timestamp";
                else
                    appendToParam = string.Empty;

                sbInsert.Append($" {propertyInfo.Name} ");
                sbInsertValues.Append($" @p_{propertyInfo.Name}{appendToParam} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}", propertyInfo.GetValue(dbModel));

                if (IsOracleClobDataType(dbModel, conn, propertyInfo))
                    prm.isClob = true;

                dbModel.dbModel_sql_insert.sql_parameters.Add(prm);
            }

            sbInsert.Append(") values (").Append(sbInsertValues).Append(")");

            if (!insertPrimaryKeyColumn)
            {
                if (conn is OracleConnection)
                {
                    if (!string.IsNullOrEmpty(dbModel.dbModel_pkName))
                        sbInsert.AppendLine($" returning {dbModel.dbModel_pkName} into @p_out_id ");
                    else
                        sbInsert.AppendLine($" returning {dbModel.dbModel_properties.FirstOrDefault()?.Name} into @p_out_id ");

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

                    SqlParam p_serial_id = new SqlParam($"@p_serial_id", dbModel.dbModel_pkName);
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
            RefreshPropertiesIfEmpty(dbModel);

            if (dbModel.dbModel_properties == null)
                throw new NullReferenceException("dbModel_properties");

            if (dbModel.dbModel_sql_insert == null)
                throw new NullReferenceException("dbModel_sql_insert");

            PropertyInfo[] propertiesToInsert;

            if (!insertPrimaryKeyColumn)
                propertiesToInsert = dbModel.dbModel_properties.Where(x => x.Name != dbModel.dbModel_pkName).ToArray();
            else
                propertiesToInsert = dbModel.dbModel_properties;

            for (int i = 0; i < propertiesToInsert.Length; i++)
            {
                PropertyInfo propertyInfo = propertiesToInsert[i];

                SqlParam? prm = dbModel.dbModel_sql_insert.sql_parameters[i];

                if (prm.name != $"@p_{propertyInfo.Name}")
                    prm = dbModel.dbModel_sql_insert.sql_parameters.FirstOrDefault(x => x.name == $"@p_{propertyInfo.Name}");

                if (prm != null)
                    prm.value = propertyInfo.GetValue(dbModel) ?? DBNull.Value;
            }
        }

        private static async Task<int> RunQueryAsync(this DbModel dbModel, DbConnection conn, string sql, List<SqlParam> parameters, bool insertPrimaryKeyColumn, bool isInsert = false)
        {
            int affected = 0;

            DeterminePrimaryKey(dbModel);

            using DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;

            DBUtils.AddParameters(cmd, parameters);

            if (isInsert)
            {
                if (conn is NpgsqlConnection || conn is SqlConnection || conn is SQLiteConnection)
                {
                    affected = 1;

                    if (!insertPrimaryKeyColumn)
                    {
                        long id = Convert.ToInt64((await cmd.ExecuteScalarAsync())!.ToString());

                        if (dbModel.dbModel_primaryKey != null)
                            dbModel.dbModel_primaryKey.SetValue(dbModel, id, null);
                    }
                    else
                    {
                        affected = await cmd.ExecuteNonQueryAsync();
                    }
                }
                else
                {
                    affected = await cmd.ExecuteNonQueryAsync();

                    if (!insertPrimaryKeyColumn)
                    {
                        foreach (DbParameter prm in cmd.Parameters)
                        {
                            if (prm.Direction == ParameterDirection.Output)
                            {
                                if (dbModel.dbModel_primaryKey != null && prm.Value != null && prm.Value != DBNull.Value)
                                {
                                    try
                                    {
                                        dbModel.dbModel_primaryKey.SetValue(dbModel, Convert.ToInt64(prm.Value), null);
                                    }
                                    catch
                                    {
                                        dbModel.dbModel_primaryKey.SetValue(dbModel, prm.Value, null);
                                    }
                                }

                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                affected = await cmd.ExecuteNonQueryAsync();
            }

            return affected;
        }

        public static void Save(this DbModel dbModel, string conn_str, string table, bool insertPrimaryKeyColumn = false)
        {
            SaveAsync(dbModel, DbModelSaveType.InsertUpdate, DbConnectionFactory.DefaultDbType, conn_str, table, insertPrimaryKeyColumn).Wait();
        }

        public static void Save(this DbModel dbModel, DbModelSaveType saveType, string conn_str, string table, bool insertPrimaryKeyColumn = false)
        {
            SaveAsync(dbModel, saveType, DbConnectionFactory.DefaultDbType, conn_str, table, insertPrimaryKeyColumn).Wait();
        }

        public static void Save(this DbModel dbModel, DbConnectionType dbtype, string conn_str, string table, bool insertPrimaryKeyColumn = false)
        {
            SaveAsync(dbModel, DbModelSaveType.InsertUpdate, dbtype, conn_str, table, insertPrimaryKeyColumn).Wait();
        }

        public static void Save(this DbModel dbModel, DbModelSaveType saveType, DbConnectionType dbtype, string conn_str, string table, bool insertPrimaryKeyColumn = false)
        {
            SaveAsync(dbModel, saveType, dbtype, conn_str, table, insertPrimaryKeyColumn).Wait();
        }

        /// <summary>
        /// Assums the first property is the primary key
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="table"></param>
        public static void Save(this DbModel dbModel, DbConnection conn, string table, bool insertPrimaryKeyColumn = false)
        {
            SaveAsync(dbModel, DbModelSaveType.InsertUpdate, conn, table, insertPrimaryKeyColumn).Wait();
        }

        /// <summary>
        /// Assums the first property is the primary key
        /// </summary>
        /// <param name="saveType"></param>
        /// <param name="conn"></param>
        /// <param name="table"></param>
        public static void Save(this DbModel dbModel, DbModelSaveType saveType, DbConnection conn, string table, bool insertPrimaryKeyColumn = false)
        {
            SaveAsync(dbModel, saveType, conn, table, insertPrimaryKeyColumn).Wait();
        }

        public static void Save(this DbModel dbModel, DbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false)
        {
            SaveAsync(dbModel, DbModelSaveType.InsertUpdate, dbConnectionFactory, table, insertPrimaryKeyColumn).Wait();
        }

        public static void Save(this DbModel dbModel, DbModelSaveType saveType, DbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false)
        {
            SaveAsync(dbModel, saveType, dbConnectionFactory, table, insertPrimaryKeyColumn).Wait();
        }

        public static async Task SaveAsync(this DbModel dbModel, string conn_str, string table, bool insertPrimaryKeyColumn = false)
        {
            await SaveAsync(dbModel, DbModelSaveType.InsertUpdate, DbConnectionFactory.DefaultDbType, conn_str, table, insertPrimaryKeyColumn);
        }

        public static async Task SaveAsync(this DbModel dbModel, DbModelSaveType saveType, string conn_str, string table, bool insertPrimaryKeyColumn = false)
        {
            await SaveAsync(dbModel, saveType, DbConnectionFactory.DefaultDbType, conn_str, table, insertPrimaryKeyColumn);
        }

        public static async Task SaveAsync(this DbModel dbModel, DbConnectionType dbtype, string conn_str, string table, bool insertPrimaryKeyColumn = false)
        {
            await SaveAsync(dbModel, DbModelSaveType.InsertUpdate, new DbConnectionFactory(dbtype, conn_str), table, insertPrimaryKeyColumn);
        }

        public static async Task SaveAsync(this DbModel dbModel, DbModelSaveType saveType, DbConnectionType dbtype, string conn_str, string table, bool insertPrimaryKeyColumn = false)
        {
            await SaveAsync(dbModel, saveType, new DbConnectionFactory(dbtype, conn_str), table, insertPrimaryKeyColumn);
        }

        public static async Task SaveAsync(this DbModel dbModel, DbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false)
        {
            using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
            await SaveAsync(dbModel, DbModelSaveType.InsertUpdate, conn, table, insertPrimaryKeyColumn);
            await conn.CloseAsync();
        }

        public static async Task SaveAsync(this DbModel dbModel, DbModelSaveType saveType, DbConnectionFactory dbConnectionFactory, string table, bool insertPrimaryKeyColumn = false)
        {
            using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
            await SaveAsync(dbModel, saveType, conn, table, insertPrimaryKeyColumn);
            await conn.CloseAsync();
        }

        /// <summary>
        /// Assums the first property is the primary key
        /// </summary>
        /// <param name="saveType"></param>
        /// <param name="conn"></param>
        /// <param name="table"></param>
        public static async Task SaveAsync(this DbModel dbModel, DbConnection conn, string table, bool insertPrimaryKeyColumn = false)
        {
            await SaveAsync(dbModel, DbModelSaveType.InsertUpdate, conn, table, insertPrimaryKeyColumn);
        }

        /// <summary>
        /// Assums the first property is the primary key
        /// </summary>
        /// <param name="saveType"></param>
        /// <param name="conn"></param>
        /// <param name="table"></param>
        public static async Task SaveAsync(this DbModel dbModel, DbModelSaveType saveType, DbConnection conn, string table, bool insertPrimaryKeyColumn = false)
        {
            if (saveType == DbModelSaveType.InsertUpdate && PrimaryKeyFieldHasValue(dbModel))
            {
                // we need to try tp update first since we have a value for the primary key field
                if (string.IsNullOrEmpty(dbModel.dbModel_sql_update.sql_query))
                    await ConstructUpdateQueryAsync(dbModel, conn, table);
                else
                    RefreshParameterValuesForUpdate(dbModel);

                // try to update
                int affected = await RunQueryAsync(dbModel, conn, dbModel.dbModel_sql_update.sql_query, dbModel.dbModel_sql_update.sql_parameters, insertPrimaryKeyColumn, false);

                if (affected > 0)
                    return;
            }

            if (string.IsNullOrEmpty(dbModel.dbModel_sql_insert.sql_query))
                await ConstructInsertQueryAsync(dbModel, conn, table, insertPrimaryKeyColumn);
            else
                RefreshParameterValuesForInsert(dbModel, insertPrimaryKeyColumn);

            // try to insert
            _ = await RunQueryAsync(dbModel, conn, dbModel.dbModel_sql_insert.sql_query, dbModel.dbModel_sql_insert.sql_parameters, insertPrimaryKeyColumn, true);
        }

        private static bool PrimaryKeyFieldHasValue(this DbModel dbModel)
        {
            RefreshPropertiesIfEmpty(dbModel);
            DeterminePrimaryKey(dbModel);

            if (dbModel.dbModel_properties == null)
                throw new NullReferenceException("dbModel_properties");

            PropertyInfo? primaryKeyProp = dbModel.dbModel_properties.Where(x => x.Name == dbModel.dbModel_pkName).FirstOrDefault();

            if (primaryKeyProp == null)
                return false;

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

            return false;
        }
    }
}
