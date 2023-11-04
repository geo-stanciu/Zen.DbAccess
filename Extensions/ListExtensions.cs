using Newtonsoft.Json;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Shared.ContractResolvers;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Shared.Models;

namespace Zen.DbAccess.Extensions;

public static class ListExtensions
{
    public static async Task SaveAllAsync<T>(
        this List<T> list,
        string conn_str,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false) where T : DbModel
    {
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, DbConnectionFactory.DefaultDbType, conn_str, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
    }

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        DbModelSaveType dbModelSaveType,
        string conn_str,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false) where T : DbModel
    {
        await list.SaveAllAsync(dbModelSaveType, DbConnectionFactory.DefaultDbType, conn_str, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
    }

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        DbConnectionType dbtype,
        string conn_str,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false) where T : DbModel
    {
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, new DbConnectionFactory(dbtype, conn_str), table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
    }

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        DbModelSaveType dbModelSaveType,
        DbConnectionType dbtype,
        string conn_str,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false) where T : DbModel
    {
        await list.SaveAllAsync(dbModelSaveType, new DbConnectionFactory(dbtype, conn_str), table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
    }

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        DbConnectionFactory dbConnectionFactory,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false) where T : DbModel
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
        await conn.CloseAsync();
    }

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        DbModelSaveType dbModelSaveType,
        DbConnectionFactory dbConnectionFactory,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false) where T : DbModel
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        await list.SaveAllAsync(dbModelSaveType, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
        await conn.CloseAsync();
    }

    public static Task BulkInsertAsync<T>(
        this List<T> list,
        DbConnection conn,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        return BulkInsertAsync<T>(
            list,
            conn,
            tx: null,
            table,
            runAllInTheSameTransaction,
            insertPrimaryKeyColumn,
            sequence2UseForPrimaryKey);
    }

    public static async Task BulkInsertAsync<T>(
        this List<T> list, 
        DbConnection conn,
        DbTransaction? tx,
        string table, 
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        if (runAllInTheSameTransaction && tx == null)
            tx = await conn.BeginTransactionAsync();

        try
        {
            T? firstModel = list.FirstOrDefault();

            if (firstModel == null)
                throw new NullReferenceException(nameof(firstModel));

            await firstModel.RefreshDbColumnsAndModelPropertiesAsync(conn, tx, table);

            PropertyInfo[] modelProps = firstModel.dbModel_properties
                ?? throw new NullReferenceException(nameof(modelProps));

            List<string> dbColumns = firstModel.dbModel_dbColumns
                ?? throw new NullReferenceException(nameof(dbColumns));

            string pkName = firstModel.dbModel_pkName
                ?? throw new NullReferenceException(nameof(pkName));

            PropertyInfo primaryKey = firstModel.dbModel_primaryKey
                ?? throw new NullReferenceException(nameof(primaryKey));

            PropertyInfo[] propertiesToInsert = modelProps.Where(x => dbColumns.Contains(x.Name)).ToArray();

            int offset = 0;
            int take = Math.Min(list.Count - offset, 256);

            while (offset < list.Count)
            {
                List<T> batch = list.Skip(offset).Take(take).ToList();
                Tuple<string, SqlParam[]> preparedQuery = PrepareBulkInsertBatch(
                    batch, 
                    conn,
                    tx,
                    table, 
                    dbColumns, 
                    pkName, 
                    propertiesToInsert, 
                    insertPrimaryKeyColumn, 
                    sequence2UseForPrimaryKey);

                string sql = preparedQuery.Item1;
                SqlParam[] sqlParams = preparedQuery.Item2;

                await sql.ExecuteScalarAsync(conn, tx, sqlParams);

                offset += batch.Count;
            }
        }
        catch
        {
            if (tx != null)
            {
                try
                {
                    await tx.RollbackAsync();
                }
                catch { }
            }

            throw;
        }

        if (tx != null)
            await tx.CommitAsync();
    }

    public static Task SaveAllAsync<T>(
        this List<T> list,
        DbModelSaveType dbModelSaveType,
        DbConnection conn,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        return SaveAllAsync<T>(
            list,
            dbModelSaveType,
            conn,
            tx: null,
            table,
            runAllInTheSameTransaction,
            insertPrimaryKeyColumn,
            sequence2UseForPrimaryKey);
    }

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        DbModelSaveType dbModelSaveType,
        DbConnection conn,
        DbTransaction? tx,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        if (dbModelSaveType == DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn)
        {
            await BulkInsertAsync<T>(list, conn, tx, table, runAllInTheSameTransaction, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
            return;
        }

        Type classType = typeof(T);

        PropertyInfo[] properties = classType.GetProperties();
        PropertyInfo sqlUpdateProp = properties.FirstOrDefault(x => x.PropertyType == typeof(DbSqlUpdateModel))
            ?? throw new NullReferenceException(nameof(sqlUpdateProp));

        PropertyInfo sqlInsertProp = properties.FirstOrDefault(x => x.PropertyType == typeof(DbSqlInsertModel))
        ?? throw new NullReferenceException(nameof(sqlInsertProp));

        PropertyInfo modelPropertiesProp = properties.FirstOrDefault(x => x.Name == "dbModel_properties")
            ?? throw new NullReferenceException(nameof(modelPropertiesProp));

        PropertyInfo dbColumnsProp = properties.FirstOrDefault(x => x.Name == "dbModel_dbColumns")
            ?? throw new NullReferenceException(nameof(dbColumnsProp));

        PropertyInfo pkNameProp = properties.FirstOrDefault(x => x.Name == "dbModel_pkName")
            ?? throw new NullReferenceException(nameof(pkNameProp));

        PropertyInfo primaryKeyProp = properties.FirstOrDefault(x => x.Name == "dbModel_primaryKey")
            ?? throw new NullReferenceException(nameof(primaryKeyProp));


        if (runAllInTheSameTransaction && tx == null)
            tx = await conn.BeginTransactionAsync();

        try
        {
            T? firstModel = list.FirstOrDefault();

            if (firstModel == null)
                throw new NullReferenceException(nameof(firstModel));

            await firstModel.SaveAsync(dbModelSaveType, conn, tx, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);

            DbSqlUpdateModel sql_update = sqlUpdateProp.GetValue(firstModel) as DbSqlUpdateModel
                ?? throw new NullReferenceException(nameof(sql_update));

            DbSqlInsertModel sql_insert = sqlInsertProp.GetValue(firstModel) as DbSqlInsertModel
                ?? throw new NullReferenceException(nameof(sql_insert));

            PropertyInfo[] modelProps = modelPropertiesProp.GetValue(firstModel) as PropertyInfo[]
                ?? throw new NullReferenceException(nameof(modelProps));

            List<string> dbColumns = dbColumnsProp.GetValue(firstModel) as List<string>
                ?? throw new NullReferenceException(nameof(dbColumns));

            string pkName = pkNameProp.GetValue(firstModel) as string
                ?? throw new NullReferenceException(nameof(pkName));

            PropertyInfo primaryKey = primaryKeyProp.GetValue(firstModel) as PropertyInfo
                ?? throw new NullReferenceException(nameof(primaryKey));

            for (int i = 1; i < list.Count; i++)
            {
                T model = list[i];

                if (model == null)
                    continue;

                // setez sql-urile si params (se face refresh cu valorile corespunzatoare la Save)
                sqlUpdateProp.SetValue(model, sql_update, null);
                sqlInsertProp.SetValue(model, sql_insert, null);
                modelPropertiesProp.SetValue(model, modelProps, null);
                dbColumnsProp.SetValue(model, dbColumns, null);
                pkNameProp.SetValue(model, pkName, null);
                primaryKeyProp.SetValue(model, primaryKey, null);

                await model.SaveAsync(dbModelSaveType, conn, tx, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
            }
        }
        catch
        {
            if (tx != null)
            {
                try
                {
                    await tx.RollbackAsync();
                }
                catch { }
            }

            throw;
        }

        if (tx != null)
            await tx.CommitAsync();
    }

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch<T>(
        List<T> list,
        DbConnection conn,
        string table,
        List<string> dbColumns,
        string pkName,
        PropertyInfo[] propertiesToInsert,
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey) where T : DbModel
    {
        return PrepareBulkInsertBatch<T>(
            list,
            conn,
            tx: null,
            table,
            dbColumns,
            pkName,
            propertiesToInsert,
            insertPrimaryKeyColumn,
            sequence2UseForPrimaryKey);
    }

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch<T>(
        List<T> list,
        DbConnection conn,
        DbTransaction? tx,
        string table,
        List<string> dbColumns,
        string pkName,
        PropertyInfo[] propertiesToInsert,
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey) where T : DbModel
    {
        if (conn is OracleConnection)
        {
            if (string.IsNullOrEmpty(pkName))
            {
                return PrepareBulkInsertBatch4Oracle<T>(
                    list,
                    conn,
                    table,
                    dbColumns,
                    propertiesToInsert);
            }

            return PrepareBulkInsertBatch4OracleWithSequence<T>(
                list,
                conn,
                table,
                dbColumns,
                pkName,
                propertiesToInsert,
                insertPrimaryKeyColumn,
                sequence2UseForPrimaryKey);
        }
        else if (conn is NpgsqlConnection)
        {
            if (string.IsNullOrEmpty(pkName))
            {
                return PrepareBulkInsertBatch4Postgresql<T>(
                    list,
                    conn,
                    table,
                    propertiesToInsert);
            }

            return PrepareBulkInsertBatch4PostgresqlWithSequence<T>(
                list,
                conn,
                table,
                dbColumns,
                pkName,
                propertiesToInsert,
                insertPrimaryKeyColumn);
        }
        else if (conn is SQLiteConnection)
        {
            if (string.IsNullOrEmpty(pkName))
            {
                return PrepareBulkInsertBatch4Sqlite<T>(
                    list,
                    conn,
                    table,
                    propertiesToInsert);
            }

            return PrepareBulkInsertBatch4Sqlite<T>(
                list,
                conn,
                table,
                dbColumns,
                pkName,
                propertiesToInsert,
                insertPrimaryKeyColumn);
        }
        else if (conn is SqlConnection)
        {
            if (string.IsNullOrEmpty(pkName))
            {
                return PrepareBulkInsertBatch4SqlServer<T>(
                    list,
                    table,
                    propertiesToInsert);
            }

            return PrepareBulkInsertBatch4SqlServerWithSequence<T>(
                list,
                table,
                dbColumns,
                pkName,
                propertiesToInsert,
                insertPrimaryKeyColumn);
        }
        else
        {
            throw new NotImplementedException($"PrepareBulkInsertBatch for {conn.GetType()}");
        }
    }

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch4Oracle<T>(
        List<T> list,
        DbConnection conn,
        string table,
        List<string> dbColumns,
        PropertyInfo[] propertiesToInsert) where T : DbModel
    {
        int k = -1;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"INSERT ALL");

        T? firstModel = list.FirstOrDefault();

        foreach (T model in list)
        {
            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            sbInsert.Append($"INTO {table} (");

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

                sbInsert.Append($" {propertyInfo.Name} ");
                sbInsertValues.Append($" @p_{propertyInfo.Name}_{k} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}_{k}", propertyInfo.GetValue(model));

                if (firstModel != null && firstModel.IsOracleClobDataType(conn, propertyInfo))
                    prm.isClob = true;

                insertParams.Add(prm);
            }

            sbInsert.Append(") VALUES (").Append(sbInsertValues).AppendLine(")");
        }

        sbInsert.AppendLine("SELECT 1 FROM dual");

        return new Tuple<string, SqlParam[]>(sbInsert.ToString(), insertParams.ToArray());
    }

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch4OracleWithSequence<T>(
        List<T> list,
        DbConnection conn,
        string table,
        List<string> dbColumns,
        string pkName,
        PropertyInfo[] propertiesToInsert,
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey) where T : DbModel
    {
        int k = -1;
        bool firstParam = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine("BEGIN");

        T? firstModel = list.FirstOrDefault();

        foreach (T model in list)
        {
            k++;
            firstParam = true;

            sbInsert.Append($"INSERT INTO {table} (");
            StringBuilder sbInsertValues = new StringBuilder();

            for (int i = 0; i < propertiesToInsert.Length; i++)
            {
                PropertyInfo propertyInfo = propertiesToInsert[i];

                if (!insertPrimaryKeyColumn
                    && string.IsNullOrEmpty(sequence2UseForPrimaryKey)
                    && propertyInfo.Name == pkName)
                {
                    continue;
                }

                if (firstParam)
                    firstParam = false;
                else
                {
                    sbInsert.Append(", ");
                    sbInsertValues.Append(", ");
                }

                if (!insertPrimaryKeyColumn
                    && !string.IsNullOrEmpty(sequence2UseForPrimaryKey)
                    && propertyInfo.Name == pkName
                    && dbColumns.Contains(propertyInfo.Name))
                {
                    sbInsert.Append($" {propertyInfo.Name} ");
                    sbInsertValues.Append($"{sequence2UseForPrimaryKey}.nextval");

                    continue;
                }

                sbInsert.Append($" {propertyInfo.Name} ");
                sbInsertValues.Append($" @p_{propertyInfo.Name}_{k} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}_{k}", propertyInfo.GetValue(model));

                if (firstModel != null && firstModel.IsOracleClobDataType(conn, propertyInfo))
                    prm.isClob = true;

                insertParams.Add(prm);
            }

            sbInsert
                .Append(") VALUES (")
                .Append(sbInsertValues)
                .AppendLine(");");
        }

        sbInsert.AppendLine("END;");

        return new Tuple<string, SqlParam[]>(sbInsert.ToString(), insertParams.ToArray());
    }

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch4Sqlite<T>(
        List<T> list,
        DbConnection conn,
        string table,
        PropertyInfo[] propertiesToInsert) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T? firstModel = list.FirstOrDefault();

        foreach (T model in list)
        {
            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            for (int i = 0; i < propertiesToInsert.Length; i++)
            {
                PropertyInfo propertyInfo = propertiesToInsert[i];

                if (firstParam)
                {
                    firstParam = false;
                }
                else
                {
                    if (firstRow)
                        sbInsert.Append(", ");

                    sbInsertValues.Append(", ");
                }

                if (firstRow)
                    sbInsert.Append($" {propertyInfo.Name} ");

                sbInsertValues.Append($" @p_{propertyInfo.Name}_{k} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}_{k}", propertyInfo.GetValue(model));

                if (firstModel != null && firstModel.IsOracleClobDataType(conn, propertyInfo))
                    prm.isClob = true;

                insertParams.Add(prm);
            }

            if (firstRow)
            {
                firstRow = false;
                sbInsert
                    .AppendLine(") values ")
                    .Append(" (")
                    .Append(sbInsertValues).AppendLine(")");
            }
            else
            {
                sbInsert.Append(", (").Append(sbInsertValues).AppendLine(")");
            }
        }

        return new Tuple<string, SqlParam[]>(sbInsert.ToString(), insertParams.ToArray());
    }

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch4Sqlite<T>(
        List<T> list,
        DbConnection conn,
        string table,
        List<string> dbColumns,
        string pkName,
        PropertyInfo[] propertiesToInsert,
        bool insertPrimaryKeyColumn) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T? firstModel = list.FirstOrDefault();

        foreach (T model in list)
        {
            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            for (int i = 0; i < propertiesToInsert.Length; i++)
            {
                PropertyInfo propertyInfo = propertiesToInsert[i];

                if (firstParam)
                {
                    firstParam = false;
                }
                else
                {
                    if (firstRow)
                        sbInsert.Append(", ");

                    sbInsertValues.Append(", ");
                }

                if (!insertPrimaryKeyColumn
                    && propertyInfo.Name == pkName
                    && dbColumns.Contains(propertyInfo.Name))
                {
                    if (firstRow)
                        sbInsert.Append($" {propertyInfo.Name} ");

                    sbInsertValues.Append($" null ");

                    continue;
                }

                if (firstRow)
                    sbInsert.Append($" {propertyInfo.Name} ");

                sbInsertValues.Append($" @p_{propertyInfo.Name}_{k} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}_{k}", propertyInfo.GetValue(model));

                if (firstModel != null && firstModel.IsOracleClobDataType(conn, propertyInfo))
                    prm.isClob = true;

                insertParams.Add(prm);
            }

            if (firstRow)
            {
                firstRow = false;
                sbInsert
                    .AppendLine(") values ")
                    .Append(" (")
                    .Append(sbInsertValues).AppendLine(")");
            }
            else
            {
                sbInsert.Append(", (").Append(sbInsertValues).AppendLine(")");
            }
        }

        return new Tuple<string, SqlParam[]>(sbInsert.ToString(), insertParams.ToArray());
    }

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch4Postgresql<T>(
        List<T> list,
        DbConnection conn,
        string table,
        PropertyInfo[] propertiesToInsert) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T? firstModel = list.FirstOrDefault();

        foreach (T model in list)
        {
            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            for (int i = 0; i < propertiesToInsert.Length; i++)
            {
                PropertyInfo propertyInfo = propertiesToInsert[i];

                if (firstParam)
                {
                    firstParam = false;
                }
                else
                {
                    if (firstRow)
                        sbInsert.Append(", ");

                    sbInsertValues.Append(", ");
                }

                if (firstRow)
                    sbInsert.Append($" {propertyInfo.Name} ");

                string appendToParam;
                if (firstModel != null && firstModel.IsPostgreSQLJsonDataType(conn, propertyInfo))
                    appendToParam = "::jsonb";
                else
                    appendToParam = string.Empty;

                sbInsertValues.Append($" @p_{propertyInfo.Name}_{k}{appendToParam} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}_{k}", propertyInfo.GetValue(model));

                insertParams.Add(prm);
            }

            if (firstRow)
            {
                firstRow = false;
                sbInsert
                    .AppendLine(") values ")
                    .Append(" (")
                    .Append(sbInsertValues).AppendLine(")");
            }
            else
            {
                sbInsert.Append(", (").Append(sbInsertValues).AppendLine(")");
            }
        }

        return new Tuple<string, SqlParam[]>(sbInsert.ToString(), insertParams.ToArray());
    }

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch4PostgresqlWithSequence<T>(
        List<T> list,
        DbConnection conn,
        string table,
        List<string> dbColumns,
        string pkName,
        PropertyInfo[] propertiesToInsert,
        bool insertPrimaryKeyColumn) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T? firstModel = list.FirstOrDefault();

        foreach (T model in list)
        {
            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            for (int i = 0; i < propertiesToInsert.Length; i++)
            {
                PropertyInfo propertyInfo = propertiesToInsert[i];

                if (firstParam)
                {
                    firstParam = false;
                }
                else
                {
                    if (firstRow)
                        sbInsert.Append(", ");

                    sbInsertValues.Append(", ");
                }

                if (!insertPrimaryKeyColumn
                    && propertyInfo.Name == pkName
                    && dbColumns.Contains(propertyInfo.Name))
                {
                    if (firstRow)
                        sbInsert.Append($" {propertyInfo.Name} ");

                    sbInsertValues.Append($" default ");

                    continue;
                }

                if (firstRow)
                    sbInsert.Append($" {propertyInfo.Name} ");

                string appendToParam;
                if (firstModel != null && firstModel.IsPostgreSQLJsonDataType(conn, propertyInfo))
                    appendToParam = "::jsonb";
                else
                    appendToParam = string.Empty;

                sbInsertValues.Append($" @p_{propertyInfo.Name}_{k}{appendToParam} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}_{k}", propertyInfo.GetValue(model));

                insertParams.Add(prm);
            }

            if (firstRow)
            {
                firstRow = false;
                sbInsert
                    .AppendLine(") values ")
                    .Append(" (")
                    .Append(sbInsertValues).AppendLine(")");
            }
            else
            {
                sbInsert.Append(", (").Append(sbInsertValues).AppendLine(")");
            }
        }

        return new Tuple<string, SqlParam[]>(sbInsert.ToString(), insertParams.ToArray());
    }

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch4SqlServer<T>(
        List<T> list,
        string table,
        PropertyInfo[] propertiesToInsert) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        foreach (T model in list)
        {
            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            for (int i = 0; i < propertiesToInsert.Length; i++)
            {
                PropertyInfo propertyInfo = propertiesToInsert[i];

                if (firstParam)
                {
                    firstParam = false;
                }
                else
                {
                    if (firstRow)
                        sbInsert.Append(", ");

                    sbInsertValues.Append(", ");
                }

                if (firstRow)
                    sbInsert.Append($" {propertyInfo.Name} ");

                sbInsertValues.Append($" @p_{propertyInfo.Name}_{k} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}_{k}", propertyInfo.GetValue(model));

                insertParams.Add(prm);
            }

            if (firstRow)
            {
                firstRow = false;
                sbInsert
                    .AppendLine(") values ")
                    .Append(" (")
                    .Append(sbInsertValues).AppendLine(")");
            }
            else
            {
                sbInsert.Append(", (").Append(sbInsertValues).AppendLine(")");
            }
        }

        return new Tuple<string, SqlParam[]>(sbInsert.ToString(), insertParams.ToArray());
    }

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch4SqlServerWithSequence<T>(
        List<T> list,
        string table,
        List<string> dbColumns,
        string pkName,
        PropertyInfo[] propertiesToInsert,
        bool insertPrimaryKeyColumn) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        foreach (T model in list)
        {
            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            for (int i = 0; i < propertiesToInsert.Length; i++)
            {
                PropertyInfo propertyInfo = propertiesToInsert[i];

                if (firstParam)
                {
                    firstParam = false;
                }
                else
                {
                    if (firstRow)
                        sbInsert.Append(", ");

                    sbInsertValues.Append(", ");
                }

                if (!insertPrimaryKeyColumn
                    && propertyInfo.Name == pkName
                    && dbColumns.Contains(propertyInfo.Name))
                {
                    if (i == 0)
                        firstParam = true; // we don't add the primary key

                    continue;
                }

                if (firstRow)
                    sbInsert.Append($" {propertyInfo.Name} ");

                sbInsertValues.Append($" @p_{propertyInfo.Name}_{k} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}_{k}", propertyInfo.GetValue(model));

                insertParams.Add(prm);
            }

            if (firstRow)
            {
                firstRow = false;
                sbInsert
                    .AppendLine(") values ")
                    .Append(" (")
                    .Append(sbInsertValues).AppendLine(")");
            }
            else
            {
                sbInsert.Append(", (").Append(sbInsertValues).AppendLine(")");
            }
        }

        return new Tuple<string, SqlParam[]>(sbInsert.ToString(), insertParams.ToArray());
    }

    public static string ToJson<T>(this List<T> list)
    {
        return JsonConvert.SerializeObject(
            list,
            Formatting.None,
            new JsonSerializerSettings { ContractResolver = new JsonModelContractResolver() }
        );
    }

    public static string ToString<T>(this List<T> list)
    {
        return list.ToJson();
    }
}
