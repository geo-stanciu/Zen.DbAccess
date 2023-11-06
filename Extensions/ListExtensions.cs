using Newtonsoft.Json;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Linq;
using System.Reflection;
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

            int offset = 0;
            int take = Math.Min(list.Count - offset, 1024);

            while (offset < list.Count)
            {
                List<T> batch = list.Skip(offset).Take(take).ToList();
                Tuple<string, SqlParam[]> preparedQuery = await PrepareBulkInsertBatchAsync(
                    batch, 
                    conn,
                    tx,
                    table,
                    firstModel.dbModel_primaryKey_dbColumns!,
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

        if (runAllInTheSameTransaction && tx == null)
            tx = await conn.BeginTransactionAsync();

        try
        {
            T? firstModel = list.FirstOrDefault();

            if (firstModel == null)
                throw new NullReferenceException(nameof(firstModel));

            await firstModel.SaveAsync(dbModelSaveType, conn, tx, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);

            for (int i = 1; i < list.Count; i++)
            {
                T model = list[i];

                if (model == null)
                    continue;

                // setez sql-urile si params (se face refresh cu valorile corespunzatoare la Save)
                model.CopyDbModelPropsFrom(firstModel);
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

    private static Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchAsync<T>(
        List<T> list,
        DbConnection conn,
        string table,
        List<string> pkNames,
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey) where T : DbModel
    {
        return PrepareBulkInsertBatchAsync<T>(
            list,
            conn,
            tx: null,
            table,
            pkNames,
            insertPrimaryKeyColumn,
            sequence2UseForPrimaryKey);
    }

    private static Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchAsync<T>(
        List<T> list,
        DbConnection conn,
        DbTransaction? tx,
        string table,
        List<string> pkNames,
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey) where T : DbModel
    {
        if (conn is OracleConnection)
        {
            if (!pkNames.Any())
            {
                return PrepareBulkInsertBatch4OracleAsync<T>(list, conn, table);
            }

            return PrepareBulkInsertBatch4OracleWithSequenceAsync<T>(list, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
        }
        else if (conn is NpgsqlConnection)
        {
            if (!pkNames.Any())
            {
                return PrepareBulkInsertBatch4PostgresqlAsync<T>(list, conn, table);
            }

            return PrepareBulkInsertBatch4PostgresqlWithSequence<T>(list, conn, table, insertPrimaryKeyColumn);
        }
        else if (conn is SQLiteConnection)
        {
            if (!pkNames.Any())
            {
                return PrepareBulkInsertBatch4SqliteAsync<T>(list, conn, table);
            }

            return PrepareBulkInsertBatch4SqliteAsync<T>(list, conn, table, insertPrimaryKeyColumn);
        }
        else if (conn is SqlConnection)
        {
            if (!pkNames.Any())
            {
                return PrepareBulkInsertBatch4SqlServerAsync<T>(list, conn, table);
            }

            return PrepareBulkInsertBatch4SqlServerWithSequenceAsync<T>(list, conn, table, insertPrimaryKeyColumn);
        }
        else
        {
            throw new NotImplementedException($"PrepareBulkInsertBatch for {conn.GetType()}");
        }
    }

    private static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatch4OracleAsync<T>(
        List<T> list,
        DbConnection conn,
        string table) where T : DbModel
    {
        int k = -1;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"INSERT ALL");

        T firstModel = list.First();
        await firstModel.SaveAsync(conn, table, insertPrimaryKeyColumn: false);

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(insertPrimaryKeyColumn: false);

        for (int i = 1; i < list.Count; i++)
        {
            T model = list[i];

            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            sbInsert.Append($"INTO {table} (");

            foreach (PropertyInfo propertyInfo in propertiesToInsert)
            {
                if (firstParam)
                    firstParam = false;
                else
                {
                    sbInsert.Append(", ");
                    sbInsertValues.Append(", ");
                }

                string dbCol = firstModel!.dbModel_prop_map![propertyInfo.Name];

                sbInsert.Append($" {dbCol} ");
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

    private static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatch4OracleWithSequenceAsync<T>(
        List<T> list,
        DbConnection conn,
        string table,
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey) where T : DbModel
    {
        int k = -1;
        bool firstParam = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine("BEGIN");

        T firstModel = list.First();
        await firstModel.SaveAsync(conn, table, insertPrimaryKeyColumn);

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(insertPrimaryKeyColumn);
        List<string> primaryKeyColumns = firstModel.dbModel_primaryKey_dbColumns!;

        for (int i = 1; i < list.Count; i++)
        {
            T model = list[i];

            k++;
            firstParam = true;

            sbInsert.Append($"INSERT INTO {table} (");
            StringBuilder sbInsertValues = new StringBuilder();

            foreach (PropertyInfo propertyInfo in propertiesToInsert)
            {
                string dbCol = firstModel!.dbModel_prop_map![propertyInfo.Name];

                if (!insertPrimaryKeyColumn
                    && string.IsNullOrEmpty(sequence2UseForPrimaryKey)
                    && primaryKeyColumns.Any(x => x == dbCol))
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
                    && primaryKeyColumns.Any(x => x == dbCol))
                {
                    sbInsert.Append($" {dbCol} ");
                    sbInsertValues.Append($"{sequence2UseForPrimaryKey}.nextval");

                    continue;
                }

                sbInsert.Append($" {dbCol} ");
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

    private static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatch4SqliteAsync<T>(
        List<T> list,
        DbConnection conn,
        string table) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T firstModel = list.First();
        await firstModel.SaveAsync(conn, table, insertPrimaryKeyColumn: false);

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(insertPrimaryKeyColumn: false);

        for (int i = 1; i < list.Count; i++)
        {
            T model = list[i];

            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            foreach (PropertyInfo propertyInfo in propertiesToInsert)
            {
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

                string dbCol = firstModel!.dbModel_prop_map![propertyInfo.Name];

                if (firstRow)
                    sbInsert.Append($" {dbCol} ");

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

    private static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatch4SqliteAsync<T>(
        List<T> list,
        DbConnection conn,
        string table,
        bool insertPrimaryKeyColumn) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T firstModel = list.First();
        await firstModel.SaveAsync(conn, table, insertPrimaryKeyColumn);

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(insertPrimaryKeyColumn);

        for (int i = 1; i < list.Count; i++)
        {
            T model = list[i];

            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            foreach (PropertyInfo propertyInfo in propertiesToInsert)
            {
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

                string dbCol = firstModel!.dbModel_prop_map![propertyInfo.Name];

                if (!insertPrimaryKeyColumn
                    && firstModel.dbModel_primaryKey_dbColumns!.Any(x => x == dbCol))
                {
                    if (firstRow)
                        sbInsert.Append($" {propertyInfo.Name} ");

                    sbInsertValues.Append($" null ");

                    continue;
                }

                if (firstRow)
                    sbInsert.Append($" {dbCol} ");

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

    private static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatch4PostgresqlAsync<T>(
        List<T> list,
        DbConnection conn,
        string table) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T firstModel = list.First();
        await firstModel.SaveAsync(conn, table, insertPrimaryKeyColumn: false);

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(insertPrimaryKeyColumn: false);

        for (int i = 1; i < list.Count; i++)
        {
            T model = list[i];

            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            foreach (PropertyInfo propertyInfo in propertiesToInsert)
            {
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

                string dbCol = firstModel!.dbModel_prop_map![propertyInfo.Name];

                if (firstRow)
                    sbInsert.Append($" {dbCol} ");

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

    private static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatch4PostgresqlWithSequence<T>(
        List<T> list,
        DbConnection conn,
        string table,
        bool insertPrimaryKeyColumn) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T firstModel = list.First();
        await firstModel.SaveAsync(conn, table, insertPrimaryKeyColumn);

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(insertPrimaryKeyColumn);

        for (int i = 1; i < list.Count; i++)
        {
            T model = list[i];

            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            foreach (PropertyInfo propertyInfo in propertiesToInsert)
            {
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

                string dbCol = firstModel!.dbModel_prop_map![propertyInfo.Name];

                if (!insertPrimaryKeyColumn
                    && firstModel.dbModel_primaryKey_dbColumns!.Any(x => x == dbCol))
                {
                    if (firstRow)
                        sbInsert.Append($" {dbCol} ");

                    sbInsertValues.Append($" default ");

                    continue;
                }

                if (firstRow)
                    sbInsert.Append($" {dbCol} ");

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

    private static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatch4SqlServerAsync<T>(
        List<T> list,
        DbConnection conn,
        string table) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T firstModel = list.First();
        await firstModel.SaveAsync(conn, table, insertPrimaryKeyColumn: false);

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(insertPrimaryKeyColumn: false);

        for (int i = 1; i < list.Count; i++)
        {
            T model = list[i];

            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            foreach (PropertyInfo propertyInfo in propertiesToInsert)
            {
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

                string dbCol = firstModel!.dbModel_prop_map![propertyInfo.Name];

                if (firstRow)
                    sbInsert.Append($" {dbCol} ");

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

    private static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatch4SqlServerWithSequenceAsync<T>(
        List<T> list,
        DbConnection conn,
        string table,
        bool insertPrimaryKeyColumn) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T firstModel = list.First();
        await firstModel.SaveAsync(conn, table, insertPrimaryKeyColumn);

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(insertPrimaryKeyColumn);

        for (int i = 1; i < list.Count; i++)
        {
            T model = list[i];

            k++;
            bool firstParam = true;
            StringBuilder sbInsertValues = new StringBuilder();

            foreach (PropertyInfo propertyInfo in propertiesToInsert)
            {
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

                string dbCol = firstModel!.dbModel_prop_map![propertyInfo.Name];

                if (!insertPrimaryKeyColumn
                    && firstModel.dbModel_primaryKey_dbColumns!.Any(x => x == dbCol))
                {
                    if (i == 0)
                        firstParam = true; // we don't add the primary key

                    continue;
                }

                if (firstRow)
                    sbInsert.Append($" {dbCol} ");

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
