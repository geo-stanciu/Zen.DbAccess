using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Extensions.Oracle;
using Zen.DbAccess.Extensions.Postgresql;
using Zen.DbAccess.Extensions.Sqlite;
using Zen.DbAccess.Extensions.SqlServer;
using Zen.DbAccess.Factories;
using Zen.DbAccess.ContractResolvers;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Models;

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
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, dbConnectionFactory.DbType, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
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
        await list.SaveAllAsync(dbModelSaveType, dbConnectionFactory.DbType, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
        await conn.CloseAsync();
    }

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        DbConnectionType dbConnectionType,
        DbConnection conn,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, dbConnectionType, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
    }

    public static Task BulkInsertAsync<T>(
        this List<T> list,
        DbConnectionType dbConnectionType,
        DbConnection conn,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        return BulkInsertAsync<T>(
            list,
            dbConnectionType,
            conn,
            tx: null,
            table,
            runAllInTheSameTransaction,
            insertPrimaryKeyColumn,
            sequence2UseForPrimaryKey);
    }

    public static async Task BulkInsertAsync<T>(
        this List<T> list,
        DbConnectionType dbConnectionType,
        DbConnection conn,
        DbTransaction? tx,
        string table, 
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        bool isInTransaction = tx != null;

        if (runAllInTheSameTransaction && tx == null)
            tx = await conn.BeginTransactionAsync();

        try
        {
            T? firstModel = list.FirstOrDefault();

            if (firstModel == null)
                throw new NullReferenceException(nameof(firstModel));

            await firstModel.RefreshDbColumnsAndModelPropertiesAsync(dbConnectionType, conn, tx, table);

            int offset = 0;
            int take = Math.Min(list.Count - offset, 1024);

            while (offset < list.Count)
            {
                List<T> batch = list.Skip(offset).Take(take).ToList();
                Tuple<string, SqlParam[]> preparedQuery = await PrepareBulkInsertBatchAsync(
                    batch,
                    dbConnectionType,
                    conn,
                    tx,
                    table,
                    firstModel.dbModel_primaryKey_dbColumns!,
                    insertPrimaryKeyColumn, 
                    sequence2UseForPrimaryKey);

                string sql = preparedQuery.Item1;
                SqlParam[] sqlParams = preparedQuery.Item2;

                if (!string.IsNullOrEmpty(sql))
                    await sql.ExecuteScalarAsync(dbConnectionType, conn, tx, sqlParams);

                offset += batch.Count;
            }
        }
        catch
        {
            if (!isInTransaction && tx != null)
            {
                try
                {
                    await tx.RollbackAsync();
                }
                catch { }
            }

            throw;
        }

        if (!isInTransaction && tx != null)
            await tx.CommitAsync();
    }

    public static Task SaveAllAsync<T>(
        this List<T> list,
        DbModelSaveType dbModelSaveType,
        DbConnectionType dbConnectionType,
        DbConnection conn,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        return SaveAllAsync<T>(
            list,
            dbModelSaveType,
            dbConnectionType,
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
        DbConnectionType dbConnectionType,
        DbConnection conn,
        DbTransaction? tx,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        bool isInTransaction = tx != null;

        if (dbModelSaveType == DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn)
        {
            await BulkInsertAsync<T>(list, dbConnectionType, conn, tx, table, runAllInTheSameTransaction, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
            return;
        }

        if (runAllInTheSameTransaction && tx == null)
            tx = await conn.BeginTransactionAsync();

        try
        {
            T? firstModel = list.FirstOrDefault();

            if (firstModel == null)
                throw new NullReferenceException(nameof(firstModel));

            await firstModel.SaveAsync(dbModelSaveType, dbConnectionType, conn, tx, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);

            for (int i = 1; i < list.Count; i++)
            {
                T model = list[i];

                if (model == null)
                    continue;

                model.CopyDbModelPropsFrom(firstModel);
                await model.SaveAsync(dbModelSaveType, dbConnectionType, conn, tx, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
            }
        }
        catch
        {
            if (!isInTransaction && tx != null)
            {
                try
                {
                    await tx.RollbackAsync();
                }
                catch { }
            }

            throw;
        }

        if (!isInTransaction && tx != null)
            await tx.CommitAsync();
    }

    public static async Task DeleteAllAsync<T>(
        this List<T> list,
        DbConnectionType dbConnectionType,
        string conn_str,
        string table,
        bool runAllInTheSameTransaction = true) where T : DbModel
    {
        using DbConnection conn = await (new DbConnectionFactory(dbConnectionType, conn_str).BuildAndOpenAsync());
        await DeleteAllAsync<T>(list, dbConnectionType, conn, tx: null, table, runAllInTheSameTransaction);
        await conn.CloseAsync();
    }

    public static async Task DeleteAllAsync<T>(
        this List<T> list,
        DbConnectionFactory dbConnectionFactory,
        string table,
        bool runAllInTheSameTransaction = true) where T : DbModel
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        await DeleteAllAsync<T>(list, dbConnectionFactory.DbType, conn, tx: null, table, runAllInTheSameTransaction);
        await conn.CloseAsync();
    }

    public static async Task DeleteAllAsync<T>(
        this List<T> list,
        DbConnectionType dbConnectionType,
        DbConnection conn,
        string table,
        bool runAllInTheSameTransaction = true) where T : DbModel
    {
        await DeleteAllAsync<T>(list, dbConnectionType, conn, tx: null, table, runAllInTheSameTransaction);
    }

    public static async Task DeleteAllAsync<T>(
        this List<T> list,
        DbConnectionType dbtype,
        DbConnection conn,
        DbTransaction? tx,
        string table,
        bool runAllInTheSameTransaction = true) where T : DbModel
    {
        bool isInTransaction = tx != null;

        if (runAllInTheSameTransaction && tx == null)
            tx = await conn.BeginTransactionAsync();

        try
        {
            T? firstModel = list.FirstOrDefault();

            if (firstModel == null)
                throw new NullReferenceException(nameof(firstModel));

            await firstModel.RefreshDbColumnsAndModelPropertiesAsync(dbtype, conn, tx, table);

            if (firstModel.dbModel_primaryKey_dbColumns == null || firstModel.dbModel_primaryKey_dbColumns!.Count == 0)
                throw new NullReferenceException(nameof(firstModel.dbModel_primaryKey_dbColumns));

            (List<PropertyInfo> primaryKeyProps, bool isMultiColumnsPrimaryKey) = PreparePrimaryKeyProps4Delete<T>(firstModel);

            string sqlBase = PrepareDeleteBaseSql(firstModel, table, isMultiColumnsPrimaryKey);

            int offset = 0;
            int take = 512;

            while (offset < list.Count)
            {
                var items = list.Skip(offset).Take(take).ToList();
                offset += items.Count;

                (List<SqlParam> sqlParams, string deleteSqlList) = PrepareDeleteBulkSqlList<T>(items, isMultiColumnsPrimaryKey, primaryKeyProps);

                string sql = $" {sqlBase} in ( {deleteSqlList} ) ";

                _ = await sql.ExecuteNonQueryAsync(dbtype, conn, tx, sqlParams.ToArray());
            }
        }
        catch
        {
            if (!isInTransaction && tx != null)
            {
                try
                {
                    await tx.RollbackAsync();
                }
                catch { }
            }

            throw;
        }

        if (!isInTransaction && tx != null)
            await tx.CommitAsync();
    }

    private static (List<PropertyInfo>, bool) PreparePrimaryKeyProps4Delete<T>(T firstModel) where T : DbModel
    {
        List<PropertyInfo> primaryKeyProps = new List<PropertyInfo>();
        bool isMultiColumnsPrimaryKey = firstModel.dbModel_primaryKey_dbColumns!.Count > 1;

        foreach (string pkDbCol in firstModel.dbModel_primaryKey_dbColumns!)
        {
            primaryKeyProps.Add(firstModel.dbModel_dbColumn_map![pkDbCol]);
        }

        return (primaryKeyProps, isMultiColumnsPrimaryKey);
    }

    private static string PrepareDeleteBaseSql<T>(T firstModel, string table, bool isMultiColumnsPrimaryKey) where T : DbModel
    {
        StringBuilder sbSql = new StringBuilder();
        sbSql.Append($" delete from {table} where ");

        if (isMultiColumnsPrimaryKey)
        {
            bool isFirst = true;
            sbSql.Append("( ");

            foreach (string pkDbCol in firstModel.dbModel_primaryKey_dbColumns!)
            {
                if (isFirst)
                    isFirst = false;
                else
                    sbSql.Append(", ");

                sbSql.Append($" {pkDbCol} ");
            }

            sbSql.Append(") ");
        }
        else
        {
            sbSql.Append($" {firstModel.dbModel_primaryKey_dbColumns!.First()} ");
        }

        return sbSql.ToString();
    }

    private static (List<SqlParam>, string) PrepareDeleteBulkSqlList<T>(IEnumerable<T> items, bool isMultiColumnsPrimaryKey, List<PropertyInfo> primaryKeyProps) where T : DbModel
    {
        List<SqlParam> sqlParams = new List<SqlParam>(items.Count() * primaryKeyProps.Count);
        StringBuilder sbDeleteSql = new StringBuilder();

        int k = 0;
        bool isFirstItem = true;

        foreach (var item in items)
        {
            if (isFirstItem)
                isFirstItem = false;
            else
                sbDeleteSql.Append(", ");

            if (isMultiColumnsPrimaryKey)
            {
                sbDeleteSql.Append("( ");

                bool isFirstProp = true;

                foreach (PropertyInfo pkProp in primaryKeyProps)
                {
                    if (isFirstProp)
                        isFirstProp = false;
                    else
                        sbDeleteSql.Append(", ");

                    string prmName = $"@p_{pkProp.Name}_{k}";
                    sbDeleteSql.Append($" {prmName} ");
                    sqlParams.Add(new SqlParam(prmName, pkProp.GetValue(item) ?? DBNull.Value));
                }

                sbDeleteSql.Append(") ");
            }
            else
            {
                PropertyInfo pkProp = primaryKeyProps.First();
                string prmName = $"@p_{pkProp.Name}_{k}";
                sbDeleteSql.Append($" {prmName} ");
                sqlParams.Add(new SqlParam(prmName, pkProp.GetValue(item) ?? DBNull.Value));
            }

            k++;
        }

        return (sqlParams, sbDeleteSql.ToString());
    }

    private static Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchAsync<T>(
        List<T> list,
        DbConnectionType dbConnectionType,
        DbConnection conn,
        string table,
        List<string> pkNames,
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey) where T : DbModel
    {
        return PrepareBulkInsertBatchAsync<T>(
            list,
            dbConnectionType,
            conn,
            tx: null,
            table,
            pkNames,
            insertPrimaryKeyColumn,
            sequence2UseForPrimaryKey);
    }

    private static Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchAsync<T>(
        List<T> list,
        DbConnectionType dbConnectionType,
        DbConnection conn,
        DbTransaction? tx,
        string table,
        List<string> pkNames,
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey) where T : DbModel
    {
        if (dbConnectionType == DbConnectionType.Oracle)
        {
            if (!pkNames.Any())
            {
                return OracleListHelper.PrepareBulkInsertBatchAsync<T>(list, dbConnectionType, conn, table);
            }

            return OracleListHelper.PrepareBulkInsertBatchWithSequenceAsync<T>(list, dbConnectionType, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
        }
        else if (dbConnectionType == DbConnectionType.Postgresql)
        {
            if (!pkNames.Any())
            {
                return PostgresqlListHelper.PrepareBulkInsertBatchAsync<T>(list, dbConnectionType, conn, table);
            }

            return PostgresqlListHelper.PrepareBulkInsertBatchWithSequence<T>(list, dbConnectionType, conn, table, insertPrimaryKeyColumn);
        }
        else if (dbConnectionType == DbConnectionType.Sqlite)
        {
            if (!pkNames.Any())
            {
                return SqliteListHelper.PrepareBulkInsertBatchAsync<T>(list, dbConnectionType, conn, table);
            }

            return SqliteListHelper.PrepareBulkInsertBatchAsync<T>(list, dbConnectionType, conn, table, insertPrimaryKeyColumn);
        }
        else if (dbConnectionType == DbConnectionType.SqlServer)
        {
            if (!pkNames.Any())
            {
                return SqlServerListHelper.PrepareBulkInsertBatchAsync<T>(list, dbConnectionType, conn, tx, table);
            }

            return SqlServerListHelper.PrepareBulkInsertBatchWithSequenceAsync<T>(list, dbConnectionType, conn, tx, table, insertPrimaryKeyColumn);
        }
        else
        {
            throw new NotImplementedException($"PrepareBulkInsertBatch for {conn.GetType()}");
        }
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
