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
using Zen.DbAccess.Extensions.Oracle;
using Zen.DbAccess.Extensions.Postgresql;
using Zen.DbAccess.Extensions.Sqlite;
using Zen.DbAccess.Extensions.SqlServer;
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

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        DbConnection conn,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
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
        bool isInTransaction = tx != null;

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

                if (!string.IsNullOrEmpty(sql))
                    await sql.ExecuteScalarAsync(conn, tx, sqlParams);

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
        bool isInTransaction = tx != null;

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

                model.CopyDbModelPropsFrom(firstModel);
                await model.SaveAsync(dbModelSaveType, conn, tx, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
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
                return OracleListHelper.PrepareBulkInsertBatchAsync<T>(list, conn, table);
            }

            return OracleListHelper.PrepareBulkInsertBatchWithSequenceAsync<T>(list, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
        }
        else if (conn is NpgsqlConnection)
        {
            if (!pkNames.Any())
            {
                return PostgresqlListHelper.PrepareBulkInsertBatchAsync<T>(list, conn, table);
            }

            return PostgresqlListHelper.PrepareBulkInsertBatchWithSequence<T>(list, conn, table, insertPrimaryKeyColumn);
        }
        else if (conn is SQLiteConnection)
        {
            if (!pkNames.Any())
            {
                return SqliteListHelper.PrepareBulkInsertBatchAsync<T>(list, conn, table);
            }

            return SqliteListHelper.PrepareBulkInsertBatchAsync<T>(list, conn, table, insertPrimaryKeyColumn);
        }
        else if (conn is SqlConnection)
        {
            if (!pkNames.Any())
            {
                return SqlServerListHelper.PrepareBulkInsertBatchAsync<T>(list, conn, tx, table);
            }

            return SqlServerListHelper.PrepareBulkInsertBatchWithSequenceAsync<T>(list, conn, tx, table, insertPrimaryKeyColumn);
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
