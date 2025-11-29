using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Models;
using Zen.DbAccess.Interfaces;
using System.Text.Json;
using Newtonsoft.Json;

namespace Zen.DbAccess.Extensions;

public static class ListExtensions
{
    public static async Task SaveAllAsync<T>(
        this List<T> list,
        IDbConnectionFactory dbConnectionFactory,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false) where T : DbModel
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
    }

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        DbModelSaveType dbModelSaveType,
        IDbConnectionFactory dbConnectionFactory,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false) where T : DbModel
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        await list.SaveAllAsync(dbModelSaveType, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
    }

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        IZenDbConnection conn,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
    }

    public static async Task BulkInsertAsync<T>(
        this List<T> list,
        IZenDbConnection conn,
        string table, 
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        bool isInTransaction = conn.Transaction != null;

        if (runAllInTheSameTransaction && conn.Transaction == null)
            await conn.BeginTransactionAsync();

        try
        {
            T? firstModel = list.FirstOrDefault();

            if (firstModel == null)
                throw new NullReferenceException(nameof(firstModel));

            firstModel.RefreshDbColumnsAndModelProperties(conn, table);

            int offset = 0;
            int take = Math.Min(list.Count - offset, 1024);

            while (offset < list.Count)
            {
                List<T> batch = list.Skip(offset).Take(take).ToList();
                Tuple<string, SqlParam[]> preparedQuery = PrepareBulkInsertBatch(
                    batch,
                    conn,
                    table,
                    firstModel.dbModel_primaryKey_dbColumns!,
                    insertPrimaryKeyColumn, 
                    sequence2UseForPrimaryKey);

                string sql = preparedQuery.Item1;
                SqlParam[] sqlParams = preparedQuery.Item2;

                if (!string.IsNullOrEmpty(sql))
                    await sql.ExecuteScalarAsync(conn, sqlParams);

                offset += batch.Count;
            }
        }
        catch
        {
            if (!isInTransaction && conn.Transaction != null)
            {
                try
                {
                    await conn.RollbackAsync();
                }
                catch { }
            }

            throw;
        }

        if (!isInTransaction && conn.Transaction != null)
            await conn.CommitAsync();
    }

    public static async Task SaveAllAsync<T>(
        this List<T> list,
        DbModelSaveType dbModelSaveType,
        IZenDbConnection conn,
        string table,
        bool runAllInTheSameTransaction = true,
        bool insertPrimaryKeyColumn = false,
        string sequence2UseForPrimaryKey = "") where T : DbModel
    {
        bool isInTransaction = conn.Transaction != null;

        if (dbModelSaveType == DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn)
        {
            await BulkInsertAsync<T>(list, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
            return;
        }

        if (runAllInTheSameTransaction && conn.Transaction == null)
            await conn.BeginTransactionAsync();

        try
        {
            T? firstModel = list.FirstOrDefault();

            if (firstModel == null)
                throw new NullReferenceException(nameof(firstModel));

            firstModel.RefreshDbColumnsAndModelProperties(conn, table);

            await firstModel.SaveAsync(dbModelSaveType, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);

            for (int i = 1; i < list.Count; i++)
            {
                T model = list[i];

                if (model == null)
                    continue;

                model.CopyDbModelPropsFrom(firstModel);
                await model.SaveAsync(dbModelSaveType, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
            }
        }
        catch
        {
            if (!isInTransaction && conn.Transaction != null)
            {
                try
                {
                    await conn.RollbackAsync();
                }
                catch { }
            }

            throw;
        }

        if (!isInTransaction && conn.Transaction != null)
            await conn.CommitAsync();
    }

    public static async Task DeleteAllAsync<T>(
        this List<T> list,
        IDbConnectionFactory dbConnectionFactory,
        string table,
        bool runAllInTheSameTransaction = true) where T : DbModel
    {
        await using IZenDbConnection conn = await dbConnectionFactory.BuildAsync();
        await DeleteAllAsync<T>(list, conn, table, runAllInTheSameTransaction);
    }

    public static async Task DeleteAllAsync<T>(
        this List<T> list,
        IZenDbConnection conn,
        string table,
        bool runAllInTheSameTransaction = true) where T : DbModel
    {
        bool isInTransaction = conn.Transaction != null;

        if (runAllInTheSameTransaction && conn.Transaction == null)
            await conn.BeginTransactionAsync();

        try
        {
            T? firstModel = list.FirstOrDefault();

            if (firstModel == null)
                throw new NullReferenceException(nameof(firstModel));

            firstModel.RefreshDbColumnsAndModelProperties(conn, table);

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

                _ = await sql.ExecuteNonQueryAsync(conn, sqlParams.ToArray());
            }
        }
        catch
        {
            if (!isInTransaction && conn.Transaction != null)
            {
                try
                {
                    await conn.RollbackAsync();
                }
                catch { }
            }

            throw;
        }

        if (!isInTransaction && conn.Transaction != null)
            await conn.CommitAsync();
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

    private static Tuple<string, SqlParam[]> PrepareBulkInsertBatch<T>(
        List<T> list,
        IZenDbConnection conn,
        string table,
        List<string> pkNames,
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey) where T : DbModel
    {
        if (!pkNames.Any())
        {
            return conn.DatabaseSpeciffic.PrepareBulkInsertBatch<T>(list, conn, table);
        }

        return conn.DatabaseSpeciffic.PrepareBulkInsertBatchWithSequence<T>(list, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
    }
    
    public static string ToJson<T>(this List<T> list)
    {
        return JsonConvert.SerializeObject(list);
    }

    public static string ToString<T>(this List<T> list)
    {
        return list.ToJson();
    }
}
