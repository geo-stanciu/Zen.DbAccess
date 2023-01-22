using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
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
    public static async Task SaveAllAsync<T>(this List<T> list, string conn_str, string table, bool runAllInTheSameTransaction = true, bool insertPrimaryKeyColumn = false)
    {
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, DbConnectionFactory.DefaultDbType, conn_str, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
    }

    public static async Task SaveAllAsync<T>(this List<T> list, DbModelSaveType dbModelSaveType, string conn_str, string table, bool runAllInTheSameTransaction = true, bool insertPrimaryKeyColumn = false)
    {
        await list.SaveAllAsync(dbModelSaveType, DbConnectionFactory.DefaultDbType, conn_str, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
    }

    public static async Task SaveAllAsync<T>(this List<T> list, DbConnectionType dbtype, string conn_str, string table, bool runAllInTheSameTransaction = true, bool insertPrimaryKeyColumn = false)
    {
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, new DbConnectionFactory(dbtype, conn_str), table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
    }

    public static async Task SaveAllAsync<T>(this List<T> list, DbModelSaveType dbModelSaveType, DbConnectionType dbtype, string conn_str, string table, bool runAllInTheSameTransaction = true, bool insertPrimaryKeyColumn = false)
    {
        await list.SaveAllAsync(dbModelSaveType, new DbConnectionFactory(dbtype, conn_str), table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
    }

    public static async Task SaveAllAsync<T>(this List<T> list, DbConnectionFactory dbConnectionFactory, string table, bool runAllInTheSameTransaction = true, bool insertPrimaryKeyColumn = false)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        await list.SaveAllAsync(DbModelSaveType.InsertUpdate, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
        await conn.CloseAsync();
    }
    public static async Task SaveAllAsync<T>(this List<T> list, DbModelSaveType dbModelSaveType, DbConnectionFactory dbConnectionFactory, string table, bool runAllInTheSameTransaction = true, bool insertPrimaryKeyColumn = false)
    {
        using DbConnection conn = await dbConnectionFactory.BuildAndOpenAsync();
        await list.SaveAllAsync(dbModelSaveType, conn, table, runAllInTheSameTransaction, insertPrimaryKeyColumn);
        await conn.CloseAsync();
    }

    public static async Task SaveAllAsync<T>(this List<T> list, DbModelSaveType dbModelSaveType, DbConnection conn, string table, bool runAllInTheSameTransaction = true, bool insertPrimaryKeyColumn = false)
    {
        if (list.Count == 0)
            return;

        if (conn == null)
            throw new ArgumentNullException(nameof(conn));

        Type classType = typeof(T);

        MethodInfo? saveAsyncMethod = classType.GetMethod("SaveAsync", new Type[] {
                typeof(DbModelSaveType),
                typeof(DbConnection),
                typeof(string),
                typeof(bool)
            });

        if (saveAsyncMethod == null)
            throw new NullReferenceException(nameof(saveAsyncMethod));

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


        DbTransaction? tx = null;

        if (runAllInTheSameTransaction)
            tx = await conn.BeginTransactionAsync();

        try
        {
            T? firstModel = list.FirstOrDefault();

            if (firstModel == null)
                throw new NullReferenceException(nameof(firstModel));

            Task? saveTask = (Task?)saveAsyncMethod?.Invoke(firstModel, new object[] { dbModelSaveType, conn, table, insertPrimaryKeyColumn });

            if (saveTask == null)
                throw new NullReferenceException(nameof(saveTask));

            await saveTask;

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

                // setez sql-urile si params (se face refresh cu valorile corespunzatoare la Save)
                sqlUpdateProp.SetValue(model, sql_update, null);
                sqlInsertProp.SetValue(model, sql_insert, null);
                modelPropertiesProp.SetValue(model, modelProps, null);
                dbColumnsProp.SetValue(model, dbColumns, null);
                pkNameProp.SetValue(model, pkName, null);
                primaryKeyProp.SetValue(model, primaryKey, null);

                saveTask = (Task?)saveAsyncMethod?.Invoke(model, new object[] { dbModelSaveType, conn, table, insertPrimaryKeyColumn });

                if (saveTask == null)
                    throw new NullReferenceException(nameof(saveTask));

                await saveTask;
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
