﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Models;

namespace Zen.DbAccess.Extensions.SqlServer;

internal static class SqlServerListHelper
{
    public static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchWithSequenceAsync<T>(
       List<T> list,
       DbConnectionType dbtype,
       DbConnection conn,
       DbTransaction? tx,
       string table,
       bool insertPrimaryKeyColumn) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T firstModel = list.First();
        await firstModel.SaveAsync(DbModelSaveType.InsertOnly, dbtype, conn, tx, table, insertPrimaryKeyColumn);

        if (list.Count <= 1)
            return new Tuple<string, SqlParam[]>("", Array.Empty<SqlParam>());

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(dbtype, insertPrimaryKeyColumn);

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

    public static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchAsync<T>(
        List<T> list,
        DbConnectionType dbtype,
        DbConnection conn,
        DbTransaction? tx,
        string table) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T firstModel = list.First();
        await firstModel.SaveAsync(DbModelSaveType.InsertOnly, dbtype, conn, tx, table, insertPrimaryKeyColumn: false);

        if (list.Count <= 1)
            return new Tuple<string, SqlParam[]>("", Array.Empty<SqlParam>());

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(dbtype, insertPrimaryKeyColumn: false);

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
}
