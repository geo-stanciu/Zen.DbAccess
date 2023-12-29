using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Shared.Models;

namespace Zen.DbAccess.Extensions.Oracle;

internal static class OracleListHelper
{
    public static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchWithSequenceAsync<T>(
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

        await firstModel.SaveAsync(DbModelSaveType.InsertOnly, conn, table, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);

        if (list.Count <= 1)
            return new Tuple<string, SqlParam[]>("", Array.Empty<SqlParam>());

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(conn, insertPrimaryKeyColumn, sequence2UseForPrimaryKey);
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

    public static async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchAsync<T>(
        List<T> list,
        DbConnection conn,
        string table) where T : DbModel
    {
        int k = -1;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"INSERT ALL");

        T firstModel = list.First();
        await firstModel.SaveAsync(DbModelSaveType.InsertOnly, conn, table, insertPrimaryKeyColumn: false);

        if (list.Count <= 1)
            return new Tuple<string, SqlParam[]>("", Array.Empty<SqlParam>());

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(conn, insertPrimaryKeyColumn: false);

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
}
