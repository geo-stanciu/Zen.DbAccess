using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Interfaces;
using Zen.DbAccess.Models;

namespace Zen.DbAccess.Sqlite;

public class DatabaseSpeciffic : IDbSpeciffic
{
    public DbParameter CreateDbParameter(DbCommand cmd, SqlParam prm)
    {
        DbParameter param = cmd.CreateParameter();

        string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;
        param.ParameterName = baseParameterName;
        cmd.CommandText = cmd.CommandText.Replace($"@{baseParameterName}", $"${baseParameterName}");

        return param;
    }

    public void EnsureTempTable(string table)
    {
        if (!table.StartsWith("temp_", StringComparison.OrdinalIgnoreCase)
            && !table.StartsWith("tmp_", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"{table} must begin with temp_ or tmp_ .");
        }
    }

    public string GetGetServerDateTimeQuery()
    {
        string sql = "SELECT current_timestamp";

        return sql;
    }

    public (string, IEnumerable<SqlParam>) GetInsertedIdQuery(string table, DbModel model, string firstPropertyName)
    {
        string sql = "; select last_insert_rowid() as ROW_ID;";

        return (sql, Array.Empty<SqlParam>());
    }

    public async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchWithSequenceAsync<T>(
        List<T> list,
        IZenDbConnection conn,
        string table,
        bool insertPrimaryKeyColumn,
        string sequence2UseForPrimaryKey) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

        T firstModel = list.First();
        await firstModel.SaveAsync(DbModelSaveType.InsertOnly, conn, table, insertPrimaryKeyColumn);

        if (list.Count <= 1)
            return new Tuple<string, SqlParam[]>("", Array.Empty<SqlParam>());

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(conn, insertPrimaryKeyColumn);

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

    public async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchAsync<T>(
        List<T> list,
        IZenDbConnection conn,
        string table) where T : DbModel
    {
        int k = -1;
        bool firstRow = true;
        StringBuilder sbInsert = new StringBuilder();
        List<SqlParam> insertParams = new List<SqlParam>();
        sbInsert.AppendLine($"insert into {table} ( ");

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
