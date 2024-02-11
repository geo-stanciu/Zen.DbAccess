using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Interfaces;
using Zen.DbAccess.Models;
using Zen.DbAccess.Postgresql.Extensions;

namespace Zen.DbAccess.Postgresql;

public class DatabaseSpeciffic : IDbSpeciffic
{
    public (string, SqlParam) PrepareParameter(DbModel model, PropertyInfo propertyInfo)
    {
        IDbSpeciffic dbSpeciffic = this;
        (string prmName, SqlParam prm) = dbSpeciffic.PrepareParameter(model, propertyInfo);

        if (model.IsJsonDataType(propertyInfo))
            prmName += "::jsonb";

        return (prmName, prm);
    }

    public void EnsureTempTable(string table)
    {
        if (!table.StartsWith("temp_", StringComparison.OrdinalIgnoreCase)
            && !table.StartsWith("tmp_", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"{table} must begin with temp_ or tmp_ .");
        }
    }

    public void SetupFunctionCall(DbCommand cmd, string sql, params SqlParam[] parameters)
    {
        IDbSpeciffic dbSpeciffic = this;
        dbSpeciffic.SetupFunctionCall(cmd, sql, parameters);

        cmd.CommandType = CommandType.StoredProcedure;
    }

    public void SetupProcedureCall(IZenDbConnection conn, DbCommand cmd, string sql, bool isDataSetReturn, bool isTableReturn, params SqlParam[] parameters)
    {
        // commented on purpose.
        // Npgsql supports this mainly for portability,
        // but this style of calling has no advantage over the regular command shown above.
        // When CommandType.StoredProcedure is set,
        // Npgsql will simply generate the appropriate SELECT my_func() for you, nothing more.
        // Unless you have specific portability requirements,
        // it is recommended you simply avoid CommandType.StoredProcedure and construct the SQL yourself.
        //cmd.CommandType = CommandType.StoredProcedure;

        int countDots = sql.Split('.').Length - 1; // if countDots > 1 then we have also the schema in the name of the procedure being called

        StringBuilder sbSql = new StringBuilder();

        if (isDataSetReturn)
        {
            // we expect the p$ to be a function returning a refcursor
            sbSql.Append($"SELECT ");
        }
        else if (isTableReturn)
        {
            // we expect the p$ to be a function returning a table
            sbSql.Append($"SELECT * FROM ");
        }
        else
        {
            sbSql.Append($"CALL ");
        }

        sbSql.Append($"{(countDots > 1 ? sql.Substring(sql.IndexOf(".") + 1) : sql)}(");

        bool firstParam = true;
        foreach (SqlParam prm in parameters)
        {
            if (firstParam)
                firstParam = false;
            else
                sbSql.Append(", ");

            if (prm.name.StartsWith("@"))
                sbSql.Append($"{prm.name}");
            else
                sbSql.Append($"@{prm.name}");
        }

        sbSql.Append(")");

        cmd.CommandText = sbSql.ToString();
    }

    public async Task<DataSet> ExecuteProcedure2DataSetAsync(IZenDbConnection conn, DbDataAdapter da)
    { 
        DataSet ds = new DataSet();
        da.Fill(ds);

        if (ds.Tables.Count == 1 && ds.Tables[0].Columns.Count == 1)
        {
            string sql = da.SelectCommand.CommandText;
            string procedureName = sql.IndexOf(".") > 0 ? sql.Substring(sql.IndexOf(".") + 1) : sql;

            if (ds.Tables[0].Columns[0].ToString().ToLower() == procedureName.ToLower())
            {
                string[] openCursors = ds.Tables[0].AsEnumerable()
                    .Select(x => x[0]!.ToString()!)
                    .Where(x => x.StartsWith("<unnamed") && x.EndsWith(">") && !x.Contains(";"))
                    .ToArray();

                if (openCursors.Any())
                {
                    ds = new DataSet();
                    int k = 1;

                    foreach (string openCursor in openCursors)
                    {
                        DataTable dt = await conn.ExecuteCursorToTableAsync(openCursor);
                        dt.TableName = $"TABLE{k++}";
                        ds.Tables.Add(dt);
                    }
                }
            }
        }

        return ds;
    }

    public string GetGetServerDateTimeQuery()
    {
        string sql = "SELECT now()";

        return sql;
    }

    public (string, IEnumerable<SqlParam>) GetInsertedIdQuery(string table, DbModel model, string firstPropertyName)
    {
        string sql = "; select currval(pg_get_serial_sequence(@p_serial_table, @p_serial_id));";

        SqlParam p_serial_table = new SqlParam($"@p_serial_table", table);

        SqlParam p_serial_id = new SqlParam(
            $"@p_serial_id",
            model.dbModel_primaryKey_dbColumns.Any() ? model.dbModel_primaryKey_dbColumns![0] : model.dbModel_prop_map![firstPropertyName]);

        return (sql, new[] { p_serial_table, p_serial_id });
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
                        sbInsert.Append($" {dbCol} ");

                    sbInsertValues.Append($" default ");

                    continue;
                }

                if (firstRow)
                    sbInsert.Append($" {dbCol} ");

                string appendToParam;
                if (firstModel != null && firstModel.IsJsonDataType(propertyInfo))
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

                string appendToParam;
                if (firstModel != null && firstModel.IsJsonDataType(propertyInfo))
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
}
