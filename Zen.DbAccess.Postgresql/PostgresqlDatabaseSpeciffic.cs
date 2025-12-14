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
using Zen.DbAccess.Utils;

namespace Zen.DbAccess.Postgresql;

public class PostgresqlDatabaseSpeciffic : IDbSpeciffic
{
    public (string, SqlParam) PrepareEmptyParameter(DbModel model, PropertyInfo propertyInfo)
    {
        (string prmName, SqlParam prm) = ((IDbSpeciffic)this).CommonPrepareEmptyParameter(propertyInfo);

        if (model.IsJsonDataType(propertyInfo))
            prmName += "::jsonb";

        return (prmName, prm);
    }

    public (string, SqlParam) PrepareParameter(DbModel model, PropertyInfo propertyInfo)
    {
        (string prmName, SqlParam prm) = ((IDbSpeciffic)this).CommonPrepareParameter(model, propertyInfo);

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
        ((IDbSpeciffic)this).CommonSetupFunctionCall(cmd, sql, parameters);

        cmd.CommandType = CommandType.StoredProcedure;
    }

    public void SetupProcedureCall(IZenDbConnection conn, DbCommand cmd, string sql, bool isQueryReturn, params SqlParam[] parameters)
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

        if (isQueryReturn)
        {
            // we expect the p$ to be a function returning one or more refcursors / a function returning a table
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

    public bool ShouldFetchProcedureAsCursorsAsync()
    {
        return true;
    }

    public async Task<List<string>> QueryCursorNamesAsync(IZenDbConnection conn, DbCommand cmd)
    {
        List<string> rez = new List<string>();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        using (var dRead = await cmd.ExecuteReaderAsync())
        {
            while (await dRead.ReadAsync())
            {
                rez.Add(dRead.GetString(0));
            }
        }

        return rez;
    }

    public async Task<List<T>> QueryCursorAsync<T>(IZenDbConnection conn, string procedureName, string cursorName)
    {
        string sql = $"FETCH ALL IN \"{cursorName}\"";

        var result = await sql.FetchCursorAsync<T>(conn, procedureName);

        return result ?? new List<T>();
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

        var pkProps = model.GetPrimaryKeyProperties();

        SqlParam p_serial_id = new SqlParam(
            $"@p_serial_id",
            model.HasPrimaryKey() ? model.GetMappedProperty(pkProps.First().Name) : model.GetMappedProperty(firstPropertyName));

        return (sql, new[] { p_serial_table, p_serial_id });
    }

    public Tuple<string, SqlParam[]> PrepareBulkInsertBatchWithSequence<T>(
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
        firstModel.ResetDbModel();
        firstModel.RefreshDbColumnsAndModelProperties(conn, table);

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(conn, insertPrimaryKeyColumn, table);

        for (int i = 0; i < list.Count; i++)
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

                string? dbCol = firstModel!.GetMappedProperty(propertyInfo.Name);

                if (!insertPrimaryKeyColumn
                    && !string.IsNullOrEmpty(dbCol)
                    && firstModel!.IsPartOfThePrimaryKey(dbCol))
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

    public Tuple<string, SqlParam[]> PrepareBulkInsertBatch<T>(
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
        firstModel.ResetDbModel();
        firstModel.RefreshDbColumnsAndModelProperties(conn, table);

        List<PropertyInfo> propertiesToInsert = firstModel.GetPropertiesToInsert(conn, insertPrimaryKeyColumn: false, table: table);

        for (int i = 0; i < list.Count; i++)
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

                string? dbCol = firstModel!.GetMappedProperty(propertyInfo.Name);

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
