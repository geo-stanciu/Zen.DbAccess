using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.Constants;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Helpers;
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

    public async Task BulkInsertAsync<T>(
        List<T> list,
        IZenDbConnection conn,
        string table,
        bool insertPrimaryKeyColumn = false) where T : DbModel
    {
        T? firstModel = list.FirstOrDefault();

        if (firstModel == null)
            throw new NullReferenceException(nameof(firstModel));

        firstModel.RefreshDbColumnsAndModelProperties(conn, table);

        var propertiesToInsert = firstModel.GetPropertiesToInsert(conn, insertPrimaryKeyColumn, table);

        if (insertPrimaryKeyColumn || list.Count < 5_000)
        {
            if (list.Count > 5_000)
            {
                int offset = 0;

                while (offset < list.Count)
                {
                    var items = list.Skip(offset).Take(5_000).ToList();

                    await UseArrayBindingAsync(items, conn, table, firstModel, propertiesToInsert, insertPrimaryKeyColumn);

                    offset += items.Count;
                }
            }
            else
            {
                await UseArrayBindingAsync(list, conn, table, firstModel, propertiesToInsert, insertPrimaryKeyColumn);
            }

            return;
        }

        await UseBulkInsertAsync(list, conn, table, firstModel, propertiesToInsert);
    }

    private async Task UseArrayBindingAsync<T>(List<T> list, IZenDbConnection conn, string table, T firstModel, List<PropertyInfo> propertiesToInsert, bool insertPrimaryKeyColumn) where T : DbModel
    {
        object[][] paramsArrays = new object[propertiesToInsert.Count][];
        NpgsqlParameter[] sqlParams = new NpgsqlParameter[propertiesToInsert.Count];

        int k = 0;

        var sbSql = new StringBuilder();
        var sbSqlColumns = new StringBuilder();
        var sbSqlValues = new StringBuilder();
        var paramNamesByPropName = new Dictionary<string, string>();

        sbSql.Append($"INSERT INTO {table} (");

        bool isFirst = true;

        foreach (var property in propertiesToInsert)
        {
            string? dbCol = firstModel.GetMappedProperty(property.Name);

            if (isFirst)
            {
                isFirst = false;
            }
            else
            {
                sbSql.Append(", ");
                sbSqlColumns.Append(", ");
                sbSqlValues.Append(", ");
            }

            sbSql.Append(dbCol);
            sbSqlColumns.Append(dbCol);

            var prmName = $"@p_{property.Name}";

            paramNamesByPropName[property.Name] = prmName;

            sbSqlValues.Append(prmName);

            if (firstModel.IsJsonDataType(property))
                sbSqlValues.Append("::jsonb[]");

            paramsArrays[k] = new object[list.Count];

            Type t = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
            var dbType = GetCorrespondingNpgsqlPropertyType<T>(firstModel, property, t);
            var nParam = new NpgsqlParameter(prmName, NpgsqlTypes.NpgsqlDbType.Array | dbType);
            nParam.Value = paramsArrays[k];

            if (insertPrimaryKeyColumn && firstModel.IsPartOfThePrimaryKey(dbCol!))
            {
                nParam.Direction = ParameterDirection.InputOutput;

                if (dbType == NpgsqlDbType.Varchar || dbType == NpgsqlDbType.Text)
                    nParam.Size = 32000;
            }

            sqlParams[k] = nParam;

            k++;
        }

        sbSql
            .Append(") SELECT ")
            .Append(sbSqlColumns)
            .Append(" FROM UNNEST (")
            .Append(sbSqlValues)
            .Append(") AS t(")
            .Append(sbSqlColumns)
            .Append(")");

        for (int i = 0; i < list.Count; i++)
        {
            for (k = 0; k < propertiesToInsert.Count; k++)
            {
                var val = propertiesToInsert[k].GetValue(list[i]);

                if (val == null)
                {
                    paramsArrays[k][i] = DBNull.Value;
                    continue;
                }

                Type t = Nullable.GetUnderlyingType(propertiesToInsert[k].PropertyType) ?? propertiesToInsert[k].PropertyType;

                if (t.IsEnum || t.IsSubclassOf(typeof(Enum)))
                {
                    paramsArrays[k][i] = (int)val;
                }
                else if (t == typeof(bool))
                {
                    paramsArrays[k][i] = (bool)val ? 1 : 0;
                }
                else if (t == typeof(DateTime))
                {
                    var date = DateTime.SpecifyKind((DateTime)val, DateTimeKind.Unspecified);

                    paramsArrays[k][i] = date;
                }
                else
                {
                    paramsArrays[k][i] = val;
                }
            }
        }

        string sql = sbSql.ToString();

        await using var cmd = new NpgsqlCommand(sql, (NpgsqlConnection)conn.Connection);

        cmd.Parameters.AddRange(sqlParams);

        _ = await cmd.ExecuteNonQueryAsync();

        for (k = 0; k < cmd.Parameters.Count; k++)
        {
            var prm = cmd.Parameters[k];

            if (insertPrimaryKeyColumn)
            {
                var propName = firstModel.GetMappedProperty(prm.ParameterName.Substring(2));
                var dbCol = !string.IsNullOrEmpty(propName) ? firstModel.GetMappedProperty(propName) : null;
                var isPartOfThePrimaryKey = !string.IsNullOrEmpty(dbCol) ? firstModel.IsPartOfThePrimaryKey(dbCol) : false;

                if (isPartOfThePrimaryKey)
                {
                    var prop = propertiesToInsert.FirstOrDefault(x => x.Name == propName)
                        ?? throw new Exception($"Property {propName} not found in the properties to insert list for {table}");

                    for (int i = 0; i < list.Count; i++)
                    {
                        PropertyMapHelper.SetPropertyValue(list[i], prop, paramsArrays[k][i]);
                    }
                }
            }
        }
    }

    public async Task UseBulkInsertAsync<T>(
        List<T> list,
        IZenDbConnection conn,
        string table,
        T firstModel,
        List<PropertyInfo> propertiesToInsert) where T : DbModel
    {
        var sbSql = new StringBuilder();

        bool isFirst = true;

        foreach (var property in propertiesToInsert)
        {
            string? dbCol = firstModel.GetMappedProperty(property.Name);

            if (isFirst)
            {
                isFirst = false;
            }
            else
            {
                sbSql.Append(", ");
            }

            sbSql.Append(dbCol);
        }

        await using var binaryWriter = await ((NpgsqlConnection)conn.Connection).BeginBinaryImportAsync(
            $"COPY {table} ({sbSql}) FROM STDIN (FORMAT BINARY)");

        foreach (var item in list)
        {
            await binaryWriter.StartRowAsync();

            foreach (var property in propertiesToInsert)
            {
                var val = property.GetValue(item);

                if (val == null)
                {
                    await binaryWriter.WriteNullAsync();

                    continue;
                }

                Type t = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                if (t.IsEnum || t.IsSubclassOf(typeof(Enum)))
                {
                    await binaryWriter.WriteAsync((int)val, NpgsqlTypes.NpgsqlDbType.Integer);

                    continue;
                }
                else if (t == typeof(bool))
                {
                    await binaryWriter.WriteAsync((bool)val ? 1 : 0, NpgsqlTypes.NpgsqlDbType.Integer);

                    continue;
                }
                else if (t == typeof(DateTime))
                {
                    var date = DateTime.SpecifyKind((DateTime)val, DateTimeKind.Unspecified);

                    await binaryWriter.WriteAsync(date, NpgsqlTypes.NpgsqlDbType.Timestamp);

                    continue;
                }

                await binaryWriter.WriteAsync(val, GetCorrespondingNpgsqlPropertyType(item, property, t));
            }
        }

        await binaryWriter.CompleteAsync();
    }

    private NpgsqlTypes.NpgsqlDbType GetCorrespondingNpgsqlPropertyType<T>(T model, PropertyInfo property, Type propertyType) where T : DbModel
    {
        if (propertyType.IsEnum || propertyType.IsSubclassOf(typeof(Enum)))
        {
            return NpgsqlDbType.Integer;
        }
        else if (propertyType == typeof(bool))
        {
            return NpgsqlDbType.Integer;
        }

        if (model.IsJsonDataType(property))
            return NpgsqlTypes.NpgsqlDbType.Jsonb;

        return propertyType switch
        {
            Type t when t == typeof(byte) => NpgsqlTypes.NpgsqlDbType.Integer,
            Type t when t == typeof(short) => NpgsqlTypes.NpgsqlDbType.Smallint,
            Type t when t == typeof(int) => NpgsqlTypes.NpgsqlDbType.Integer,
            Type t when t == typeof(long) => NpgsqlTypes.NpgsqlDbType.Bigint,
            Type t when t == typeof(float) => NpgsqlTypes.NpgsqlDbType.Real,
            Type t when t == typeof(double) => NpgsqlTypes.NpgsqlDbType.Double,
            Type t when t == typeof(decimal) => NpgsqlTypes.NpgsqlDbType.Numeric,
            Type t when t == typeof(DateOnly) => NpgsqlTypes.NpgsqlDbType.Date,
            Type t when t == typeof(TimeOnly) => NpgsqlTypes.NpgsqlDbType.Time,
            Type t when t == typeof(DateTime) => NpgsqlTypes.NpgsqlDbType.Timestamp,
            Type t when t == typeof(DateTimeOffset) => NpgsqlTypes.NpgsqlDbType.TimestampTz,
            Type t when t == typeof(byte[]) => NpgsqlTypes.NpgsqlDbType.Bytea,
            Type t when t == typeof(string) => NpgsqlTypes.NpgsqlDbType.Text,
            _ => throw new NotImplementedException($"Type {propertyType} is not supported for bulk insert.")
        };
    }
}
