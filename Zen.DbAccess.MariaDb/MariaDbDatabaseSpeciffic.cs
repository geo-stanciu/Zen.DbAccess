using MySqlConnector;
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
using Zen.DbAccess.Constants;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Helpers;
using Zen.DbAccess.Interfaces;
using Zen.DbAccess.MariaDb.Constants;
using Zen.DbAccess.Models;

namespace Zen.DbAccess.MariaDb;

public class MariaDbDatabaseSpeciffic : IDbSpeciffic
{
    public char EscapeCustomNameStartChar()
    {
        return '`';
    }

    public char EscapeCustomNameEndChar()
    {
        return '`';
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

    public string GetGetServerDateTimeQuery()
    {
        string sql = "SELECT now(3)";

        return sql;
    }

    public (string, IEnumerable<SqlParam>) GetInsertedIdQuery(string table, DbModel model, string firstPropertyName)
    {
        string sql = "; select LAST_INSERT_ID() as ROW_ID;";

        return (sql, Array.Empty<SqlParam>());
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

        if (await DbHasBulkInsertEnabledAsync(conn))
        {
            await UseBulkInsertAsync(list, conn, table, firstModel, propertiesToInsert);

            return;
        }

        await RunInsertBatchAsync(list, conn, table, firstModel, propertiesToInsert);
    }

    private async Task RunInsertBatchAsync<T>(
        List<T> list,
        IZenDbConnection conn,
        string table,
        T firstModel,
        List<PropertyInfo> propertiesToInsert) where T : DbModel
    {
        var sqlParamNames = new Dictionary<string, string>();

        var sbSql = new StringBuilder();

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
            }

            sbSql.Append(dbCol);

            var prmName = $"@p_{property.Name}";

            sqlParamNames[property.Name] = prmName;
        }

        sbSql.Append(") VALUES ");

        int offset = 0;
        int dbMaxBatchSize = (int)Math.Floor((decimal)MariaDbConstants.MaxParametersPerQuery / propertiesToInsert.Count);

        if (dbMaxBatchSize == 0)
        {
            throw new InvalidOperationException("The number of properties to insert exceeds the maximum allowed parameters per query.");
        }

        int batchSize = Math.Min(1024, dbMaxBatchSize);

        while (offset < list.Count)
        {
            var sbValues = new StringBuilder();

            var items = list.Skip(offset).Take(batchSize).ToList();
            offset += items.Count;

            var sqlParams = new SqlParam[items.Count * propertiesToInsert.Count];

            int k = 0;

            for (int i = 0; i < items.Count; i++)
            {
                if (i > 0)
                {
                    sbValues.Append(", ");
                }

                sbValues.Append("(");

                var item = items[i];

                isFirst = true;

                foreach (var property in propertiesToInsert)
                {
                    string prmName = $"{sqlParamNames[property.Name]}_{i}";

                    if (isFirst)
                    {
                        isFirst = false;
                    }
                    else
                    {
                        sbValues.Append(", ");
                    }

                    sbValues.Append(prmName);

                    var val = property.GetValue(item);

                    sqlParams[k++] = new SqlParam(prmName, val);
                }

                sbValues.Append(")");
            }

            string sql = $"{sbSql} {sbValues}";

            _ = await sql.ExecuteNonQueryAsync(conn, sqlParams);
        }
    }

    private async Task UseBulkInsertAsync<T>(
        List<T> list,
        IZenDbConnection conn,
        string table,
        T firstModel,
        List<PropertyInfo> propertiesToInsert) where T : DbModel
    {
        using DataTable dt = new DataTable();

        MySqlBulkCopy bulkCopy = new MySqlBulkCopy((MySqlConnection)conn.Connection, (MySqlTransaction?)conn.Transaction);

        bulkCopy.DestinationTableName = table;

        bulkCopy.BulkCopyTimeout = DbAccessConstants.DefaultCommandTimeoutSeconds;

        int k = 0;

        foreach (var property in propertiesToInsert)
        {
            string? dbColName = firstModel.GetMappedProperty(property.Name);

            Type t = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

            if (t.IsEnum || t.IsSubclassOf(typeof(Enum)))
            {
                dt.Columns.Add(dbColName, typeof(int));
            }
            else if (t == typeof(bool))
            {
                dt.Columns.Add(dbColName, typeof(int));
            }
            else
            {
                dt.Columns.Add(dbColName, t);
            }

            bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(k++, dbColName!));
        }

        foreach (var item in list)
        {
            k = 0;

            var values = new object[propertiesToInsert.Count];

            foreach (var property in propertiesToInsert)
            {
                var val = property.GetValue(item);

                if (val == null)
                {
                    values[k++] = DBNull.Value;
                    continue;
                }

                Type t = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                if (t.IsEnum || t.IsSubclassOf(typeof(Enum)))
                {
                    values[k++] = (int)val;
                }
                else if (t == typeof(bool))
                {
                    values[k++] = (bool)val ? 1 : 0;
                }
                else
                {
                    values[k++] = val;
                }
            }

            dt.Rows.Add(values);
        }

        var result = await bulkCopy.WriteToServerAsync(dt);
    }

    private async Task<bool> DbHasBulkInsertEnabledAsync(IZenDbConnection conn)
    {
        string cachekey = $"{conn.DbType}_HasBulkInsertEnabled";

        var cachedProps = await CacheHelper.GetOrAdd(cachekey, async () =>
        {
            string sql = "SELECT @@GLOBAL.local_infile;";

            var bulkInsertEnabled = Convert.ToInt32(await sql.ExecuteScalarAsync(conn)) == 1;

            if (bulkInsertEnabled)
            {
                var builder = new MySqlConnectionStringBuilder(conn.Connection.ConnectionString);

                bulkInsertEnabled = builder.AllowLoadLocalInfile;
            }

            return new DbPropertiesCacheModel
            {
                HasBulkInsertEnabled = bulkInsertEnabled,
            };
        });

        return cachedProps.HasBulkInsertEnabled;
    }
}
