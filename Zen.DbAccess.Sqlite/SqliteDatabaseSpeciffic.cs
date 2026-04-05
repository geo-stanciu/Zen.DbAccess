using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Constants;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Interfaces;
using Zen.DbAccess.Models;
using Zen.DbAccess.Sqlite.Constants;

namespace Zen.DbAccess.Sqlite;

public class SqliteDatabaseSpeciffic : IDbSpeciffic
{
    public DbProviderFactory BuildDbProviderFactory(DbConnectionType dbType)
    {
        var factory = SqliteFactory.Instance;
        return factory;
    }

    public (string, SqlParam) PrepareParameter(DbModel model, PropertyInfo propertyInfo)
    {
        (string prmName, SqlParam prm) = ((IDbSpeciffic)this).CommonPrepareParameter(model, propertyInfo);
        
        if (prm.value != null && prm.value != DBNull.Value)
        {
            Type t = Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType;

            if (t == typeof(DateOnly))
            {
                prm.value = ((DateOnly)prm.value).ToDateTime(TimeOnly.MinValue);
            }
            else if (t == typeof(TimeOnly))
            {
                prm.value = DateTime.MinValue.Date.Add(((TimeOnly)prm.value).ToTimeSpan());
            }
        }

        return (prmName, prm);
    }

    public object GetValueForPreparedParameter(DbModel dbModel, PropertyInfo propertyInfo)
    {
        var val = propertyInfo.GetValue(dbModel) ?? DBNull.Value;

        if (val != null && val != DBNull.Value)
        {
            Type t = Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType;

            if (t == typeof(DateOnly))
            {
                return ((DateOnly)val).ToDateTime(TimeOnly.MinValue);
            }
            else if (t == typeof(TimeOnly))
            {
                return DateTime.MinValue.Date.Add(((TimeOnly)val).ToTimeSpan());
            }
        }

        return val!;
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
        int dbMaxBatchSize = (int)Math.Floor((decimal)SqliteConstants.MaxParametersPerQuery / propertiesToInsert.Count);

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
}
