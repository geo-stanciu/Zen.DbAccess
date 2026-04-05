using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
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
using Zen.DbAccess.SqlServer.Extensions;

namespace Zen.DbAccess.SqlServer;

public class SqlServerDatabaseSpeciffic : IDbSpeciffic
{
    public char EscapeCustomNameStartChar()
    {
        return '[';
    }

    public char EscapeCustomNameEndChar()
    {
        return ']';
    }

    public (string, SqlParam) PrepareEmptyParameter(DbModel model, PropertyInfo propertyInfo)
    {
        (string prmName, SqlParam prm) = ((IDbSpeciffic)this).CommonPrepareEmptyParameter(propertyInfo);

        if (!prm.isBlob && model.IsBlobDataType(propertyInfo))
        {
            prm.isBlob = true;
        }

        return (prmName, prm);
    }

    public (string, SqlParam) PrepareParameter(DbModel model, PropertyInfo propertyInfo)
    {
        (string prmName, SqlParam prm) = ((IDbSpeciffic)this).CommonPrepareParameter(model, propertyInfo);

        if (!prm.isBlob && model.IsBlobDataType(propertyInfo))
        {
            prm.isBlob = true;
        }

        return (prmName, prm);
    }

    public void EnsureTempTable(string table)
    {
        if (!table.StartsWith("#", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"{table} must begin with #.");
        }
    }

    public string GetGetServerDateTimeQuery()
    {
        string sql = "SELECT GETDATE()";

        return sql;
    }

    public (string, IEnumerable<SqlParam>) GetInsertedIdQuery(string table, DbModel model, string firstPropertyName)
    {
        string sql = "; select SCOPE_IDENTITY() as ROW_ID;";

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

        using SqlBulkCopy bulkCopy = new SqlBulkCopy((SqlConnection)conn.Connection, SqlBulkCopyOptions.Default, (SqlTransaction)conn.Transaction!);

        bulkCopy.DestinationTableName = table;

        bulkCopy.BatchSize = 5000;
        bulkCopy.BulkCopyTimeout = DbAccessConstants.DefaultCommandTimeoutSeconds;

        using DataTable dt = new DataTable();

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

            bulkCopy.ColumnMappings.Add(k++, dbColName!);
        }

        foreach (var item in list)
        {
            var values = new List<object>(propertiesToInsert.Count);

            foreach (var property in propertiesToInsert)
            {
                var val = property.GetValue(item);

                if (val == null)
                {
                    values.Add(DBNull.Value);
                    continue;
                }

                Type t = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                if (t.IsEnum || t.IsSubclassOf(typeof(Enum)))
                {
                    values.Add((int)val);
                }
                else if (t == typeof(bool))
                {
                    values.Add((bool)val ? 1 : 0);
                }
                else
                {
                    values.Add(val);
                }
            }

            dt.Rows.Add(values.ToArray());
        }

        await Task.Run(() => bulkCopy.WriteToServer(dt))
            .ContinueWith(t =>
            {
                if (t.IsFaulted && t.Exception != null)
                {
                    throw t.Exception;
                }
            });
    }
}
