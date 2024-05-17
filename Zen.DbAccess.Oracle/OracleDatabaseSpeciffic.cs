using System;
using System.Data.Common;
using System.Text;
using Zen.DbAccess.Models;
using Zen.DbAccess.DatabaseSpeciffic;
using Oracle.ManagedDataAccess.Types;
using Oracle.ManagedDataAccess.Client;
using Zen.DbAccess.Interfaces;
using System.Data;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Reflection;
using Zen.DbAccess.Enums;
using System.Linq;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Oracle.Extensions;

namespace Zen.DbAccess.Oracle;

public class OracleDatabaseSpeciffic : IDbSpeciffic
{
    public (string, SqlParam) PrepareParameter(DbModel model, PropertyInfo propertyInfo)
    {
        (string prmName, SqlParam prm) = ((IDbSpeciffic)this).CommonPrepareParameter(model, propertyInfo);

        if (model.IsClobDataType(propertyInfo))
            prm.isClob = true;

        return (prmName, prm);
    }

    public void DisposeBlob(DbCommand cmd, SqlParam prm)
    {
        if (prm.value != null && prm.value != DBNull.Value)
        {
            string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;

            if (cmd.Parameters[baseParameterName].Value as OracleBlob != null)
                (cmd.Parameters[baseParameterName].Value as OracleBlob)!.Dispose();
        }
    }

    public void DisposeClob(DbCommand cmd, SqlParam prm)
    {
        if (prm.value != null && prm.value != DBNull.Value)
        {
            string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;

            if (cmd.Parameters[baseParameterName].Value as OracleClob != null)
                (cmd.Parameters[baseParameterName].Value as OracleClob)!.Dispose();
        }
    }

    public object GetValueAsBlob(IZenDbConnection conn, object value)
    {
        OracleBlob blob = new OracleBlob(conn.Connection as OracleConnection);
        byte[] byteContent = (value as byte[])!;
        blob.Write(byteContent, 0, byteContent.Length);

        return blob;
    }

    public object GetValueAsClob(IZenDbConnection conn, object value)
    {
        OracleClob clob = new OracleClob(conn.Connection as OracleConnection);
        byte[] byteContent = Encoding.Unicode.GetBytes((value as string)!);
        clob.Write(byteContent, 0, byteContent.Length);

        return clob;
    }

    public DbDataAdapter CreateDataAdapter(IZenDbConnection conn)
    {
        DbDataAdapter? da = DbProviderFactories.GetFactory(conn.Connection)?.CreateDataAdapter();

        if (da != null)
            (da as OracleDataAdapter)!.SuppressGetDecimalInvalidCastException = true;
        else
            throw new NullReferenceException("DataAdapter");

        return da;
    }

    public DbParameter CreateDbParameter(DbCommand cmd, SqlParam prm)
    {
        DbParameter param = cmd.CreateParameter();

        string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;
        param.ParameterName = baseParameterName;
        cmd.CommandText = cmd.CommandText.Replace($"@{baseParameterName}", $":{baseParameterName}");

        return param;
    }

    public bool UsePrimaryKeyPropertyForInsert()
    {
        return true;
    }

    public async Task InsertAsync(DbModel model, DbCommand cmd, bool insertPrimaryKeyColumn, DbModelSaveType saveType)
    {
        await cmd.ExecuteNonQueryAsync();

        if (insertPrimaryKeyColumn || saveType == DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn)
        {
            return;
        }

        foreach (DbParameter prm in cmd.Parameters)
        {
            if (prm.Direction != ParameterDirection.Output)
                continue;

            if (model.HasPrimaryKey() && prm.Value != null && prm.Value != DBNull.Value)
            {
                var pkProp = model.GetPrimaryKeyProperties().First();

                if (pkProp.PropertyType == typeof(int))
                {
                    pkProp.SetValue(model, Convert.ToInt32(prm.Value), null);
                }
                else if (pkProp.PropertyType == typeof(long))
                {
                    pkProp.SetValue(model, Convert.ToInt64(prm.Value), null);
                }
                else
                {
                    pkProp.SetValue(model, prm.Value, null);
                }
            }

            break;
        }
    }

    public void EnsureTempTable(string table)
    {
        string simplifiedName = table.IndexOf(".") > 0 ? table.Substring(table.IndexOf(".") + 1) : table;

        if (!simplifiedName.StartsWith("temp_", StringComparison.OrdinalIgnoreCase)
            && !simplifiedName.StartsWith("tmp_", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"{table} must begin with temp_ or tmp_ .");
        }
    }

    public void SetupFunctionCall(DbCommand cmd, string sql, params SqlParam[] parameters)
    {
        ((IDbSpeciffic)this).CommonSetupFunctionCall(cmd, sql, parameters);

        cmd.CommandText += " from dual";
    }

    public string GetGetServerDateTimeQuery()
    {
        string sql = "SELECT sysdate from dual";

        return sql;
    }

    public (string, IEnumerable<SqlParam>) GetInsertedIdQuery(string table, DbModel model, string firstPropertyName)
    {
        string sql;

        var pkProps = model.GetPrimaryKeyProperties();

        if (pkProps.Count == 1)
            sql = $" returning {model.GetMappedProperty(pkProps.First().Name)} into @p_out_id ";
        else
            sql = $" returning {model.GetMappedProperty(firstPropertyName)} into @p_out_id ";

        SqlParam prm = new SqlParam($"@p_out_id") { paramDirection = ParameterDirection.Output };

        return (sql, new[] { prm });
    }

    public async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchWithSequenceAsync<T>(
        List<T> list,
        IZenDbConnection conn,
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
        List<string>? primaryKeyColumns = firstModel.GetPrimaryKeyColumns();

        for (int i = 1; i < list.Count; i++)
        {
            T model = list[i];

            k++;
            firstParam = true;

            sbInsert.Append($"INSERT INTO {table} (");
            StringBuilder sbInsertValues = new StringBuilder();

            foreach (PropertyInfo propertyInfo in propertiesToInsert)
            {
                string? dbCol = firstModel!.GetMappedProperty(propertyInfo.Name);

                if (!insertPrimaryKeyColumn
                    && string.IsNullOrEmpty(sequence2UseForPrimaryKey)
                    && !string.IsNullOrEmpty(dbCol)
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

                if (firstModel != null && firstModel.IsClobDataType(propertyInfo))
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

    public async Task<Tuple<string, SqlParam[]>> PrepareBulkInsertBatchAsync<T>(
        List<T> list,
        IZenDbConnection conn,
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

                string? dbCol = firstModel!.GetMappedProperty(propertyInfo.Name);

                sbInsert.Append($" {dbCol} ");
                sbInsertValues.Append($" @p_{propertyInfo.Name}_{k} ");

                SqlParam prm = new SqlParam($"@p_{propertyInfo.Name}_{k}", propertyInfo.GetValue(model));

                if (firstModel != null && firstModel.IsClobDataType(propertyInfo))
                    prm.isClob = true;

                insertParams.Add(prm);
            }

            sbInsert.Append(") VALUES (").Append(sbInsertValues).AppendLine(")");
        }

        sbInsert.AppendLine("SELECT 1 FROM dual");

        return new Tuple<string, SqlParam[]>(sbInsert.ToString(), insertParams.ToArray());
    }
}
