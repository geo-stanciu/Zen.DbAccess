using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Shared.Models;
using System.Reflection.Metadata.Ecma335;
using System.Reflection;

namespace Zen.DbAccess.Repositories;

public abstract class BaseRepository
{
    protected DbConnectionFactory? _dbConnectionFactory;

    public BaseRepository()
    {

    }

    protected async Task<ResponseModel> RunQueryAsync(DbModel model, string table, string procedure2Execute)
    {
        ResponseModel rez = (await RunProcedureAsync<ResponseModel, DbModel>(
            table: table,
            tempTableDDL: null,
            insertPrimaryKeyColumn: false,
            procedure2Execute: procedure2Execute,
            models: new List<DbModel> { model })
        ).Single();

        return rez;
    }

    protected Task<List<T>> RunProcedureAsync<T>(string procedure2Execute, params SqlParam[] parameters) where T : ResponseModel
    {
        return RunProcedureAsync<T, DbModel>(table: null, tempTableDDL: null, models: null, insertPrimaryKeyColumn: false, procedure2Execute, parameters);
    }

    protected Task<List<T>> RunProcedureAsync<T>(string? table, string? tempTableDDL, string procedure2Execute, params SqlParam[] parameters) where T : ResponseModel
    {
        return RunProcedureAsync<T, DbModel>(table, tempTableDDL, models: null, insertPrimaryKeyColumn: false, procedure2Execute: procedure2Execute, parameters);
    }

    protected async Task<List<T>> RunProcedureAsync<T, TDBModel>(string? table, string? tempTableDDL, List<TDBModel>? models, bool? insertPrimaryKeyColumn, string procedure2Execute, params SqlParam[] parameters) where T : ResponseModel where TDBModel : DbModel
    {
        if (_dbConnectionFactory == null)
            throw new NullReferenceException(nameof(_dbConnectionFactory));

        using DbConnection conn = await _dbConnectionFactory.BuildAndOpenAsync();
        using DbTransaction tx = await conn.BeginTransactionAsync();

        try
        {
            if (!string.IsNullOrEmpty(table) && !string.IsNullOrEmpty(tempTableDDL))
                await CreateTempTableAndGrantAccessToProcedureOwnerAsync(conn, table, tempTableDDL, procedure2Execute);
            else if (!string.IsNullOrEmpty(table))
                await ClearTempTableAsync(conn, table);

            if (models != null && !string.IsNullOrEmpty(table))
                await models.SaveAllAsync(DbModelSaveType.InsertOnly, conn, table, runAllInTheSameTransaction: false, insertPrimaryKeyColumn: insertPrimaryKeyColumn ?? false);

            var rez = await RunProcedureAsync<T>(conn, procedure2Execute, parameters);
            await tx.CommitAsync();

            return rez;
        }
        catch
        {
            await tx.RollbackAsync();
            throw;
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    protected async Task<List<T>> RunProcedureAsync<T>(DbConnection conn, string procedure2Execute, params SqlParam[] parameters) where T : ResponseModel
    {
        DataSet? result = await procedure2Execute.ExecuteProcedure2DataSetAsync(conn, parameters);

        if (result == null || result.Tables.Count == 0)
            throw new Exception("empty query response");

        var rez = result.Tables[0].ToList<T>();

        return rez;
    }

    private async Task CreateTempTableAndGrantAccessToProcedureOwnerAsync(DbConnection conn, string table, string tempTableDDL, string procedure)
    {
        if (conn is OracleConnection)
        {
            await ClearTempTableAsync(conn, table);
            return; // the temp table must exist in Oracle as global temp - cleanup and return
        }

        string sql = "p$create_temp_table_and_grant_access_to_procedure";

        await sql.ExecuteProcedureAsync(conn,
            new SqlParam("@sTempTable", table),
            new SqlParam("@sTempTableDDL", tempTableDDL),
            new SqlParam("@sProcedure", procedure));
    }

    protected async Task ClearTempTableAsync(DbConnection conn, string table)
    {
        if (conn is OracleConnection)
        {
            string simplifiedName = table.IndexOf(".") > 0 ? table.Substring(table.IndexOf(".") + 1) : table;

            if (!simplifiedName.StartsWith("temp_", StringComparison.OrdinalIgnoreCase)
                && !simplifiedName.StartsWith("tmp_", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"{table} must begin with temp_ or tmp_ .");
            }
        }
        else if (!table.StartsWith("temp_", StringComparison.OrdinalIgnoreCase)
            && !table.StartsWith("tmp_", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"{table} must begin with temp_ or tmp_ .");
        }

        string sql = $"delete from {table}";
        await sql.ExecuteNonQueryAsync(conn);
    }
}
