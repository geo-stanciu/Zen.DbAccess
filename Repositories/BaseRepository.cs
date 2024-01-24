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
using System.Reflection;
using System.Data.SqlClient;
using Npgsql;
using System.Reflection.Emit;

namespace Zen.DbAccess.Repositories;

public abstract class BaseRepository
{
    protected DbConnectionFactory? _dbConnectionFactory;

    public BaseRepository()
    {

    }

    protected async Task<ResponseModel> RunQueryAsync(
        DbModel model,
        string table,
        string procedure2Execute)
    {
        ResponseModel rez = (await RunProcedureAsync<ResponseModel, DbModel>(
            table: table,
            insertPrimaryKeyColumn: false,
            bulkInsert: false,
            sequence2UseForPrimaryKey: "",
            procedure2Execute: procedure2Execute,
            CreateTempTableCallBack: null,
            models: new List<DbModel> { model })
        ).Single();

        return rez;
    }

    protected Task<List<T>> RunProcedureAsync<T>(
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
    {
        return RunProcedureAsync<T, DbModel>(
            table: null,
            models: null,
            insertPrimaryKeyColumn: false,
            procedure2Execute: procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected async Task<List<T>> RunProcedureAsync<T, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        Func<DbConnection, DbTransaction?, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel where TDBModel : DbModel
    {
        return await (RunProcedureAsync<T, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            false,
            sequence2UseForPrimaryKey: "",
            procedure2Execute,
            CreateTempTableCallBack,
            parameters)
        );

    }

    //prot

    protected async Task<List<T>> RunProcedureAsync<T, TDBModel>(
        string? table,
        List<TDBModel>? models, 
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        Func<DbConnection, DbTransaction?, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel where TDBModel : DbModel
    {
        if (_dbConnectionFactory == null)
            throw new NullReferenceException(nameof(_dbConnectionFactory));

        using DbConnection conn = await _dbConnectionFactory.BuildAndOpenAsync();
        using DbTransaction tx = await conn.BeginTransactionAsync();

        try
        {
            if (CreateTempTableCallBack != null)
            {
                await CreateTempTableCallBack(conn, tx);
            }
            
            if (!string.IsNullOrEmpty(table))
            {
                await ClearTempTableAsync(conn, tx, table);
            }

            if (models != null && !string.IsNullOrEmpty(table))
            {
                if (bulkInsert ?? false)
                {
                    await models.BulkInsertAsync(
                        conn,
                        tx, 
                        table, 
                        runAllInTheSameTransaction: false, 
                        insertPrimaryKeyColumn: insertPrimaryKeyColumn ?? false, 
                        sequence2UseForPrimaryKey ?? ""
                    );
                }
                else
                {
                    await models.SaveAllAsync(
                        DbModelSaveType.InsertOnly, 
                        conn,
                        tx, 
                        table, 
                        runAllInTheSameTransaction: false, 
                        insertPrimaryKeyColumn: insertPrimaryKeyColumn ?? false
                    );
                }
            }

            var rez = await RunProcedureAsync<T>(conn, tx, procedure2Execute, parameters);
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

    protected async Task<List<T>> RunProcedureAsync<T>(
        DbConnection conn, 
        DbTransaction? tx,
        string procedure2Execute, 
        params SqlParam[] parameters) where T : ResponseModel
    {
        DataTable? result = await procedure2Execute.ExecuteProcedure2DataTableAsync(conn, tx, parameters);

        if (result == null)
            throw new Exception("empty query response");

        var rez = result.ToList<T>();

        return rez;
    }

    protected async Task ClearTempTableAsync(DbConnection conn, DbTransaction? tx, string table)
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
        else if (conn is SqlConnection)
        {
            if (!table.StartsWith("##", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"{table} must begin with ##.");
            }
        }
        else if (!table.StartsWith("temp_", StringComparison.OrdinalIgnoreCase)
            && !table.StartsWith("tmp_", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"{table} must begin with temp_ or tmp_ .");
        }

        string sql = $"delete from {table}";
        await sql.ExecuteNonQueryAsync(conn, tx);
    }
}
