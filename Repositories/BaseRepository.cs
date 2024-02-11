using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Models;
using Zen.DbAccess.Interfaces;

namespace Zen.DbAccess.Repositories;

public abstract class BaseRepository
{
    protected IDbConnectionFactory? _dbConnectionFactory;

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

    protected Task<List<T>> RunProcedureAsync<T, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel where TDBModel : DbModel
    {
        return RunProcedureAsync<T, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            false,
            sequence2UseForPrimaryKey: "",
            procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);

    }

    protected async Task<List<T>> RunProcedureAsync<T, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        Func<IZenDbConnection, Task>? CreateTempTableCallBack,
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

    protected Task<List<T>> RunProcedureAsync<T, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel where TDBModel : DbModel
    {
        return RunProcedureAsync<T, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            bulkInsert,
            sequence2UseForPrimaryKey,
            procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected async Task<List<T>> RunProcedureAsync<T, TDBModel>(
        string? table,
        List<TDBModel>? models, 
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        Func<IZenDbConnection, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel where TDBModel : DbModel
    {
        if (_dbConnectionFactory == null)
            throw new NullReferenceException(nameof(_dbConnectionFactory));

        await using IZenDbConnection conn = await _dbConnectionFactory.BuildAsync();
        await conn.BeginTransactionAsync();

        try
        {
            if (CreateTempTableCallBack != null)
            {
                await CreateTempTableCallBack(conn);
            }
            
            if (!string.IsNullOrEmpty(table))
            {
                await ClearTempTableAsync(conn, table);
            }

            if (models != null && !string.IsNullOrEmpty(table))
            {
                if (bulkInsert ?? false)
                {
                    await models.BulkInsertAsync(
                        conn,
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
                        table, 
                        runAllInTheSameTransaction: false, 
                        insertPrimaryKeyColumn: insertPrimaryKeyColumn ?? false
                    );
                }
            }

            var rez = await RunProcedureAsync<T>(conn, procedure2Execute, parameters);
            await conn.CommitAsync();

            return rez;
        }
        catch
        {
            await conn.RollbackAsync();
            throw;
        }
    }

    protected async Task<List<T>> RunProcedureAsync<T>(
        IZenDbConnection conn,
        string procedure2Execute, 
        params SqlParam[] parameters) where T : ResponseModel
    {
        DataTable? result = await procedure2Execute.ExecuteProcedure2DataTableAsync(conn, parameters);

        if (result == null)
            throw new Exception("empty query response");

        var rez = result.ToList<T>();

        return rez;
    }

    protected async Task ClearTempTableAsync(IZenDbConnection conn, string table)
    {
        conn.DatabaseSpeciffic.EnsureTempTable(table);

        string sql = $"delete from {table}";
        await sql.ExecuteNonQueryAsync(conn);
    }
}
