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

    protected async Task<ResponseModel> RunQueryAsync(
        DbModel model,
        string table,
        string procedure2Execute,
        bool isTableProcedure)
    {
        ResponseModel rez = (await RunProcedureAsync<ResponseModel, DbModel>(
            table: table,
            insertPrimaryKeyColumn: false,
            bulkInsert: false,
            sequence2UseForPrimaryKey: "",
            procedure2Execute: procedure2Execute,
            isTableProcedure: isTableProcedure,
            CreateTempTableCallBack: null,
            models: new List<DbModel> { model })
        ).Single();

        return rez;
    }

    protected Task<(List<T>, List<T2>, List<T3>, List<T4>, List<T5>)> RunProcedureAsync<T, T2, T3, T4, T5>(
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
                                      where T5 : ResponseModel
    {
        return RunProcedureAsync<T, T2, T3, T4, T5, DbModel>(
            table: null,
            models: null,
            insertPrimaryKeyColumn: false,
            procedure2Execute: procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected Task<(List<T>, List<T2>, List<T3>, List<T4>)> RunProcedureAsync<T, T2, T3, T4>(
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
    {
        return RunProcedureAsync<T, T2, T3, T4, DbModel>(
            table: null,
            models: null,
            insertPrimaryKeyColumn: false,
            procedure2Execute: procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected Task<(List<T>, List<T2>, List<T3>)> RunProcedureAsync<T, T2, T3>(
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
    {
        return RunProcedureAsync<T, T2, T3, DbModel>(
            table: null,
            models: null,
            insertPrimaryKeyColumn: false,
            procedure2Execute: procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected Task<(List<T>, List<T2>)> RunProcedureAsync<T, T2>(
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
    {
        return RunProcedureAsync<T, T2, DbModel>(
            table: null,
            models: null,
            insertPrimaryKeyColumn: false,
            procedure2Execute: procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);
    }


    protected Task<List<T>> RunTableProcedureAsync<T>(
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
    {
        return RunProcedureAsync<T>(
            procedure2Execute: procedure2Execute,
            isTableProcedure: true,
            parameters);
    }

    protected Task<List<T>> RunProcedureAsync<T>(
        string procedure2Execute,
        bool isTableProcedure,
        params SqlParam[] parameters) where T : ResponseModel
    {
        return RunProcedureAsync<T, DbModel>(
            table: null,
            models: null,
            insertPrimaryKeyColumn: false,
            procedure2Execute: procedure2Execute,
            isTableProcedure: isTableProcedure,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected Task<List<T>> RunProcedureAsync<T>(
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
    {
        return RunProcedureAsync<T>(
            procedure2Execute: procedure2Execute,
            isTableProcedure: false,
            parameters);
    }

    protected Task<(List<T>, List<T2>, List<T3>, List<T4>, List<T5>)> RunProcedureAsync<T, T2, T3, T4, T5, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
                                      where T5 : ResponseModel
                                      where TDBModel : DbModel
    {
        return RunProcedureAsync<T, T2, T3, T4, T5, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            false,
            sequence2UseForPrimaryKey: "",
            procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);

    }

    protected Task<(List<T>, List<T2>, List<T3>, List<T4>)> RunProcedureAsync<T, T2, T3, T4, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
                                      where TDBModel : DbModel
    {
        return RunProcedureAsync<T, T2, T3, T4, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            false,
            sequence2UseForPrimaryKey: "",
            procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);

    }

    protected Task<(List<T>, List<T2>, List<T3>)> RunProcedureAsync<T, T2, T3, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel where T2 : ResponseModel where T3 : ResponseModel where TDBModel : DbModel
    {
        return RunProcedureAsync<T, T2, T3, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            false,
            sequence2UseForPrimaryKey: "",
            procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);

    }

    protected Task<(List<T>, List<T2>)> RunProcedureAsync<T, T2, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel where T2 : ResponseModel where TDBModel : DbModel
    {
        return RunProcedureAsync<T, T2, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            false,
            sequence2UseForPrimaryKey: "",
            procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);

    }

    protected Task<List<T>> RunProcedureAsync<T, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        bool isTableProcedure,
        params SqlParam[] parameters) where T : ResponseModel where TDBModel : DbModel
    {
        return RunProcedureAsync<T, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            false,
            sequence2UseForPrimaryKey: "",
            procedure2Execute,
            isTableProcedure: isTableProcedure,
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
            isTableProcedure: false,
            CreateTempTableCallBack: null,
            parameters);

    }

    protected async Task<(List<T>, List<T2>, List<T3>, List<T4>, List<T5>)> RunProcedureAsync<T, T2, T3, T4, T5, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        Func<IZenDbConnection, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
                                      where T5 : ResponseModel
                                      where TDBModel : DbModel
    {
        return await (RunProcedureAsync<T, T2, T3, T4, T5, TDBModel>(
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

    protected async Task<(List<T>, List<T2>, List<T3>, List<T4>)> RunProcedureAsync<T, T2, T3, T4, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        Func<IZenDbConnection, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
                                      where TDBModel : DbModel
    {
        return await (RunProcedureAsync<T, T2, T3, T4, TDBModel>(
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

    protected async Task<(List<T>, List<T2>, List<T3>)> RunProcedureAsync<T, T2, T3, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        Func<IZenDbConnection, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel where T2 : ResponseModel where T3 : ResponseModel where TDBModel : DbModel
    {
        return await (RunProcedureAsync<T, T2, T3, TDBModel>(
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

    protected async Task<(List<T>, List<T2>)> RunProcedureAsync<T, T2, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        Func<IZenDbConnection, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel where T2 : ResponseModel where TDBModel : DbModel
    {
        return await (RunProcedureAsync<T, T2, TDBModel>(
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

    protected async Task<List<T>> RunProcedureAsync<T, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        string procedure2Execute,
        bool isTableProcedure,
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
            isTableProcedure: isTableProcedure,
            CreateTempTableCallBack,
            parameters)
        );
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
            isTableProcedure: false,
            CreateTempTableCallBack,
            parameters)
        );
    }

    protected Task<(List<T>, List<T2>, List<T3>, List<T4>, List<T5>)> RunProcedureAsync<T, T2, T3, T4, T5, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
                                      where T5 : ResponseModel
                                      where TDBModel : DbModel
    {
        return RunProcedureAsync<T, T2, T3, T4, T5, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            bulkInsert,
            sequence2UseForPrimaryKey,
            procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected Task<(List<T>, List<T2>, List<T3>, List<T4>)> RunProcedureAsync<T, T2, T3, T4, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
                                      where TDBModel : DbModel
    {
        return RunProcedureAsync<T, T2, T3, T4, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            bulkInsert,
            sequence2UseForPrimaryKey,
            procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected Task<(List<T>, List<T2>, List<T3>)> RunProcedureAsync<T, T2, T3, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel where T2 : ResponseModel where T3 : ResponseModel where TDBModel : DbModel
    {
        return RunProcedureAsync<T, T2, T3, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            bulkInsert,
            sequence2UseForPrimaryKey,
            procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected Task<(List<T>, List<T2>)> RunProcedureAsync<T, T2, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel where T2 : ResponseModel where TDBModel : DbModel
    {
        return RunProcedureAsync<T, T2, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            bulkInsert,
            sequence2UseForPrimaryKey,
            procedure2Execute,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected Task<List<T>> RunProcedureAsync<T, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        bool isTableProcedure,
        params SqlParam[] parameters) where T : ResponseModel where TDBModel : DbModel
    {
        return RunProcedureAsync<T, TDBModel>(
            table,
            models,
            insertPrimaryKeyColumn,
            bulkInsert,
            sequence2UseForPrimaryKey,
            procedure2Execute,
            isTableProcedure: isTableProcedure,
            CreateTempTableCallBack: null,
            parameters);
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
            isTableProcedure: false,
            CreateTempTableCallBack: null,
            parameters);
    }

    protected async Task<(List<T>, List<T2>, List<T3>, List<T4>, List<T5>)> RunProcedureAsync<T, T2, T3, T4, T5, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        Func<IZenDbConnection, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
                                      where T5 : ResponseModel
                                      where TDBModel : DbModel
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

            var rez = await RunProcedureAsync<T, T2, T3, T4, T5>(conn, procedure2Execute, parameters);
            await conn.CommitAsync();

            return rez;
        }
        catch
        {
            await conn.RollbackAsync();
            throw;
        }
    }

    protected async Task<(List<T>, List<T2>, List<T3>, List<T4>)> RunProcedureAsync<T, T2, T3, T4, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        Func<IZenDbConnection, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
                                      where TDBModel : DbModel
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

            var rez = await RunProcedureAsync<T, T2, T3, T4>(conn, procedure2Execute, parameters);
            await conn.CommitAsync();

            return rez;
        }
        catch
        {
            await conn.RollbackAsync();
            throw;
        }
    }

    protected async Task<(List<T>, List<T2>, List<T3>)> RunProcedureAsync<T, T2, T3, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        Func<IZenDbConnection, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel where T2 : ResponseModel where T3 : ResponseModel where TDBModel : DbModel
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

            var rez = await RunProcedureAsync<T, T2, T3>(conn, procedure2Execute, parameters);
            await conn.CommitAsync();

            return rez;
        }
        catch
        {
            await conn.RollbackAsync();
            throw;
        }
    }

    protected async Task<(List<T>, List<T2>)> RunProcedureAsync<T, T2, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        Func<IZenDbConnection, Task>? CreateTempTableCallBack,
        params SqlParam[] parameters) where T : ResponseModel where T2 : ResponseModel where TDBModel : DbModel
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

            var rez = await RunProcedureAsync<T, T2>(conn, procedure2Execute, parameters);
            await conn.CommitAsync();

            return rez;
        }
        catch
        {
            await conn.RollbackAsync();
            throw;
        }
    }

    protected async Task<List<T>> RunProcedureAsync<T, TDBModel>(
        string? table,
        List<TDBModel>? models,
        bool? insertPrimaryKeyColumn,
        bool? bulkInsert,
        string? sequence2UseForPrimaryKey,
        string procedure2Execute,
        bool isTableProcedure,
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

            var rez = await RunProcedureAsync<T>(conn, procedure2Execute, isTableProcedure, parameters);
            await conn.CommitAsync();

            return rez;
        }
        catch
        {
            await conn.RollbackAsync();
            throw;
        }
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

            var rez = await RunProcedureAsync<T>(conn, procedure2Execute, isTableProcedure: false, parameters);
            await conn.CommitAsync();

            return rez;
        }
        catch
        {
            await conn.RollbackAsync();
            throw;
        }
    }

    protected async Task<(List<T>, List<T2>, List<T3>, List<T4>, List<T5>)> RunProcedureAsync<T, T2, T3, T4, T5>(
        IZenDbConnection conn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
                                      where T5 : ResponseModel
    {
        var result = await procedure2Execute.QueryProcedureAsync<T, T2, T3, T4, T5>(conn, parameters);

        return result;
    }

    protected async Task<(List<T>, List<T2>, List<T3>, List<T4>)> RunProcedureAsync<T, T2, T3, T4>(
        IZenDbConnection conn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
                                      where T2 : ResponseModel
                                      where T3 : ResponseModel
                                      where T4 : ResponseModel
    {
        var result = await procedure2Execute.QueryProcedureAsync<T, T2, T3, T4>(conn, parameters);

        return result;
    }

    protected async Task<(List<T>, List<T2>, List<T3>)> RunProcedureAsync<T, T2, T3>(
        IZenDbConnection conn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel where T2 : ResponseModel where T3 : ResponseModel
    {
        var result = await procedure2Execute.QueryProcedureAsync<T, T2, T3>(conn, parameters);

        return result;
    }

    protected async Task<(List<T>, List<T2>)> RunProcedureAsync<T, T2>(
        IZenDbConnection conn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel where T2 : ResponseModel
    {
        var result = await procedure2Execute.QueryProcedureAsync<T, T2>(conn, parameters);

        return result;
    }

    protected async Task<List<T>> RunTableProcedureAsync<T>(
        IZenDbConnection conn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
    {
        var result = await procedure2Execute.QueryProcedureAsync<T>(conn, isTableProcedure: true, parameters);

        if (result == null)
            throw new Exception("empty query response");

        var rez = result.ToList<T>();

        return rez;
    }

    protected async Task<List<T>> RunProcedureAsync<T>(
        IZenDbConnection conn,
        string procedure2Execute,
        bool isTableProcedure,
        params SqlParam[] parameters) where T : ResponseModel
    {
        var result = await procedure2Execute.QueryProcedureAsync<T>(conn, isTableProcedure, parameters);

        if (result == null)
            throw new Exception("empty query response");

        var rez = result.ToList<T>();

        return rez;
    }

    protected async Task<List<T>> RunProcedureAsync<T>(
        IZenDbConnection conn,
        string procedure2Execute,
        params SqlParam[] parameters) where T : ResponseModel
    {
        var result = await procedure2Execute.QueryProcedureAsync<T>(conn, isTableProcedure: false, parameters);

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
