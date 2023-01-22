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

namespace Zen.DbAccess.Repositories;

public abstract class BaseRepository
{
    protected DbConnectionFactory? _dbConnectionFactory;

    public BaseRepository()
    {

    }

    protected async Task<ResponseModel> RunQueryAsync(DbModel model, string table, string procedure2Execute)
    {
        if (_dbConnectionFactory == null)
            throw new NullReferenceException(nameof(_dbConnectionFactory));

        using DbConnection conn = await _dbConnectionFactory.BuildAndOpenAsync();

        var rez = await RunQueryAsync(conn, model, table, procedure2Execute);

        await conn.CloseAsync();

        return rez;
    }

    protected async Task<ResponseModel> RunQueryAsync(DbConnection conn, DbModel model, string table, string procedure2Execute)
    {
        await ClearTempTableAsync(conn, table);

        await model.SaveAsync(DbModelSaveType.InsertOnly, conn, table);

        List<SqlParam> result = await procedure2Execute.ExecuteProcedureAsync(conn,
            new SqlParam("lError") { paramDirection = ParameterDirection.Output },
            new SqlParam("sError") { size = 32767, paramDirection = ParameterDirection.Output }
        );

        ResponseModel rez = new ResponseModel
        {
            is_error = Convert.ToInt32(result.FirstOrDefault(x => x.name == "lError")?.value ?? (object)0) == 1,
            error_message = result.FirstOrDefault(x => x.name == "sError")?.value?.ToString() ?? ""
        };

        return rez;
    }

    protected async Task<ResponseModel> RunQueryAsync<T>(string table, string procedure2Execute, List<T> models) where T : DbModel
    {
        if (_dbConnectionFactory == null)
            throw new NullReferenceException(nameof(_dbConnectionFactory));

        using DbConnection conn = await _dbConnectionFactory.BuildAndOpenAsync();

        var rez = await RunQueryAsync<T>(conn, table, procedure2Execute, models);

        await conn.CloseAsync();

        return rez;
    }

    protected async Task<ResponseModel> RunQueryAsync<T>(DbConnection conn, string table, string procedure2Execute, List<T> models) where T : DbModel
    {
        await ClearTempTableAsync(conn, table);

        await models.SaveAllAsync(DbModelSaveType.InsertOnly, conn, table);

        List<ResponseModel> rez = await RunProcedureAsync<ResponseModel>(conn, procedure2Execute);

        return rez.First();
    }

    protected async Task<List<T>> RunProcedureAsync<T>(string procedure2Execute, params SqlParam[] parameters) where T : ResponseModel
    {
        if (_dbConnectionFactory == null)
            throw new NullReferenceException(nameof(_dbConnectionFactory));

        using DbConnection conn = await _dbConnectionFactory.BuildAndOpenAsync();

        var rez = await RunProcedureAsync<T>(conn, procedure2Execute, parameters);

        await conn.CloseAsync();

        return rez;
    }

    protected async Task<List<T>> RunProcedureAsync<T>(DbConnection conn, string procedure2Execute, params SqlParam[] parameters) where T : ResponseModel
    {
        DataSet? result = await procedure2Execute.ExecuteProcedure2DataSetAsync(conn, parameters);

        if (result == null || result.Tables.Count == 0)
            throw new Exception("empty query response");

        var rez = result.Tables[0].ToList<T>();

        return rez;
    }

    protected async Task CreateTempTableAndGrantAccessToProcedureOwnerAsync(DbConnection conn, string table, string tempTableDDL, string procedure)
    {
        if (conn is OracleConnection)
            return; // the temp table must exist in Oracle as global temp

        string sql = "p$create_temp_table_and_grant_access_to_procedure";

        await sql.ExecuteProcedureAsync(conn,
            new SqlParam("sTempTable", table),
            new SqlParam("sTempTableDDL", tempTableDDL),
            new SqlParam("sProcedure", procedure));
    }

    protected async Task ClearTempTableAsync(DbConnection conn, string table)
    {
        if (!table.StartsWith("temp_", StringComparison.OrdinalIgnoreCase)
            && !table.StartsWith("tmp_", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"{table} must begin with temp_ or tmp_ .");
        }

        string sql = $"delete from {table}";
        await sql.ExecuteNonQueryAsync(conn);
    }
}
