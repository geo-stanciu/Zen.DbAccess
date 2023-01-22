using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Shared.Models;

namespace Zen.DbAccess.Factories;

public class DbConnectionFactory
{
    private string conn_str;
    private DbConnectionType dbType;
    private bool commitNoWait;

    public static DbConnectionType DefaultDbType { get; set; } = DbConnectionType.Oracle;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="conn_str"></param>
    /// <param name="commitNoWait">Signals if Oracle and PostgreSql are set to not wait on synchronus commit</param>
    public DbConnectionFactory(string conn_str, bool commitNoWait = true)
    {
        dbType = DefaultDbType;
        this.conn_str = conn_str;
        this.commitNoWait = commitNoWait;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="dbType"></param>
    /// <param name="conn_str"></param>
    /// <param name="commitNoWait">Signals if Oracle and PostgreSql are set to not wait on synchronus commit</param>
    public DbConnectionFactory(DbConnectionType dbType, string conn_str, bool commitNoWait = true)
    {
        this.dbType = dbType;
        this.conn_str = conn_str;
        this.commitNoWait = commitNoWait;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <returns>A database connection object. The database connection is opened.</returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<DbConnection> BuildAndOpenAsync()
    {
        DbConnection conn;

        if (dbType == DbConnectionType.SqlServer)
        {
            conn = new SqlConnection(conn_str);
        }
        else if (dbType == DbConnectionType.Oracle)
        {
            conn = new OracleConnection(conn_str);

            if (commitNoWait)
            {
                await conn.OpenAsync();
                await OracleSetCommitNoWait(conn);
            }
        }
        else if (dbType == DbConnectionType.Postgresql)
        {
            conn = new NpgsqlConnection(conn_str);

            if (commitNoWait)
            {
                await conn.OpenAsync();
                await PostgresqlSetCommitNoWait(conn);
            }
        }
        else if (dbType == DbConnectionType.Sqlite)
        {
            conn = new SQLiteConnection(conn_str);
        }
        else
        {
            throw new NotImplementedException($"Unknown database type {dbType}");
        }

        if (conn.State != ConnectionState.Open)
            await conn.OpenAsync();

        return conn;
    }

    private async Task OracleSetCommitNoWait(DbConnection conn)
    {
        string sql = "alter session set commit_logging=batch commit_wait=nowait";

        using (DbCommand cmd = conn.CreateCommand())
        {
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task PostgresqlSetCommitNoWait(DbConnection conn)
    {
        string sql = "SET synchronous_commit = 'off'";

        using (DbCommand cmd = conn.CreateCommand())
        {
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public static DbConnectionFactory FromConfiguration(
        IConfiguration configuration,
        string connectionStringName)
    {
        string? connStringDetails = configuration?
            .GetSection("DatabaseConnections")?
            .GetSection(connectionStringName)?
            .Value;

        if (connStringDetails == null)
            throw new NullReferenceException("connection string details");

        ConnectionStringModel? connStringModel = JsonConvert.DeserializeObject<ConnectionStringModel>(connStringDetails);

        if (connStringModel == null)
            throw new NullReferenceException(nameof(connStringModel));

        DbConnectionFactory dbConnectionFactory = new DbConnectionFactory(
            connStringModel.DbConnectionType,
            connStringModel.ConnectionString);

        return dbConnectionFactory;
    }
}
