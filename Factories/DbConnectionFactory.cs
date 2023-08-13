using Microsoft.Extensions.Configuration;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Threading.Tasks;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Shared.Models;

namespace Zen.DbAccess.Factories;

public class DbConnectionFactory
{
    private string _connStr;
    private bool _commitNoWait;

    public DbConnectionType DbType { get; private set; }
    public string TimeZone { get; private set; }

    public static DbConnectionType DefaultDbType { get; set; } = DbConnectionType.Oracle;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="conn_str"></param>
    /// <param name="commitNoWait">Signals if Oracle and PostgreSql are set to not wait on synchronus commit</param>
    /// <param name="timeZone">The time zone</param>
    public DbConnectionFactory(string conn_str, bool commitNoWait = true, string timeZone = "")
    {
        DbType = DefaultDbType;
        _connStr = conn_str;
        _commitNoWait = commitNoWait;
        TimeZone = timeZone;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="dbType"></param>
    /// <param name="conn_str"></param>
    /// <param name="commitNoWait">Signals if Oracle and PostgreSql are set to not wait on synchronus commit</param>
    public DbConnectionFactory(DbConnectionType dbType, string conn_str, bool commitNoWait = true, string timeZone = "")
    {
        DbType = dbType;
        _connStr = conn_str;
        _commitNoWait = commitNoWait;
        TimeZone = timeZone;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <returns>A database connection object. The database connection is opened.</returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<DbConnection> BuildAndOpenAsync()
    {
        DbConnection conn;

        if (DbType == DbConnectionType.SqlServer)
        {
            conn = new SqlConnection(_connStr);
        }
        else if (DbType == DbConnectionType.Oracle)
        {
            conn = new OracleConnection(_connStr);
            await conn.OpenAsync().ConfigureAwait(false);

            if (_commitNoWait)
            {
                string sql = "alter session set commit_logging=batch commit_wait=nowait";
                await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);
            }

            if (!string.IsNullOrEmpty(TimeZone))
            {
                string sql = $"alter session set time_zone = '{TimeZone.Replace("'", "''").Replace("&", "")}' ";
                await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);
            }
        }
        else if (DbType == DbConnectionType.Postgresql)
        {
            if (!string.IsNullOrEmpty(TimeZone) && !_connStr.Contains(";Timezone=", StringComparison.OrdinalIgnoreCase))
            {
                _connStr += $"Timezone={TimeZone};";
            }

            conn = new NpgsqlConnection(_connStr);
            await conn.OpenAsync().ConfigureAwait(false);

            if (_commitNoWait)
            {
                string sql = "SET synchronous_commit = 'off'";
                await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);
            }
        }
        else if (DbType == DbConnectionType.Sqlite)
        {
            conn = new SQLiteConnection(_connStr);
        }
        else
        {
            throw new NotImplementedException($"Unknown database type {DbType}");
        }

        if (conn.State != ConnectionState.Open)
            await conn.OpenAsync().ConfigureAwait(false);

        return conn;
    }

    public static DbConnectionFactory FromConfiguration(
        IConfiguration configuration,
        string connectionStringName)
    {
        ConnectionStringModel? connStringModel = configuration?
                .GetSection("DatabaseConnections")?
                .GetSection(connectionStringName)?
                .Get<ConnectionStringModel>();

        if (connStringModel == null)
            throw new NullReferenceException(nameof(connStringModel));

        DbConnectionFactory dbConnectionFactory = new DbConnectionFactory(
            connStringModel.DbConnectionType,
            connStringModel.ConnectionString,
            commitNoWait: true,
            connStringModel.TimeZone
        );

        return dbConnectionFactory;
    }
}
