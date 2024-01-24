using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Zen.DbAccess.Constants;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Models;

namespace Zen.DbAccess.Factories;

public class DbConnectionFactory
{
    private string _connStr;
    private bool _commitNoWait;

    public DbConnectionType DbType { get; private set; }
    public string TimeZone { get; private set; }

    public static DbConnectionType DefaultDbType { get; set; } = DbConnectionType.Oracle;

    public static Dictionary<DbConnectionType, IDbSpeciffic> DatabaseSpeciffic { get; private set; } = new Dictionary<DbConnectionType, IDbSpeciffic>();

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

    public static void RegisterDatabaseFactory(string factoryName, DbProviderFactory dbProviderFactory, IDbSpeciffic? dbSpeciffic = null)
    {
        DbProviderFactories.RegisterFactory(factoryName, dbProviderFactory);
        RegisterDatabaseSpeciffc(factoryName, dbProviderFactory, dbSpeciffic);
    }

    public static void RegisterDatabaseFactories((string factoryName, DbProviderFactory dbProviderFactory, IDbSpeciffic? dbSpeciffic)[] factories)
    {
        foreach (var factory in factories)
        {
            DbProviderFactories.RegisterFactory(factory.factoryName, factory.dbProviderFactory);
            RegisterDatabaseSpeciffc(factory.factoryName, factory.dbProviderFactory, factory.dbSpeciffic);
        }
    }

    private static void RegisterDatabaseSpeciffc(string factoryName, DbProviderFactory dbProviderFactory, IDbSpeciffic? dbSpeciffic)
    {
        DbConnectionType dbType = factoryName switch
        {
            DbFactoryNames.SQL_SERVER => DbConnectionType.SqlServer,
            DbFactoryNames.ORACLE => DbConnectionType.Oracle,
            DbFactoryNames.POSTGRESQL => DbConnectionType.Postgresql,
            DbFactoryNames.SQLITE => DbConnectionType.Sqlite,
            _ => throw new NotImplementedException($"Not implemented {factoryName}")
        };

        if (dbSpeciffic != null)
            DatabaseSpeciffic[dbType] = dbSpeciffic;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <returns>A database connection object. The database connection is opened.</returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<DbConnection> BuildAndOpenAsync()
    {
        DbConnection conn;

        string driverClass = DbType switch
        {
            DbConnectionType.SqlServer => DbFactoryNames.SQL_SERVER,
            DbConnectionType.Oracle => DbFactoryNames.ORACLE,
            DbConnectionType.Postgresql => DbFactoryNames.POSTGRESQL,
            DbConnectionType.Sqlite => DbFactoryNames.SQLITE,
            _ => throw new NotImplementedException($"Not implemented {DbType}")
        };

        DbProviderFactory dbProviderFactory = DbProviderFactories.GetFactory(driverClass);
        conn = dbProviderFactory.CreateConnection();
        conn.ConnectionString = _connStr;

        if (DbType == DbConnectionType.Oracle)
        {
            await conn.OpenAsync();

            if (_commitNoWait)
            {
                string sql = "alter session set commit_logging=batch commit_wait=nowait";
                await sql.ExecuteNonQueryAsync(DbType, conn);
            }

            if (!string.IsNullOrEmpty(TimeZone))
            {
                string sql = $"alter session set time_zone = '{TimeZone.Replace("'", "''").Replace("&", "")}' ";
                await sql.ExecuteNonQueryAsync(DbType, conn);
            }
        }
        else if (DbType == DbConnectionType.Postgresql)
        {
            await conn.OpenAsync();

            if (!string.IsNullOrEmpty(TimeZone) && !_connStr.Contains(";Timezone=", StringComparison.OrdinalIgnoreCase))
            {
                _connStr += $"Timezone={TimeZone};";
            }

            if (_commitNoWait)
            {
                string sql = "SET synchronous_commit = 'off'";
                await sql.ExecuteNonQueryAsync(DbType, conn);
            }
        }

        if (conn.State != ConnectionState.Open)
            await conn.OpenAsync();

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
