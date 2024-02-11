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
using Zen.DbAccess.Interfaces;
using Zen.DbAccess.Utils;

namespace Zen.DbAccess.Factories;

public class DbConnectionFactory : IDbConnectionFactory
{
    private string? _connStr;
    private bool? _commitNoWait;
    private DbConnectionType? _dbType;
    private string? _timeZone;
    private IConfiguration? _configuration;

    private static Dictionary<DbConnectionType, IDbSpeciffic> _databaseSpeciffic = new Dictionary<DbConnectionType, IDbSpeciffic>();
    private static Dictionary<string, DbConnectionType> _connectionTypes = new Dictionary<string, DbConnectionType>();

    public DbConnectionFactory(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    private DbConnectionFactory(DbConnectionType dbType, string conn_str, bool commitNoWait = true, string timeZone = "")
    {
        _dbType = dbType;
        _connStr = conn_str;
        _commitNoWait = commitNoWait;
        _timeZone = timeZone;
    }

    public DbConnectionType DbType
    {
        get
        {
            if (_dbType == null)
                throw new NullReferenceException(nameof(DbType));

            return _dbType.Value;
        }
    }

    public IDbConnectionFactory Get(string connectionStringName)
    {
        if (_configuration == null)
            throw new NullReferenceException(nameof(_configuration));

        if (!_connectionTypes.TryGetValue(connectionStringName, out DbConnectionType dbType))
        {
            throw new NotImplementedException($"Connection string {connectionStringName} is not configured.");
        }

        IDbConnectionFactory factory = CreateFromConfiguration(_configuration!, connectionStringName, dbType);

        return factory;
    }

    public IDbConnectionFactory Create(DbConnectionType dbType, string conn_str, bool commitNoWait = true, string timeZone = "")
    {
        IDbConnectionFactory factory = new DbConnectionFactory(dbType, conn_str, commitNoWait = true, timeZone = "");

        return factory;
    }

    public static void RegisterDatabaseFactory(string factoryName, DbProviderFactory dbProviderFactory, IDbSpeciffic? dbSpeciffic = null)
    {
        DbProviderFactories.RegisterFactory(factoryName, dbProviderFactory);
        RegisterDatabaseSpeciffc(factoryName, dbSpeciffic);
    }

    public static void RegisterDatabaseFactories((string factoryName, DbProviderFactory dbProviderFactory, IDbSpeciffic? dbSpeciffic)[] factories)
    {
        foreach (var factory in factories)
        {
            DbProviderFactories.RegisterFactory(factory.factoryName, factory.dbProviderFactory);
            RegisterDatabaseSpeciffc(factory.factoryName, factory.dbSpeciffic);
        }
    }

    public static void RegisterConnectionDI(DbConnectionType dbType, string connectionStringName)
    {
        _connectionTypes[connectionStringName] = dbType;
    }

    public void RegisterConnection(DbConnectionType dbType, string connectionStringName)
    {
        _connectionTypes[connectionStringName] = dbType;
    }

    private static void RegisterDatabaseSpeciffc(string factoryName, IDbSpeciffic? dbSpeciffic)
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
            _databaseSpeciffic[dbType] = dbSpeciffic;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <returns>A database connection object. The database connection is opened.</returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<IZenDbConnection> BuildAsync()
    {
        DbConnection conn;

        string driverClass = _dbType switch
        {
            DbConnectionType.SqlServer => DbFactoryNames.SQL_SERVER,
            DbConnectionType.Oracle => DbFactoryNames.ORACLE,
            DbConnectionType.Postgresql => DbFactoryNames.POSTGRESQL,
            DbConnectionType.Sqlite => DbFactoryNames.SQLITE,
            _ => throw new NotImplementedException($"Not implemented {_dbType}")
        };

        DbProviderFactory dbProviderFactory = DbProviderFactories.GetFactory(driverClass);
        conn = dbProviderFactory.CreateConnection();
        conn.ConnectionString = _connStr;

        _databaseSpeciffic.TryGetValue(_dbType!.Value, out IDbSpeciffic? dbSpeciffic);
        ZenDbConnection connection = new ZenDbConnection(conn, _dbType!.Value, dbSpeciffic);

        if (_dbType == DbConnectionType.Oracle)
        {
            await conn.OpenAsync();

            if (_commitNoWait!.Value)
            {
                string sql = "alter session set commit_logging=batch commit_wait=nowait";
                await sql.ExecuteNonQueryAsync(connection);
            }

            if (!string.IsNullOrEmpty(_timeZone))
            {
                string sql = $"alter session set time_zone = '{_timeZone.Replace("'", "''").Replace("&", "")}' ";
                await sql.ExecuteNonQueryAsync(connection);
            }
        }
        else if (_dbType == DbConnectionType.Postgresql)
        {
            await conn.OpenAsync();

            if (!string.IsNullOrEmpty(_timeZone) && !_connStr!.Contains(";Timezone=", StringComparison.OrdinalIgnoreCase))
            {
                _connStr += $"Timezone={_timeZone};";
            }

            if (_commitNoWait!.Value)
            {
                string sql = "SET synchronous_commit = 'off'";
                await sql.ExecuteNonQueryAsync(connection);
            }
        }

        if (conn.State != ConnectionState.Open)
            await conn.OpenAsync();

        return connection;
    }

    private DbConnectionFactory CreateFromConfiguration(
        IConfiguration configuration,
        string connectionStringName,
        DbConnectionType dbType)
    {
        DbConnectionFactory? dbConnectionFactory = null;
        string? connString = configuration.GetConnectionString(connectionStringName);

        if (!string.IsNullOrEmpty(connString))
        {
            dbConnectionFactory = new DbConnectionFactory(
                dbType,
                connString,
                commitNoWait: true,
                "UTC"
            );

            return dbConnectionFactory;
        }

        ConnectionStringModel? connStringModel = configuration?
                .GetSection("DatabaseConnections")?
                .GetSection(connectionStringName)?
                .Get<ConnectionStringModel>();

        if (connStringModel == null)
            throw new NullReferenceException(nameof(connStringModel));

        dbConnectionFactory = new DbConnectionFactory(
            connStringModel.DbConnectionType,
            connStringModel.ConnectionString,
            commitNoWait: true,
            connStringModel.TimeZone
        );

        return dbConnectionFactory;
    }
}
