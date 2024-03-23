﻿using Microsoft.Extensions.Configuration;
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
using System.Diagnostics.Tracing;

namespace Zen.DbAccess.Factories;

public class DbConnectionFactory : IDbConnectionFactory
{
    private string? _connStr;
    private bool? _commitNoWait;
    private DbConnectionType? _dbType;
    private string? _timeZone;
    private IDbSpeciffic? _dbSpeciffic;

    public DbConnectionFactory(DbConnectionType dbType, string conn_str, IDbSpeciffic? dbSpeciffic = null, bool commitNoWait = true, string timeZone = "")
    {
        _dbType = dbType;
        _connStr = conn_str;
        _commitNoWait = commitNoWait;
        _timeZone = timeZone;
        _dbSpeciffic = dbSpeciffic;
    }

    public DbConnectionType DbType
    {
        get
        {
            if (_dbType == null)
                throw new NullReferenceException(nameof(DbType));

            return _dbType.Value;
        }
        set
        {
            _dbType = value;
        }
    }

    public string? ConnectionString
    {
        get { return _connStr; }
        set { _connStr = value; }
    }

    public IDbConnectionFactory Copy(string? newConnectionString = null)
    {
        if (_dbType == null)
            throw new NullReferenceException(nameof(_dbType));

        string? connString = newConnectionString;

        if (connString == null)
            connString = _connStr ?? string.Empty;

        return new DbConnectionFactory(_dbType.Value, connString, _dbSpeciffic, _commitNoWait ?? true, _timeZone ?? string.Empty);
    }

    public static void RegisterDatabaseFactory(string factoryName, DbProviderFactory dbProviderFactory)
    {
        if (DbProviderFactories.TryGetFactory(factoryName, out _))
        {
            return;
        }

        DbProviderFactories.RegisterFactory(factoryName, dbProviderFactory);
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

        ZenDbConnection connection = new ZenDbConnection(conn, _dbType!.Value, _dbSpeciffic);

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

    public static DbConnectionFactory CreateFromConfiguration(
        IConfigurationManager configurationManager,
        string connectionStringName,
        DbConnectionType dbType,
        IDbSpeciffic? dbSpeciffic = null,
        bool commitNoWait = true,
        string? timeZone = null)
    {
        string? connString = configurationManager.GetConnectionString(connectionStringName);

        if (!string.IsNullOrEmpty(connString))
        {
            return GetDbConnectionFactoryWithConnectionString(connString, dbType, dbSpeciffic, commitNoWait, timeZone);
        }

        ConnectionStringModel? connStringModel = configurationManager?
                .GetSection("DatabaseConnections")?
                .GetSection(connectionStringName)?
                .Get<ConnectionStringModel>();

        if (connStringModel == null)
            throw new NullReferenceException(nameof(connStringModel));

        return GetDbConnectionFactoryFromConnectionSection(connStringModel, dbSpeciffic, commitNoWait);
    }

    public static DbConnectionFactory CreateFromConfiguration(
        IConfiguration configuration,
        string connectionStringName,
        DbConnectionType dbType,
        IDbSpeciffic? dbSpeciffic = null,
        bool commitNoWait = true,
        string? timeZone = null)
    {
        string? connString = configuration.GetConnectionString(connectionStringName);

        if (!string.IsNullOrEmpty(connString))
        {
            return GetDbConnectionFactoryWithConnectionString(connString, dbType, dbSpeciffic, commitNoWait, timeZone);
        }

        ConnectionStringModel? connStringModel = configuration?
                .GetSection("DatabaseConnections")?
                .GetSection(connectionStringName)?
                .Get<ConnectionStringModel>();

        if (connStringModel == null)
            throw new NullReferenceException(nameof(connStringModel));

        return GetDbConnectionFactoryFromConnectionSection(connStringModel, dbSpeciffic, commitNoWait);
    }

    private static DbConnectionFactory GetDbConnectionFactoryWithConnectionString(
        string connString,
        DbConnectionType dbType,
        IDbSpeciffic? dbSpeciffic = null,
        bool commitNoWait = true,
        string? timeZone = null)
    {
        DbConnectionFactory dbConnectionFactory = new DbConnectionFactory(
                dbType,
                connString,
                dbSpeciffic,
                commitNoWait,
                timeZone ?? "UTC"
            );

        return dbConnectionFactory;
    }

    private static DbConnectionFactory GetDbConnectionFactoryFromConnectionSection(
        ConnectionStringModel connStringModel,
        IDbSpeciffic? dbSpeciffic = null,
        bool commitNoWait = true)
    {
        DbConnectionFactory dbConnectionFactory = new DbConnectionFactory(
            connStringModel.DbConnectionType,
            connStringModel.ConnectionString,
            dbSpeciffic,
            commitNoWait,
            connStringModel.TimeZone
        );

        return dbConnectionFactory;
    }
}
