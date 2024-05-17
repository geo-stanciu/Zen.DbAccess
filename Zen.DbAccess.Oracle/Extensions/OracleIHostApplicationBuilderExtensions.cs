using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Factories;

namespace Zen.DbAccess.Oracle.Extensions;

public static class OracleIHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddOracleZenDbAccessConnection<T>(
        this IHostApplicationBuilder builder,
        T serviceKey,
        string connectionStringName = "",
        bool commitNoWait = true,
        string? timeZone = null,
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.ORACLE, OracleClientFactory.Instance);

        IConfigurationManager configurationManager = builder.Configuration;

        DbConnectionFactory dbConnectionFactory = DbConnectionFactory.CreateFromConfiguration(
            configurationManager,
            connectionStringName,
            DbConnectionType.Oracle,
            new OracleDatabaseSpeciffic(),
            commitNoWait,
            timeZone,
            dbNamingConvention);

        builder.Services.AddKeyedSingleton<IDbConnectionFactory, DbConnectionFactory>(serviceKey, (_ /* serviceProvider */, _ /* object */) => dbConnectionFactory);

        return builder;
    }

    public static void AddOracleZenDbAccessConnection<T>(
        this HostBuilderContext hostingContext,
        IServiceCollection services,
        T serviceKey,
        string connectionStringName = "",
        bool commitNoWait = true,
        string? timeZone = null,
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.ORACLE, OracleClientFactory.Instance);

        IConfiguration configuration = hostingContext.Configuration;

        DbConnectionFactory dbConnectionFactory = DbConnectionFactory.CreateFromConfiguration(
            configuration,
            connectionStringName,
            DbConnectionType.Oracle,
            new OracleDatabaseSpeciffic(),
            commitNoWait,
            timeZone,
            dbNamingConvention);

        services.AddKeyedSingleton<IDbConnectionFactory, DbConnectionFactory>(serviceKey, (_ /* serviceProvider */, _ /* object */) => dbConnectionFactory);
    }
}
