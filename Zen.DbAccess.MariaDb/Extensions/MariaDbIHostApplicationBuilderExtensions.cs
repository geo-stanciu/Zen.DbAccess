using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MySqlConnector;
using System.Data.Common;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Factories;

namespace Zen.DbAccess.MariaDb.Extensions;

public static class MariaDbIHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddMariaDbZenDbAccessConnection<T>(
        this IHostApplicationBuilder builder,
        T serviceKey,
        string connectionStringName = "",
        bool commitNoWait = true,
        string? timeZone = null,
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.MARIADB, MySqlConnectorFactory.Instance);

        IConfigurationManager configurationManager = builder.Configuration;

        DbConnectionFactory dbConnectionFactory = DbConnectionFactory.CreateFromConfiguration(
            configurationManager,
            connectionStringName,
            DbConnectionType.MariaDb,
            new MariaDbDatabaseSpeciffic(),
            commitNoWait,
            timeZone,
            dbNamingConvention);

        builder.Services.AddKeyedSingleton<IDbConnectionFactory, DbConnectionFactory>(serviceKey, (_ /* serviceProvider */, _ /* object */) => dbConnectionFactory);

        return builder;
    }

    public static void AddMariaDbZenDbAccessConnection<T>(
        this HostBuilderContext hostingContext,
        IServiceCollection services,
        T serviceKey,
        string connectionStringName = "",
        bool commitNoWait = true,
        string? timeZone = null,
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.MARIADB, MySqlConnectorFactory.Instance);

        IConfiguration configuration = hostingContext.Configuration;

        DbConnectionFactory dbConnectionFactory = DbConnectionFactory.CreateFromConfiguration(
            configuration,
            connectionStringName,
            DbConnectionType.MariaDb,
            new MariaDbDatabaseSpeciffic(),
            commitNoWait,
            timeZone,
            dbNamingConvention);

        services.AddKeyedSingleton<IDbConnectionFactory, DbConnectionFactory>(serviceKey, (_ /* serviceProvider */, _ /* object */) => dbConnectionFactory);
    }
}
