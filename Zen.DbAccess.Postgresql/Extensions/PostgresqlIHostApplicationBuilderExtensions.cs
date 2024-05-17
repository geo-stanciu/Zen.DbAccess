using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Factories;
using Npgsql;
using Zen.DbAccess.Enums;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Zen.DbAccess.Postgresql.Extensions;

public static class PostgresqlIHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddPostgresqlZenDbAccessConnection<T>(
        this IHostApplicationBuilder builder,
        T serviceKey,
        string connectionStringName = "",
        bool commitNoWait = true,
        string? timeZone = null,
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.POSTGRESQL, NpgsqlFactory.Instance);

        IConfigurationManager configurationManager = builder.Configuration;

        DbConnectionFactory dbConnectionFactory = DbConnectionFactory.CreateFromConfiguration(
            configurationManager,
            connectionStringName,
            DbConnectionType.Postgresql,
            new PostgresqlDatabaseSpeciffic(),
            commitNoWait,
            timeZone,
            dbNamingConvention);

        builder.Services.AddKeyedSingleton<IDbConnectionFactory, DbConnectionFactory>(serviceKey, (_ /* serviceProvider */, _ /* object */) => dbConnectionFactory);

        return builder;
    }

    public static void AddPostgresqlZenDbAccessConnection<T>(
        this HostBuilderContext hostingContext,
        IServiceCollection services,
        T serviceKey,
        string connectionStringName = "",
        bool commitNoWait = true,
        string? timeZone = null,
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.POSTGRESQL, NpgsqlFactory.Instance);

        IConfiguration configuration = hostingContext.Configuration;

        DbConnectionFactory dbConnectionFactory = DbConnectionFactory.CreateFromConfiguration(
            configuration,
            connectionStringName,
            DbConnectionType.Postgresql,
            new PostgresqlDatabaseSpeciffic(),
            commitNoWait,
            timeZone,
            dbNamingConvention);

        services.AddKeyedSingleton<IDbConnectionFactory, DbConnectionFactory>(serviceKey, (_ /* serviceProvider */, _ /* object */) => dbConnectionFactory);
    }
}
