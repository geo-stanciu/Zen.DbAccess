using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Enums;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Zen.DbAccess.Sqlite.Extensions;

public static class SqliteIHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddSqliteZenDbAccessConnection<T>(
        this IHostApplicationBuilder builder,
        T serviceKey,
        string connectionStringName = "",
        bool commitNoWait = true,
        string? timeZone = null,
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQLITE, Microsoft.Data.Sqlite.SqliteFactory.Instance);

        IConfigurationManager configurationManager = builder.Configuration;

        DbConnectionFactory dbConnectionFactory = DbConnectionFactory.CreateFromConfiguration(
            configurationManager,
            connectionStringName,
            DbConnectionType.Sqlite,
            new SqliteDatabaseSpeciffic(),
            commitNoWait,
            timeZone,
            dbNamingConvention);

        builder.Services.AddKeyedSingleton<IDbConnectionFactory, DbConnectionFactory>(serviceKey, (_ /* serviceProvider */, _ /* object */) => dbConnectionFactory);

        return builder;
    }

    public static void AddSqliteZenDbAccessConnection<T>(
        this HostBuilderContext hostingContext,
        IServiceCollection services,
        T serviceKey,
        string connectionStringName = "",
        bool commitNoWait = true,
        string? timeZone = null,
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQLITE, Microsoft.Data.Sqlite.SqliteFactory.Instance);

        IConfiguration configuration = hostingContext.Configuration;

        DbConnectionFactory dbConnectionFactory = DbConnectionFactory.CreateFromConfiguration(
            configuration,
            connectionStringName,
            DbConnectionType.Sqlite,
            new SqliteDatabaseSpeciffic(),
            commitNoWait,
            timeZone,
            dbNamingConvention);

        services.AddKeyedSingleton<IDbConnectionFactory, DbConnectionFactory>(serviceKey, (_ /* serviceProvider */, _ /* object */) => dbConnectionFactory);
    }
}
