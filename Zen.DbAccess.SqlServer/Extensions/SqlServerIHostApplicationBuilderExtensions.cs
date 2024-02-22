using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Factories;
using System.Data.SqlClient;
using Zen.DbAccess.Enums;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Zen.DbAccess.SqlServer.Extensions;

public static class SqlServerIHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddSqlServerZenDbAccessConnection<T>(
        this IHostApplicationBuilder builder,
        T serviceKey,
        string connectionStringName = "",
        bool commitNoWait = true,
        string? timeZone = null)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQL_SERVER, SqlClientFactory.Instance);

        IConfigurationManager configurationManager = builder.Configuration;

        DbConnectionFactory dbConnectionFactory = DbConnectionFactory.CreateFromConfiguration(
            configurationManager,
            connectionStringName,
            DbConnectionType.SqlServer,
            new SqlServerDatabaseSpeciffic(),
            commitNoWait,
            timeZone);

        builder.Services.AddKeyedSingleton<IDbConnectionFactory, DbConnectionFactory>(serviceKey, (_ /* serviceProvider */, _ /* object */) => dbConnectionFactory);

        return builder;
    }

    public static void AddSqlServerZenDbAccessConnection<T>(
        this HostBuilderContext hostingContext,
        IServiceCollection services,
        T serviceKey,
        string connectionStringName = "",
        bool commitNoWait = true,
        string? timeZone = null)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQL_SERVER, SqlClientFactory.Instance);

        IConfiguration configuration = hostingContext.Configuration;

        DbConnectionFactory dbConnectionFactory = DbConnectionFactory.CreateFromConfiguration(
            configuration,
            connectionStringName,
            DbConnectionType.SqlServer,
            new SqlServerDatabaseSpeciffic(),
            commitNoWait,
            timeZone);

        services.AddKeyedSingleton<IDbConnectionFactory, DbConnectionFactory>(serviceKey, (_ /* serviceProvider */, _ /* object */) => dbConnectionFactory);
    }
}
