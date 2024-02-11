using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Factories;
using Npgsql;

namespace Zen.DbAccess.Postgresql.Extensions;

public static class PostgresqlIHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddPostgresqlZenDbAccessConnection(
        this IHostApplicationBuilder builder,
        string connectionName,
        string connectionStringName)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.POSTGRESQL, NpgsqlFactory.Instance, new PostgresqlDatabaseSpeciffic());

        DbConnectionFactory.RegisterConnection(connectionName, connectionStringName);

        return builder;
    }
}
