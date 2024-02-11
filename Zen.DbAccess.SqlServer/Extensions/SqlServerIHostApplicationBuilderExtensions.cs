using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Factories;
using System.Data.SqlClient;
using Zen.DbAccess.Enums;

namespace Zen.DbAccess.SqlServer.Extensions;

public static class SqlServerIHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddSqlServerZenDbAccessConnection(
        this IHostApplicationBuilder builder,
        string connectionStringName)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQL_SERVER, SqlClientFactory.Instance, new SqlServerDatabaseSpeciffic());

        DbConnectionFactory.RegisterConnectionDI(DbConnectionType.SqlServer, connectionStringName);

        return builder;
    }
}
