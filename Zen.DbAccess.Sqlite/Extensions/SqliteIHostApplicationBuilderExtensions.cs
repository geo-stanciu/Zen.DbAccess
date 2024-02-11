using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Factories;
using System.Data.SQLite;
using Zen.DbAccess.Enums;

namespace Zen.DbAccess.Sqlite.Extensions;

public static class SqliteIHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddSqliteZenDbAccessConnection(
        this IHostApplicationBuilder builder,
        string connectionStringName = "")
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQLITE, SQLiteFactory.Instance, new DatabaseSpeciffic());

        if (!string.IsNullOrEmpty(connectionStringName))
            DbConnectionFactory.RegisterConnectionDI(DbConnectionType.SqlServer, connectionStringName);

        return builder;
    }

    public static void AddSqliteZenDbAccessConnection(
       this HostBuilderContext hostingContext,
       string connectionStringName = "")
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQLITE, SQLiteFactory.Instance, new DatabaseSpeciffic());

        if (!string.IsNullOrEmpty(connectionStringName))
            DbConnectionFactory.RegisterConnectionDI(DbConnectionType.SqlServer, connectionStringName);
    }
}
