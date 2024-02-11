using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Factories;
using System.Data.SQLite;

namespace Zen.DbAccess.Sqlite.Extensions;

public static class SqliteIHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddSqliteZenDbAccessConnection(
        this IHostApplicationBuilder builder,
        string connectionName,
        string connectionStringName)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQLITE, SQLiteFactory.Instance, new DatabaseSpeciffic());

        DbConnectionFactory.RegisterConnection(connectionName, connectionStringName);

        return builder;
    }
}
